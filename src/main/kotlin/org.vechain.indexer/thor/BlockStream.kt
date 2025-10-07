package org.vechain.indexer.thor

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

interface BlockStream {
    suspend fun subscribe(): BlockStreamSubscription

    suspend fun reset()

    suspend fun close()
}

interface BlockStreamSubscription {
    val nextBlockNumber: Long

    suspend fun next(): Block

    suspend fun close()
}

/**
 * A block stream that prefetches blocks from the blockchain and provides them through a single
 * subscription.
 *
 * This implementation supports only one active subscription at a time. The stream fetches blocks in
 * the background and buffers them based on the configured batch size.
 *
 * @param scope The coroutine scope for launching background jobs
 * @param batchSize The number of blocks to buffer
 * @param currentBlockProvider Provider function that returns the current block number to start from
 * @param thorClient The client for fetching blocks from the blockchain
 */
class PrefetchingBlockStream(
    private val scope: CoroutineScope,
    private val batchSize: Int,
    private val currentBlockProvider: () -> Long,
    private val thorClient: ThorClient,
) : BlockStream {

    private val mutex = Mutex()
    private var state: StreamState = StreamState.Idle

    override suspend fun subscribe(): BlockStreamSubscription {
        mutex.withLock {
            check(state is StreamState.Idle) {
                "Only one active subscription is supported. Close the existing subscription first."
            }

            val startBlock = currentBlockProvider()
            val channel = Channel<Block>(capacity = batchSize)
            val fetchJob = startBlockFetcher(startBlock, channel)

            state = StreamState.Active(channel, fetchJob, startBlock)

            return DefaultBlockStreamSubscription(
                streamState = { state },
                onClose = { handleSubscriptionClose() }
            )
        }
    }

    override suspend fun reset() {
        mutex.withLock {
            when (val current = state) {
                is StreamState.Active -> {
                    current.fetchJob.cancelAndJoin()
                    current.channel.close()

                    val newStartBlock = currentBlockProvider()
                    val newChannel = Channel<Block>(capacity = batchSize)
                    val newFetchJob = startBlockFetcher(newStartBlock, newChannel)

                    state = StreamState.Active(newChannel, newFetchJob, newStartBlock)
                }
                is StreamState.Idle -> {
                    // Nothing to reset
                }
            }
        }
    }

    override suspend fun close() {
        mutex.withLock {
            when (val current = state) {
                is StreamState.Active -> {
                    current.fetchJob.cancelAndJoin()
                    current.channel.close()
                    state = StreamState.Idle
                }
                is StreamState.Idle -> {
                    // Already closed
                }
            }
        }
    }

    private fun startBlockFetcher(startBlock: Long, channel: Channel<Block>): Job {
        return scope.launch {
            var nextBlock = startBlock
            try {
                while (isActive) {
                    val block = thorClient.waitForBlock(nextBlock)
                    channel.send(block)
                    nextBlock++
                }
            } catch (_: CancellationException) {
                // Fetcher cancelled - this is expected during reset or close
            } finally {
                channel.close()
            }
        }
    }

    private suspend fun handleSubscriptionClose() {
        mutex.withLock {
            when (val current = state) {
                is StreamState.Active -> {
                    current.fetchJob.cancelAndJoin()
                    current.channel.close()
                    state = StreamState.Idle
                }
                is StreamState.Idle -> {
                    // Already closed
                }
            }
        }
    }

    private sealed interface StreamState {
        data object Idle : StreamState

        data class Active(val channel: Channel<Block>, val fetchJob: Job, val startBlock: Long) :
            StreamState
    }

    private class DefaultBlockStreamSubscription(
        private val streamState: () -> StreamState,
        private val onClose: suspend () -> Unit,
    ) : BlockStreamSubscription {
        private var expectedBlockNumber: Long? = null

        override val nextBlockNumber: Long
            get() =
                expectedBlockNumber
                    ?: when (val state = streamState()) {
                        is StreamState.Active -> state.startBlock
                        is StreamState.Idle -> throw IllegalStateException("Stream is not active")
                    }

        override suspend fun next(): Block {
            val state = streamState()
            val (channel, startBlock) =
                when (state) {
                    is StreamState.Active -> state.channel to state.startBlock
                    is StreamState.Idle -> throw CancellationException("Subscription closed")
                }

            val block =
                try {
                    channel.receive()
                } catch (e: ClosedReceiveChannelException) {
                    throw CancellationException("Subscription closed", e)
                }

            val expected = expectedBlockNumber ?: startBlock
            check(block.number == expected) {
                "Received block ${block.number} but expected $expected"
            }

            expectedBlockNumber = block.number + 1
            return block
        }

        override suspend fun close() {
            onClose()
        }
    }
}
