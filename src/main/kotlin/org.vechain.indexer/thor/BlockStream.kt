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
    private var channel: Channel<Block>? = null
    private var fetchJob: Job? = null
    private var currentStartBlock: Long = currentBlockProvider()
    private var subscriptionActive = false

    override suspend fun subscribe(): BlockStreamSubscription {
        mutex.withLock {
            if (subscriptionActive) {
                throw IllegalStateException(
                    "Only one active subscription is supported. Close the existing subscription first."
                )
            }

            val newChannel = Channel<Block>(capacity = batchSize)
            channel = newChannel
            subscriptionActive = true
            currentStartBlock = currentBlockProvider()

            startFetcher(currentStartBlock, newChannel)

            return SingleSubscription(
                channel = newChannel,
                startBlock = currentStartBlock,
                onClose = { handleSubscriptionClose() }
            )
        }
    }

    override suspend fun reset() {
        mutex.withLock {
            // Cancel existing fetch job
            fetchJob?.cancelAndJoin()
            fetchJob = null

            // Close existing channel if any
            channel?.close()

            // Update start block
            currentStartBlock = currentBlockProvider()

            // If there's an active subscription, restart fetcher with new channel
            if (subscriptionActive) {
                val newChannel = Channel<Block>(capacity = batchSize)
                channel = newChannel
                startFetcher(currentStartBlock, newChannel)
            }
        }
    }

    override suspend fun close() {
        mutex.withLock {
            fetchJob?.cancelAndJoin()
            fetchJob = null
            channel?.close()
            channel = null
            subscriptionActive = false
        }
    }

    private fun startFetcher(startBlock: Long, targetChannel: Channel<Block>) {
        val job =
            scope.launch {
                var nextBlock = startBlock
                try {
                    while (isActive) {
                        val block = thorClient.waitForBlock(nextBlock)
                        targetChannel.send(block)
                        nextBlock++
                    }
                } catch (_: CancellationException) {
                    // Fetcher cancelled - this is expected during reset or close
                } finally {
                    targetChannel.close()
                }
            }
        fetchJob = job
    }

    private suspend fun handleSubscriptionClose() {
        mutex.withLock {
            subscriptionActive = false
            fetchJob?.cancelAndJoin()
            fetchJob = null
            channel?.close()
            channel = null
        }
    }

    private inner class SingleSubscription(
        private var channel: Channel<Block>,
        startBlock: Long,
        private val onClose: suspend () -> Unit,
    ) : BlockStreamSubscription {
        private var expectedBlock: Long = startBlock

        override val nextBlockNumber: Long
            get() = expectedBlock

        override suspend fun next(): Block {
            val block =
                try {
                    channel.receive()
                } catch (e: ClosedReceiveChannelException) {
                    // Channel was closed, check if there's a new one (after reset)
                    mutex.withLock {
                        val newChannel = this@PrefetchingBlockStream.channel
                        if (newChannel != null && newChannel != channel) {
                            // Reset happened, use new channel and update expected block
                            channel = newChannel
                            expectedBlock = currentStartBlock
                            // Try to receive from new channel
                            return try {
                                val newBlock = channel.receive()
                                if (newBlock.number != expectedBlock) {
                                    throw IllegalStateException(
                                        "Received block ${newBlock.number} but expected $expectedBlock"
                                    )
                                }
                                expectedBlock = newBlock.number + 1
                                newBlock
                            } catch (e: ClosedReceiveChannelException) {
                                throw CancellationException("Subscription closed", e)
                            }
                        }
                    }
                    throw CancellationException("Subscription closed", e)
                }

            if (block.number != expectedBlock) {
                throw IllegalStateException(
                    "Received block ${block.number} but expected $expectedBlock"
                )
            }

            expectedBlock = block.number + 1
            return block
        }

        override suspend fun close() {
            onClose()
        }
    }
}
