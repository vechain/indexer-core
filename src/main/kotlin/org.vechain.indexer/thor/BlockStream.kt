package org.vechain.indexer.thor

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
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

class PrefetchingBlockStream(
    private val scope: CoroutineScope,
    private val batchSize: Int,
    private val currentBlockProvider: () -> Long,
    private val thorClient: ThorClient,
) : BlockStream {

    private val logger = LoggerFactory.getLogger(PrefetchingBlockStream::class.java)

    private sealed interface StreamEvent {
        val version: Int

        data class Reset(override val version: Int, val startBlock: Long) : StreamEvent

        data class Block(
            override val version: Int,
            val block: org.vechain.indexer.thor.model.Block
        ) : StreamEvent
    }

    private data class SubscriptionState(
        val id: Int,
        val channel: Channel<StreamEvent>,
        val collectorJob: Job,
        var expectedBlock: Long,
        var version: Int,
    )

    private val mutex = Mutex()
    private val subscriptions = mutableMapOf<Int, SubscriptionState>()
    private val stream =
        MutableSharedFlow<StreamEvent>(
            replay = 0,
            extraBufferCapacity = batchSize,
            onBufferOverflow = BufferOverflow.SUSPEND,
        )
    private var supervisor: CompletableJob = newSupervisor()
    private var streamScope = CoroutineScope(scope.coroutineContext + supervisor)
    private var fetchJob: Job? = null
    private val subscriptionSequence = AtomicInteger(0)
    private var latestReset = StreamEvent.Reset(version = 0, startBlock = currentBlockProvider())

    init {
        startFetcher(latestReset)
    }

    override suspend fun subscribe(): BlockStreamSubscription {
        val (state, reset) =
            mutex.withLock {
                val id = subscriptionSequence.incrementAndGet()
                val channel = Channel<StreamEvent>(capacity = batchSize)
                val resetSnapshot = latestReset
                val collectorJob = startCollector(channel)
                val subscriptionState =
                    SubscriptionState(
                        id = id,
                        channel = channel,
                        collectorJob = collectorJob,
                        expectedBlock = resetSnapshot.startBlock,
                        version = resetSnapshot.version,
                    )
                subscriptions[id] = subscriptionState
                subscriptionState to resetSnapshot
            }

        // Send the latest reset event outside the lock to avoid potential suspension.
        state.channel.trySend(reset).getOrThrow()

        return Subscription(state)
    }

    override suspend fun reset() {
        val (resetEvent, previousJob) =
            mutex.withLock {
                val newReset =
                    StreamEvent.Reset(
                        version = latestReset.version + 1,
                        startBlock = currentBlockProvider()
                    )
                val existingJob = fetchJob
                fetchJob = null
                latestReset = newReset
                newReset to existingJob
            }

        previousJob?.cancelAndJoin()
        startFetcher(resetEvent)
    }

    override suspend fun close() {
        val states =
            mutex.withLock {
                val existing = subscriptions.values.toList()
                subscriptions.clear()
                existing
            }

        states.forEach { state ->
            state.collectorJob.cancel()
            state.channel.cancel()
        }

        val job = fetchJob
        fetchJob = null
        job?.cancelAndJoin()
        supervisor.cancel()
    }

    private fun startCollector(channel: Channel<StreamEvent>): Job =
        streamScope.launch {
            try {
                stream.collect { event -> channel.send(event) }
            } catch (_: CancellationException) {
                // Collector cancelled by subscription closing/reset.
            } finally {
                channel.close()
            }
        }

    private fun startFetcher(resetEvent: StreamEvent.Reset) {
        val job =
            streamScope.launch {
                stream.emit(resetEvent)
                var nextBlock = resetEvent.startBlock
                val version = resetEvent.version
                while (isActive) {
                    val block = thorClient.waitForBlock(nextBlock)
                    stream.emit(StreamEvent.Block(version, block))
                    nextBlock++
                }
            }
        fetchJob = job
    }

    private fun newSupervisor(): CompletableJob {
        val parentJob = scope.coroutineContext[Job]
        return SupervisorJob(parentJob)
    }

    private inner class Subscription(
        private val state: SubscriptionState,
    ) : BlockStreamSubscription {
        override val nextBlockNumber: Long
            get() = state.expectedBlock

        override suspend fun next(): Block {
            while (true) {
                val event =
                    try {
                        state.channel.receive()
                    } catch (e: ClosedReceiveChannelException) {
                        throw CancellationException("Subscription closed", e)
                    }
                when (event) {
                    is StreamEvent.Reset -> {
                        if (event.version < state.version) continue
                        state.version = event.version
                        state.expectedBlock = event.startBlock
                    }
                    is StreamEvent.Block -> {
                        if (event.version < state.version) continue
                        if (event.version > state.version) {
                            state.version = event.version
                            state.expectedBlock = event.block.number
                        }
                        val expected = state.expectedBlock
                        if (event.block.number < expected) {
                            continue
                        }
                        if (event.block.number > expected) {
                            state.expectedBlock = event.block.number
                        }
                        state.expectedBlock = event.block.number + 1
                        return event.block
                    }
                }
            }
        }

        override suspend fun close() {
            val stateToClose = mutex.withLock { subscriptions.remove(state.id) }
            if (stateToClose != null) {
                state.collectorJob.cancel()
                state.channel.cancel()
            }
        }
    }
}
