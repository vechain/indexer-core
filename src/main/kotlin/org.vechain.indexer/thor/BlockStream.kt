package org.vechain.indexer.thor

import java.util.UUID
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
import kotlinx.coroutines.flow.first
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

class PrefetchingBlockStream(
    private val scope: CoroutineScope,
    private val batchSize: Int,
    private val currentBlockProvider: () -> Long,
    private val thorClient: ThorClient,
) : BlockStream {

    private sealed interface StreamEvent {
        val resetToken: String

        data class Reset(override val resetToken: String, val startBlock: Long) : StreamEvent

        data class Block(
            override val resetToken: String,
            val block: org.vechain.indexer.thor.model.Block
        ) : StreamEvent
    }

    private data class SubscriptionState(
        val id: Int,
        val channel: Channel<StreamEvent>,
        val collectorJob: Job,
        var expectedBlock: Long,
        var resetToken: String,
        var previousResetToken: String?,
    )

    private val mutex = Mutex()
    private val subscriptions = mutableMapOf<Int, SubscriptionState>()
    private val stream =
        MutableSharedFlow<StreamEvent>(
            replay = 0,
            extraBufferCapacity = maxOf(batchSize - 1, 0),
            onBufferOverflow = BufferOverflow.SUSPEND,
        )
    private var supervisor: CompletableJob = newSupervisor()
    private var streamScope = CoroutineScope(scope.coroutineContext + supervisor)
    private var fetchJob: Job? = null
    private val subscriptionSequence = AtomicInteger(0)
    private var latestReset =
        StreamEvent.Reset(resetToken = newResetToken(), startBlock = currentBlockProvider())

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
                        resetToken = resetSnapshot.resetToken,
                        previousResetToken = null,
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
                        resetToken = newResetToken(),
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
                // Ensure at least one collector is active before emitting to avoid dropping events.
                stream.subscriptionCount.first { count -> count > 0 }
                stream.emit(resetEvent)
                var nextBlock = resetEvent.startBlock
                val resetToken = resetEvent.resetToken
                while (isActive) {
                    val block = thorClient.waitForBlock(nextBlock)
                    stream.emit(StreamEvent.Block(resetToken, block))
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
                        when (event.resetToken) {
                            state.resetToken -> continue
                            state.previousResetToken -> continue
                            else -> {
                                state.previousResetToken = state.resetToken
                                state.resetToken = event.resetToken
                                state.expectedBlock = event.startBlock
                            }
                        }
                    }
                    is StreamEvent.Block -> {
                        when (event.resetToken) {
                            state.resetToken -> {
                                val expected = state.expectedBlock
                                if (event.block.number != expected) {
                                    throw IllegalStateException(
                                        "Received block ${event.block.number} but expected $expected"
                                    )
                                }
                                state.expectedBlock = event.block.number + 1
                                return event.block
                            }
                            state.previousResetToken -> continue
                            else ->
                                throw IllegalStateException(
                                    "Received block from unknown reset ${event.resetToken} before reset was processed"
                                )
                        }
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

    private fun newResetToken(): String = UUID.randomUUID().toString()
}
