package org.vechain.indexer

import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.vechain.indexer.thor.client.ThorClient

open class IndexerCoordinator(
    private val thorClient: ThorClient,
    private val batchSize: Int = 1,
) {
    companion object {
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: Collection<BlockIndexer>,
            blockBatchSize: Int = 1,
            maxBlocks: Long? = null,
        ): Job {
            if (indexers.isEmpty()) {
                return scope.launch {}
            }

            val blockIndexers = indexers.toList()
            val indexerCoordinator = IndexerCoordinator(thorClient, batchSize = blockBatchSize)

            return scope.launch {
                indexerCoordinator.run(
                    indexers = blockIndexers,
                    maxBlocks = maxBlocks,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<BlockIndexer>,
        maxBlocks: Long? = null,
    ) = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        val executionGroups = CoordinatorSupport.topologicalOrder(indexers)
        CoordinatorSupport.prepareIndexers(scope = this, indexers = indexers)

        val startBlockNumber = indexers.minOf { it.currentBlockNumber }
        val maxBlockExclusive = maxBlocks?.let { startBlockNumber + it }

        val stream =
            PrefetchingBlockStream(
                scope = this,
                batchSize = batchSize,
                currentBlockProvider = { indexers.minOf { it.currentBlockNumber } },
                thorClient = thorClient,
            )

        val resetChannel = Channel<Unit>(capacity = Channel.CONFLATED)

        suspend fun CoroutineScope.launchGroupProcessors(): Pair<List<Job>, Deferred<Unit>> {
            val jobs =
                executionGroups.map { group ->
                    val subscription = stream.subscribe()
                    launch {
                        processGroup(
                            group = group,
                            subscription = subscription,
                            allIndexers = indexers,
                            resetChannel = resetChannel,
                            maxBlockExclusive = maxBlockExclusive,
                        )
                    }
                }
            val completion = async { jobs.joinAll() }
            return jobs to completion
        }

        var (groupJobs, groupCompletion) = launchGroupProcessors()

        try {
            while (isActive) {
                if (groupCompletion.isCompleted) {
                    groupCompletion.await()
                    break
                }

                val resetSignal = withTimeoutOrNull(100L) { resetChannel.receive() }
                if (resetSignal != null) {
                    groupCompletion.cancel()
                    groupJobs.forEach { job -> job.cancel() }
                    groupJobs.joinAll()
                    stream.reset()
                    val restarted = launchGroupProcessors()
                    groupJobs = restarted.first
                    groupCompletion = restarted.second
                }
            }
        } finally {
            groupCompletion.cancel()
            groupJobs.forEach { it.cancel() }
            groupJobs.joinAll()
            stream.close()
        }
    }

    private suspend fun CoroutineScope.processGroup(
        group: List<BlockIndexer>,
        subscription: BlockStreamSubscription,
        allIndexers: List<BlockIndexer>,
        resetChannel: Channel<Unit>,
        maxBlockExclusive: Long?,
    ) {
        val resetRequested = AtomicBoolean(false)

        fun requestReset() {
            if (resetRequested.compareAndSet(false, true)) {
                resetChannel.trySend(Unit).isSuccess
            }
        }

        try {
            while (isActive && !resetRequested.get()) {
                if (
                    maxBlockExclusive != null && subscription.nextBlockNumber >= maxBlockExclusive
                ) {
                    break
                }

                val block =
                    try {
                        subscription.next()
                    } catch (e: CancellationException) {
                        if (!resetRequested.get()) {
                            throw e
                        }
                        break
                    } catch (e: Exception) {
                        val blockNumber = subscription.nextBlockNumber
                        allIndexers.forEach { indexer ->
                            indexer.logBlockFetchError(blockNumber, e)
                            indexer.handleError()
                        }
                        requestReset()
                        break
                    }

                if (maxBlockExclusive != null && block.number >= maxBlockExclusive) {
                    break
                }

                try {
                    coroutineScope {
                        fun triggerReset() {
                            if (!resetRequested.get()) {
                                requestReset()
                            }
                            cancel()
                        }

                        group
                            .map { indexer ->
                                launch {
                                    if (resetRequested.get()) return@launch

                                    indexer.restartIfNeeded()
                                    if (resetRequested.get()) return@launch

                                    indexer.processBlock(block) { triggerReset() }

                                    if (
                                        indexer.status == Status.ERROR ||
                                            indexer.status == Status.REORG
                                    ) {
                                        triggerReset()
                                    }
                                }
                            }
                            .joinAll()
                    }
                } catch (e: CancellationException) {
                    if (!resetRequested.get()) {
                        throw e
                    }
                }

                if (resetRequested.get()) {
                    break
                }
            }
        } finally {
            subscription.close()
        }
    }
}
