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
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.PrefetchingBlockStream
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

open class IndexerCoordinator(
    private val thorClient: ThorClient,
    private val batchSize: Int = 1,
) {
    companion object {
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: Collection<Indexer>,
            blockBatchSize: Int = 1,
        ): Job {
            if (indexers.isEmpty()) {
                return scope.launch {}
            }

            val blockIndexers = indexers.toList()
            val indexerCoordinator = IndexerCoordinator(thorClient, batchSize = blockBatchSize)

            return scope.launch {
                indexerCoordinator.run(
                    indexers = blockIndexers,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<Indexer>,
    ) = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        val executionGroups = CoordinatorSupport.topologicalOrder(indexers)
        CoordinatorSupport.prepareIndexers(scope = this, indexers = indexers)

        val stream =
            PrefetchingBlockStream(
                scope = this,
                batchSize = batchSize,
                currentBlockProvider = { indexers.minOf { it.getCurrentBlockNumber() } },
                thorClient = thorClient,
            )

        val supervisor =
            GroupSupervisor(
                parentScope = this,
                stream = stream,
                executionGroups = executionGroups,
            )

        supervisor.run()
    }
}

internal class GroupSupervisor(
    private val parentScope: CoroutineScope,
    private val stream: BlockStream,
    private val executionGroups: List<List<Indexer>>,
) {
    private val resetChannel = Channel<Unit>(capacity = Channel.CONFLATED)

    suspend fun run() {
        var activeGroups = launchGroups()
        try {
            var shouldExit = false
            while (parentScope.isActive) {
                kotlinx.coroutines.selects.select<Unit> {
                    activeGroups.completion.onAwait {
                        // All jobs completed, exit loop
                        shouldExit = true
                    }
                    resetChannel.onReceive { activeGroups = restartGroups(activeGroups) }
                }
                if (shouldExit) break
            }
        } finally {
            activeGroups.cancelAll()
            stream.close()
            resetChannel.close()
        }
    }

    private suspend fun restartGroups(current: GroupSet): GroupSet {
        current.cancelAll()
        stream.reset()
        return launchGroups()
    }

    private suspend fun launchGroups(): GroupSet {
        val jobs =
            executionGroups.map { group ->
                val subscription = stream.subscribe()
                parentScope.launch {
                    processGroup(
                        group = group,
                        subscription = subscription,
                        onResetRequested = { resetChannel.trySend(Unit).isSuccess },
                    )
                }
            }
        val completion = parentScope.async { jobs.joinAll() }
        return GroupSet(jobs, completion)
    }

    private suspend fun GroupSet.cancelAll() {
        completion.cancel()
        jobs.forEach { it.cancel() }
        jobs.joinAll()
    }

    private data class GroupSet(
        val jobs: List<Job>,
        val completion: Deferred<Unit>,
    )
}

internal suspend fun CoroutineScope.processGroup(
    group: List<Indexer>,
    subscription: BlockStreamSubscription,
    onResetRequested: () -> Unit,
) {
    val resetController = ResetController(onResetRequested)

    try {
        while (isActive && !resetController.isRequested()) {

            val block =
                try {
                    subscription.next()
                } catch (e: CancellationException) {
                    throw e
                } catch (_: Exception) {
                    resetController.request()
                    break
                }

            try {
                runGroupForBlock(group, block, resetController)
            } catch (e: CancellationException) {
                throw e
            }
        }
    } finally {
        subscription.close()
    }
}

internal suspend fun runGroupForBlock(
    group: List<Indexer>,
    block: Block,
    resetController: ResetController,
) = coroutineScope {
    group
        .filter { it.getCurrentBlockNumber() == block.number }
        .forEach { indexer ->
            launch {
                try {
                    indexer.processBlock(block)
                } catch (_: Exception) {
                    resetController.request()
                    cancel()
                }
            }
        }
}

internal class ResetController(private val triggerReset: () -> Unit) {
    private val requested = AtomicBoolean(false)

    fun request() {
        if (requested.compareAndSet(false, true)) {
            triggerReset()
        }
    }

    fun isRequested(): Boolean = requested.get()
}
