package org.vechain.indexer

import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.PrefetchingBlockStream
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

private const val RESET_POLL_INTERVAL_MS = 100L

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

        val supervisor =
            GroupSupervisor(
                parentScope = this,
                stream = stream,
                executionGroups = executionGroups,
                allIndexers = indexers,
                maxBlockExclusive = maxBlockExclusive,
            )

        supervisor.run()
    }
}

internal class GroupSupervisor(
    private val parentScope: CoroutineScope,
    private val stream: BlockStream,
    private val executionGroups: List<List<BlockIndexer>>,
    private val allIndexers: List<BlockIndexer>,
    private val maxBlockExclusive: Long?,
) {
    private val resetChannel = Channel<Unit>(capacity = Channel.CONFLATED)

    suspend fun run() {
        var activeGroups = launchGroups()

        try {
            while (parentScope.isActive) {
                if (activeGroups.completion.isCompleted) {
                    activeGroups.completion.await()
                    break
                }

                val resetSignal = tryReceiveReset()
                if (resetSignal != null) {
                    activeGroups = restartGroups(activeGroups)
                }
            }
        } finally {
            activeGroups.cancelAll()
            stream.close()
            resetChannel.close()
        }
    }

    private suspend fun tryReceiveReset(): Unit? =
        try {
            withTimeoutOrNull(RESET_POLL_INTERVAL_MS) { resetChannel.receive() }
        } catch (_: ClosedReceiveChannelException) {
            null
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
                        allIndexers = allIndexers,
                        maxBlockExclusive = maxBlockExclusive,
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
    group: List<BlockIndexer>,
    subscription: BlockStreamSubscription,
    allIndexers: List<BlockIndexer>,
    maxBlockExclusive: Long?,
    onResetRequested: () -> Unit,
) {
    val resetController = ResetController(onResetRequested)

    try {
        while (isActive && !resetController.isRequested()) {
            if (maxBlockExclusive != null && subscription.nextBlockNumber >= maxBlockExclusive) {
                break
            }

            val block =
                try {
                    subscription.next()
                } catch (e: CancellationException) {
                    if (!resetController.isRequested()) {
                        throw e
                    }
                    break
                } catch (e: Exception) {
                    val blockNumber = subscription.nextBlockNumber
                    allIndexers.forEach { indexer ->
                        indexer.logBlockFetchError(blockNumber, e)
                        indexer.handleError()
                    }
                    resetController.request()
                    break
                }

            if (maxBlockExclusive != null && block.number >= maxBlockExclusive) {
                break
            }

            if (resetController.isRequested()) {
                break
            }

            try {
                runGroupForBlock(group, block, resetController)
            } catch (e: CancellationException) {
                if (!resetController.isRequested()) {
                    throw e
                }
            }

            if (resetController.isRequested()) {
                break
            }
        }
    } finally {
        subscription.close()
    }
}

internal suspend fun runGroupForBlock(
    group: List<BlockIndexer>,
    block: Block,
    resetController: ResetController,
) = coroutineScope {
    group.forEach { indexer ->
        launch {
            if (resetController.isRequested()) return@launch

            indexer.restartIfNeeded()
            if (resetController.isRequested()) return@launch

            indexer.processBlock(block) {
                resetController.request()
                cancel()
            }

            if (indexer.status == Status.ERROR || indexer.status == Status.REORG) {
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
