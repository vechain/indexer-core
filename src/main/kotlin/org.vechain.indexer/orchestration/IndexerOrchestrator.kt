package org.vechain.indexer.orchestration

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.vechain.indexer.Indexer
import org.vechain.indexer.orchestration.OrchestrationUtils.runWithInterruptHandling
import org.vechain.indexer.thor.PrefetchingBlockStream
import org.vechain.indexer.thor.client.ThorClient

open class IndexerOrchestrator {
    companion object {
        @Suppress("unused")
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val indexerOrchestrator = IndexerOrchestrator()

            return scope.launch {
                indexerOrchestrator.run(
                    indexers = indexers,
                    batchSize = blockBatchSize,
                    thorClient = thorClient,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<Indexer>,
        batchSize: Int,
        thorClient: ThorClient,
    ) = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        val interruptController = InterruptController()
        val executionGroups = OrchestrationUtils.topologicalOrder(indexers)

        val supervisor =
            InterruptibleSupervisor(scope = this, interruptController = interruptController)

        supervisor.runPhases(
            listOf(
                {
                    initialiseAndSyncPhase(
                        scope = this,
                        indexers = indexers,
                        interruptController = interruptController,
                    )
                },
                {
                    processBlocksPhase(
                        scope = this,
                        batchSize = batchSize,
                        thorClient = thorClient,
                        executionGroups = executionGroups,
                        interruptController = interruptController,
                    )
                },
            ),
        )
    }
}

suspend fun initialiseAndSyncPhase(
    scope: CoroutineScope,
    indexers: List<Indexer>,
    interruptController: InterruptController,
) {
    val parentContext = scope.coroutineContext
    val parentJob = parentContext[Job]
    val supervisorJob = parentJob?.let { SupervisorJob(it) } ?: SupervisorJob()
    try {
        withContext(parentContext + supervisorJob) {
            val tasks =
                indexers.map { indexer ->
                    async {
                        runWithInterruptHandling(interruptController) {
                            indexer.initialise()
                            indexer.fastSync()
                        }
                    }
                }
            tasks.awaitAll()
        }
    } finally {
        supervisorJob.cancel()
    }
}

suspend fun processBlocksPhase(
    scope: CoroutineScope,
    batchSize: Int,
    thorClient: ThorClient,
    executionGroups: List<List<Indexer>>,
    interruptController: InterruptController,
) {
    val stream =
        PrefetchingBlockStream(
            scope = scope,
            batchSize = batchSize,
            currentBlockProvider = {
                executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
            },
            thorClient = thorClient,
        )
    GroupSupervisor(scope, stream, executionGroups, interruptController).run()
}
