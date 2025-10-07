package org.vechain.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.orchestration.OrchestrationUtils.topologicalOrder
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.PrefetchingBlockStream
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

open class IndexerRunner {
    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        @Suppress("unused")
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val indexerOrchestrator = IndexerRunner()

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
    ): Unit = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        fastSyncAll(indexers)
        runAllIndexers(indexers, thorClient, batchSize)
    }

    /**
     * Initialises and fast syncs all indexers concurrently with retry logic.
     *
     * If an indexer fails during initialization or fast sync, it will log the error, wait for 1
     * second, and then retry until it succeeds or the coroutine is cancelled.
     *
     * @param indexers The list of indexers to initialise and fast sync.
     */
    suspend fun fastSyncAll(indexers: List<Indexer>) {
        coroutineScope {
            val tasks =
                indexers.map { indexer ->
                    async {
                        retryUntilSuccess {
                            indexer.initialise()
                            indexer.fastSync()
                        }
                    }
                }
            tasks.awaitAll()
        }
    }

    suspend fun runAllIndexers(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
    ) {
        coroutineScope {
            val executionGroups = topologicalOrder(indexers)
            if (executionGroups.isEmpty()) return@coroutineScope

            val blockStream = createBlockStream(this, executionGroups, thorClient, batchSize)
            val subscription = blockStream.subscribe()

            launch {
                try {
                    processBlocks(subscription, executionGroups)
                } finally {
                    subscription.close()
                }
            }
        }
    }

    private fun createBlockStream(
        scope: CoroutineScope,
        executionGroups: List<List<Indexer>>,
        thorClient: ThorClient,
        batchSize: Int
    ): PrefetchingBlockStream {
        return PrefetchingBlockStream(
            scope = scope,
            batchSize = batchSize,
            currentBlockProvider = {
                executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
            },
            thorClient = thorClient,
        )
    }

    private suspend fun processBlocks(
        subscription: BlockStreamSubscription,
        executionGroups: List<List<Indexer>>
    ) = coroutineScope {
        while (isActive) {
            val block = fetchNextBlock(subscription) ?: continue
            processBlockAcrossGroups(block, executionGroups)
        }
    }

    private suspend fun fetchNextBlock(subscription: BlockStreamSubscription): Block? {
        return try {
            subscription.next()
        } catch (e: Exception) {
            logger.error("Error fetching next block from stream", e)
            delay(1000L)
            null
        }
    }

    private suspend fun processBlockAcrossGroups(
        block: Block,
        executionGroups: List<List<Indexer>>
    ) {
        for (group in executionGroups) {
            processGroupBlock(group, block)
        }
    }

    private suspend fun processGroupBlock(
        group: List<Indexer>,
        block: Block,
    ) {
        for (indexer in group) {
            processIndexerBlock(indexer, block)
        }
    }

    private suspend fun processIndexerBlock(indexer: Indexer, block: Block) {
        val currentNumber = indexer.getCurrentBlockNumber()

        when {
            currentNumber == block.number -> {
                retryUntilSuccess { indexer.processBlock(block) }
            }
            currentNumber > block.number -> {
                // Indexer already processed this block, skip
            }
            else -> {
                throw IllegalStateException(
                    "Indexer ${indexer.name} is behind the current block ${block.number}"
                )
            }
        }
    }

    private suspend fun retryUntilSuccess(operation: suspend () -> Unit) {
        while (true) {
            try {
                operation()
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Operation failed, retrying...", e)
                delay(1000L)
            }
        }
    }
}
