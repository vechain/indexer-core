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
import org.vechain.indexer.thor.PrefetchingBlockStream
import org.vechain.indexer.thor.client.ThorClient

open class IndexerRunner {
    protected val logger: Logger =
        LoggerFactory.getLogger(
            this::class.java,
        )

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

        // 1. Initialise all indexers and perform fast sync
        fastSyncAll(indexers)

        // 2. Start processing blocks
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
    suspend fun fastSyncAll(
        indexers: List<Indexer>,
    ) {
        coroutineScope {
            val tasks =
                indexers.map { indexer ->
                    async {
                        while (true) {
                            try {
                                indexer.initialise()
                                indexer.fastSync()
                                break // Success, exit loop
                            } catch (e: CancellationException) {
                                // Shutdown was requested so don't retry
                                return@async
                            } catch (e: Exception) {
                                // An error occurred, log, backoff and retry
                                logger.error(
                                    "Error during sync for ${indexer.name}, retrying...",
                                    e,
                                )
                                delay(1000L) // 1 second backoff
                            }
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
            // 1. Arrange the indexers into groups based on their dependencies
            val executionGroups = topologicalOrder(indexers)

            // 2. Start the block stream
            val stream =
                PrefetchingBlockStream(
                        scope = this,
                        batchSize = batchSize,
                        currentBlockProvider = {
                            executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
                        },
                        thorClient = thorClient,
                    )
                    .subscribe()

            // 3. Start processing blocks
            launch {
                while (isActive) {
                    // i) Get the next block from the stream
                    val block =
                        try {
                            stream.next()
                        } catch (e: Exception) {
                            // An error occurred, log, backoff and retry
                            logger.error("Error fetching next block from stream", e)
                            delay(1000L)
                            return@launch
                        }

                    // ii) Process the block with all indexer groups in parallel
                    coroutineScope {
                        executionGroups.forEach { group ->
                            launch {
                                // Throw an error if any of the indexers are behind the current
                                // block
                                group.forEach { indexer ->
                                    require(indexer.getCurrentBlockNumber() >= block.number) {
                                        "Indexer ${indexer.name} is behind the current block ${block.number}"
                                    }
                                }

                                group
                                    .filter { it.getCurrentBlockNumber() == block.number }
                                    .forEach { indexer ->
                                        while (true) {
                                            try {
                                                indexer.processBlock(block)
                                                break // Success, exit retry loop
                                            } catch (e: CancellationException) {
                                                // Shutdown was requested so don't retry
                                                return@launch
                                            } catch (e: Exception) {
                                                // An error occurred, log, backoff and retry
                                                logger.error(
                                                    "Error processing block ${block.number} for ${indexer.name}, retrying...",
                                                    e,
                                                )
                                                delay(1000L) // 1 second backoff before retry
                                            }
                                        }
                                    }
                            }
                        }
                    }
                }
            }
        }
    }
}
