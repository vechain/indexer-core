package org.vechain.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.utils.IndexerOrderUtils.topologicalOrder

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

        logger.info("Starting ${indexers.size} Indexer ${indexers.map { it.name }}")

        initialiseAndSyncAll(indexers)
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
    suspend fun initialiseAndSyncAll(indexers: List<Indexer>) {
        logger.info("Initialising and syncing indexers...")
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
        logger.info("Running indexers...")
        coroutineScope {
            val executionGroups = topologicalOrder(indexers)
            if (executionGroups.isEmpty()) return@coroutineScope

            // Create a channel for each group to receive blocks
            val groupChannels = executionGroups.map { Channel<Block>(capacity = batchSize) }

            // Launch a coroutine for each group to process blocks
            executionGroups.forEachIndexed { groupIndex, group ->
                launch { processGroupBlocks(group, groupChannels[groupIndex]) }
            }

            // Main block fetcher and distributor
            launch {
                try {
                    var blockNumber = executionGroups.flatten().minOf { it.getCurrentBlockNumber() }

                    while (isActive) {
                        val block = fetchBlock(thorClient, blockNumber)

                        // Send block to all groups in parallel
                        groupChannels.forEach { channel -> channel.send(block) }

                        blockNumber++
                    }
                } finally {
                    groupChannels.forEach { it.close() }
                }
            }
        }
    }

    private suspend fun fetchBlock(thorClient: ThorClient, blockNumber: Long): Block {
        return retryUntilSuccess { thorClient.waitForBlock(blockNumber) }
    }

    private suspend fun processGroupBlocks(group: List<Indexer>, channel: Channel<Block>) {
        for (block in channel) {
            // Process all indexers in the group in parallel
            coroutineScope {
                group.map { indexer -> async { processIndexerBlock(indexer, block) } }.awaitAll()
            }
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

    private suspend fun <T> retryUntilSuccess(operation: suspend () -> T): T {
        while (true) {
            try {
                return operation()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Operation failed, retrying...", e)
                delay(1000L)
            }
        }
    }
}
