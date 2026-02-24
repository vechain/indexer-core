package org.vechain.indexer

import kotlin.time.Duration
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.utils.ClauseIndexMapping
import org.vechain.indexer.utils.ClauseUtils.buildClauseListWithMapping
import org.vechain.indexer.utils.IndexerOrderUtils.topologicalOrder
import org.vechain.indexer.utils.retryOnFailure

class IndexerRunner(private val timeSource: TimeSource = TimeSource.Monotonic) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
            duration: Duration? = null,
            timeSource: TimeSource = TimeSource.Monotonic,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val runner = IndexerRunner(timeSource)

            return scope.launch {
                runner.run(
                    indexers = indexers,
                    batchSize = blockBatchSize,
                    thorClient = thorClient,
                    duration = duration,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<Indexer>,
        batchSize: Int,
        thorClient: ThorClient,
        duration: Duration? = null,
    ): Unit = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        val deadlineMark = duration?.let { timeSource.markNow() + it }

        logger.info("Starting ${indexers.size} Indexer ${indexers.map { it.name }}")

        while (isActive && (deadlineMark == null || deadlineMark.hasNotPassedNow())) {
            try {
                initialiseAndSync(indexers)
                runIndexers(indexers, thorClient, batchSize, deadlineMark)
            } catch (e: ReorgException) {
                logger.error("Reorg detected, restarting all indexers", e)
                // Exception caught, job will complete normally and loop will restart
            }
        }
    }

    /**
     * Initialises and fast syncs all indexers concurrently with retry logic.
     *
     * @param indexers The list of indexers to initialise and fast sync.
     */
    suspend fun initialiseAndSync(indexers: List<Indexer>) {
        logger.info("Initialising and syncing indexers...")
        coroutineScope {
            val tasks = indexers.map { indexer -> async { initialiseAndSync(indexer) } }
            tasks.awaitAll()
        }
    }

    /**
     * Initialises and fast syncs a single indexer with retry logic.
     *
     * @param indexer The indexer to initialise and fast sync.
     */
    private suspend fun initialiseAndSync(indexer: Indexer) {
        logger.info("Initialising and syncing indexer ${indexer.name}...")
        retryOnFailure {
            indexer.initialise()
            indexer.fastSync()
        }
    }

    suspend fun runIndexers(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
        deadlineMark: TimeMark? = null,
    ) {
        require(batchSize >= 1) { "batchSize must be >= 1" }
        logger.info("Running indexers...")
        coroutineScope {
            val executionGroups = topologicalOrder(indexers)
            if (executionGroups.isEmpty()) return@coroutineScope

            // Build combined clause list and track which indices belong to which indexer
            val (allClauses, clauseIndexMapping) = buildClauseListWithMapping(indexers)

            // Create a channel for each group to receive prepared blocks
            val groupChannels = executionGroups.map { Channel<PreparedBlock>(capacity = batchSize) }

            // Launch a coroutine for each group to process blocks
            executionGroups.forEachIndexed { groupIndex, group ->
                launch { processGroupBlocks(group, groupChannels[groupIndex], clauseIndexMapping) }
            }

            // Pipelined block fetcher and distributor
            launch {
                try {
                    val startBlock = executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
                    val fetcher = BlockFetcher(thorClient, allClauses)

                    fetcher.prefetchBlocksInOrder(
                        startBlock = startBlock,
                        maxBatchSize = batchSize,
                        groupChannels = groupChannels,
                        deadlineMark = deadlineMark,
                    )
                } finally {
                    groupChannels.forEach { it.close() }
                }
            }
        }
    }

    private suspend fun processGroupBlocks(
        group: List<Indexer>,
        channel: Channel<PreparedBlock>,
        clauseIndexMapping: ClauseIndexMapping,
    ) {
        for (preparedBlock in channel) {
            // Process indexers in the group sequentially to preserve order
            for (indexer in group) {
                processIndexerBlock(indexer, preparedBlock, clauseIndexMapping)
            }
        }
    }

    private suspend fun processIndexerBlock(
        indexer: Indexer,
        preparedBlock: PreparedBlock,
        clauseIndexMapping: ClauseIndexMapping,
    ) {
        val currentNumber = indexer.getCurrentBlockNumber()
        val block = preparedBlock.block

        when {
            currentNumber == block.number -> {
                retryOnFailure {
                    // Use pre-computed inspection results if indexer has clauses
                    val indexerIndices = clauseIndexMapping[indexer]
                    if (indexerIndices != null) {
                        // Extract only this indexer's results from the batched response
                        val indexerResults =
                            indexerIndices.map { preparedBlock.inspectionResults[it] }
                        indexer.processBlock(block, indexerResults)
                    } else {
                        indexer.processBlock(block)
                    }
                }
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
}
