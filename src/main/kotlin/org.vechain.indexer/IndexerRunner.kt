package org.vechain.indexer

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
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult
import org.vechain.indexer.utils.IndexerOrderUtils.topologicalOrder
import org.vechain.indexer.utils.retryOnFailure

/**
 * Data class holding a block with its pre-fetched inspection results. Used to pipeline block
 * fetching and contract calls ahead of processing.
 */
data class PreparedBlock(val block: Block, val inspectionResults: List<InspectionResult>)

/**
 * Maps each indexer to the indices of its clauses in the combined clause list. Used to extract the
 * correct inspection results for each indexer from the batched response.
 */
typealias ClauseIndexMapping = Map<Indexer, List<Int>>

open class IndexerRunner {
    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private const val BLOCK_INTERVAL_SECONDS = 10L

        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val runner = IndexerRunner()

            return scope.launch {
                runner.run(
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

        while (isActive) {
            try {
                initialiseAndSyncAll(indexers)
                runAllIndexers(indexers, thorClient, batchSize)
            } catch (e: ReorgException) {
                logger.error("Reorg detected, restarting all indexers", e)
                // Exception caught, job will complete normally and loop will restart
            }
        }
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
                        retryOnFailure {
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

                    // Launch parallel prefetch workers that maintain order
                    prefetchBlocksInOrder(
                        thorClient = thorClient,
                        allClauses = allClauses,
                        startBlock = startBlock,
                        maxBatchSize = batchSize,
                        groupChannels = groupChannels,
                    )
                } finally {
                    groupChannels.forEach { it.close() }
                }
            }
        }
    }

    /**
     * Builds a combined list of unique clauses from all indexers and creates a mapping from each
     * indexer to the indices of its clauses in the combined list.
     *
     * This is necessary because Thor returns inspection results in the same order as the clauses
     * sent, so we need to track which results belong to which indexer.
     */
    internal fun buildClauseListWithMapping(
        indexers: List<Indexer>
    ): Pair<List<Clause>, ClauseIndexMapping> {
        val allClauses = mutableListOf<Clause>()
        val clauseToIndex = mutableMapOf<Clause, Int>()
        val indexerToIndices = mutableMapOf<Indexer, List<Int>>()

        for (indexer in indexers) {
            val clauses = indexer.getInspectionClauses() ?: continue
            val indices = mutableListOf<Int>()

            for (clause in clauses) {
                val existingIndex = clauseToIndex[clause]
                if (existingIndex != null) {
                    // Clause already exists, reuse its index
                    indices.add(existingIndex)
                } else {
                    // New clause, add to list and record index
                    val newIndex = allClauses.size
                    allClauses.add(clause)
                    clauseToIndex[clause] = newIndex
                    indices.add(newIndex)
                }
            }

            indexerToIndices[indexer] = indices
        }

        return Pair(allClauses, indexerToIndices)
    }

    /**
     * Prefetches blocks and their inspection results in parallel while maintaining order. Uses a
     * sliding window of concurrent fetches to hide network latency.
     */
    protected suspend fun prefetchBlocksInOrder(
        thorClient: ThorClient,
        allClauses: List<Clause>,
        startBlock: Long,
        maxBatchSize: Int,
        groupChannels: List<Channel<PreparedBlock>>,
    ) = coroutineScope {
        require(startBlock >= 0) { "startBlock must be >= 0" }
        require(maxBatchSize >= 1) { "maxBatchSize must be >= 1" }

        var nextBlockNumber = startBlock
        var lastBlockTimestamp: Long? = null

        while (isActive) {
            val currentBlock = nextBlockNumber
            val windowSize = calculateWindowSize(lastBlockTimestamp, maxBatchSize)
            logger.debug("Block fetch window size: $windowSize")
            // Launch prefetch for next batch of blocks in parallel
            val deferredBlocks =
                (0 ..< windowSize).map { offset ->
                    val blockNum = currentBlock + offset
                    async { fetchAndPrepareBlock(thorClient, blockNum, allClauses) }
                }

            // Await and send in order
            deferredBlocks.forEach { deferred ->
                val preparedBlock = deferred.await()
                groupChannels.forEach { channel -> channel.send(preparedBlock) }
                nextBlockNumber++
                lastBlockTimestamp = preparedBlock.block.timestamp
            }
        }
    }

    /**
     * Fetches a block and performs inspection calls, returning a PreparedBlock. This combines the
     * network calls so they can be pipelined.
     */
    protected suspend fun fetchAndPrepareBlock(
        thorClient: ThorClient,
        blockNumber: Long,
        allClauses: List<Clause>,
    ): PreparedBlock {
        return retryOnFailure {
            val block = thorClient.waitForBlock(BlockRevision.Number(blockNumber))
            val inspectionResults =
                if (allClauses.isNotEmpty()) {
                    thorClient.inspectClauses(allClauses, BlockRevision.Id(block.id))
                } else {
                    emptyList()
                }
            PreparedBlock(block, inspectionResults)
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

    protected fun calculateWindowSize(
        lastBlockTimestampSeconds: Long?,
        maxPrefetchSize: Int,
    ): Int {
        if (lastBlockTimestampSeconds == null) {
            return maxPrefetchSize
        }
        val nowSeconds = System.currentTimeMillis() / 1000
        val secondsBehind = (nowSeconds - lastBlockTimestampSeconds).coerceAtLeast(0)
        val estimatedBlocksBehind =
            (secondsBehind / BLOCK_INTERVAL_SECONDS).toInt().coerceAtLeast(0) + 1
        return minOf(maxPrefetchSize, estimatedBlocksBehind)
    }
}
