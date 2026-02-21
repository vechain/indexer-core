package org.vechain.indexer

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.exception.RateLimitException
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult
import org.vechain.indexer.utils.IndexerOrderUtils.proximityGroups
import org.vechain.indexer.utils.IndexerOrderUtils.topologicalOrder

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
        private const val DEFAULT_RESHUFFLE_INTERVAL_MS = 300_000L
        private const val INITIAL_RETRY_DELAY_MS = 1_000L
        private const val MAX_RETRY_DELAY_MS = 60_000L
        private const val RATE_LIMIT_DELAY_MS = 30_000L

        @Suppress("unused")
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
            proximityThreshold: Long = 1_000_000L,
            reshuffleIntervalMs: Long = DEFAULT_RESHUFFLE_INTERVAL_MS,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val indexerOrchestrator = IndexerRunner()

            return scope.launch {
                indexerOrchestrator.run(
                    indexers = indexers,
                    batchSize = blockBatchSize,
                    thorClient = thorClient,
                    proximityThreshold = proximityThreshold,
                    reshuffleIntervalMs = reshuffleIntervalMs,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<Indexer>,
        batchSize: Int,
        thorClient: ThorClient,
        proximityThreshold: Long = 1_000_000L,
        reshuffleIntervalMs: Long = DEFAULT_RESHUFFLE_INTERVAL_MS,
    ): Unit = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        logger.info("Starting ${indexers.size} Indexer ${indexers.map { it.name }}")

        while (isActive) {
            try {
                // Phase 1: Init all
                initialiseAll(indexers)

                // Phase 2: Fast sync with early block processing
                fastSyncWithEarlyProcessing(
                    indexers,
                    thorClient,
                    batchSize,
                    proximityThreshold,
                    reshuffleIntervalMs,
                )

                // Phase 3: All synced, dynamic grouping until converged
                runWithDynamicGroups(
                    indexers,
                    thorClient,
                    batchSize,
                    proximityThreshold,
                    reshuffleIntervalMs,
                )
            } catch (e: ReorgException) {
                logger.error("Reorg detected, restarting all indexers", e)
            }
        }
    }

    /**
     * Initialises all indexers concurrently with retry logic.
     *
     * @param indexers The list of indexers to initialise.
     */
    suspend fun initialiseAll(indexers: List<Indexer>) {
        logger.info("Initialising indexers...")
        coroutineScope {
            val tasks =
                indexers.map { indexer -> async { retryUntilSuccess { indexer.initialise() } } }
            tasks.awaitAll()
        }
    }

    /**
     * Launches fast sync for FastSyncable indexers while concurrently running block processing for
     * indexers that are already ready (non-FastSyncable). As each FastSyncable indexer completes
     * its fast sync, it joins the ready set. When all fast syncs complete, early processing is
     * cancelled and control returns (no progress lost since getCurrentBlockNumber() persists).
     */
    suspend fun fastSyncWithEarlyProcessing(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
        proximityThreshold: Long,
        reshuffleIntervalMs: Long = DEFAULT_RESHUFFLE_INTERVAL_MS,
    ) = coroutineScope {
        val readyIndexers = ConcurrentHashMap.newKeySet<Indexer>()
        val firstReady = CompletableDeferred<Unit>()

        // Non-fast-syncable indexers are immediately ready
        indexers.filter { it !is FastSyncable }.forEach { readyIndexers.add(it) }
        if (readyIndexers.isNotEmpty()) firstReady.complete(Unit)

        val fastSyncableIndexers = indexers.filterIsInstance<FastSyncable>()

        if (fastSyncableIndexers.isEmpty()) {
            // Nothing to fast sync, skip this phase
            return@coroutineScope
        }

        // Launch fast sync for FastSyncable indexers
        val fastSyncJobs =
            fastSyncableIndexers.map { indexer ->
                launch {
                    retryUntilSuccess { indexer.fastSync() }
                    readyIndexers.add(indexer as Indexer)
                    firstReady.complete(Unit)
                }
            }

        // Concurrently process blocks for ready indexers (excluding those with unresolved deps)
        val processingJob = launch {
            firstReady.await()
            val snapshot = readyIndexers.toList()
            val snapshotSet = snapshot.toSet()
            val processable =
                snapshot.filter { it.dependsOn == null || it.dependsOn in snapshotSet }
            if (processable.isNotEmpty()) {
                runWithDynamicGroups(
                    processable,
                    thorClient,
                    batchSize,
                    proximityThreshold,
                    reshuffleIntervalMs,
                )
            }
        }

        // Wait for all fast syncs, then cancel early processing
        fastSyncJobs.forEach { it.join() }
        processingJob.cancel()
    }

    /**
     * Runs indexers in dynamic proximity-based groups. Forms groups based on block number
     * proximity, runs each group with its own prefetcher. Periodically reshuffles groups as they
     * converge. When all indexers are in a single group, delegates to [runAllIndexers] (steady
     * state).
     */
    suspend fun runWithDynamicGroups(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
        proximityThreshold: Long,
        reshuffleIntervalMs: Long = DEFAULT_RESHUFFLE_INTERVAL_MS,
    ) = coroutineScope {
        while (isActive) {
            val groups = proximityGroups(indexers, proximityThreshold)
            if (groups.size <= 1) {
                logger.info("All indexers converged into a single group, entering steady state")
                runAllIndexers(indexers, thorClient, batchSize)
                return@coroutineScope
            }

            logger.info(
                "Formed ${groups.size} proximity groups (threshold=$proximityThreshold): " +
                    groups.joinToString("; ") { g ->
                        "[${g.joinToString { "${it.name}@${it.getCurrentBlockNumber()}" }}]"
                    }
            )

            // Run groups concurrently, reshuffle after timeout
            withTimeoutOrNull(reshuffleIntervalMs) {
                coroutineScope {
                    groups.forEach { group ->
                        launch { runAllIndexers(group, thorClient, batchSize) }
                    }
                }
            }

            logger.info("Reshuffling proximity groups after ${reshuffleIntervalMs}ms")
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
        return retryUntilSuccess {
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
                retryUntilSuccess {
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

    private suspend fun <T> retryUntilSuccess(operation: suspend () -> T): T {
        var delayMs = INITIAL_RETRY_DELAY_MS
        while (true) {
            try {
                return operation()
            } catch (e: CancellationException) {
                throw e
            } catch (e: ReorgException) {
                logger.error("Reorg detected, propagating to restart indexers", e)
                throw e
            } catch (e: RateLimitException) {
                logger.warn("Rate limited, backing off {}ms", RATE_LIMIT_DELAY_MS, e)
                delay(RATE_LIMIT_DELAY_MS)
                delayMs = INITIAL_RETRY_DELAY_MS
            } catch (e: Exception) {
                logger.error("Operation failed, retrying in {}ms...", delayMs, e)
                delay(delayMs)
                delayMs = (delayMs * 2).coerceAtMost(MAX_RETRY_DELAY_MS)
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
