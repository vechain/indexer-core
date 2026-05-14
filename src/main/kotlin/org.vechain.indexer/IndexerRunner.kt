package org.vechain.indexer

import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
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
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.utils.ClauseIndexMapping
import org.vechain.indexer.utils.ClauseUtils.buildClauseListWithMapping
import org.vechain.indexer.utils.IndexerOrderUtils.proximityGroups
import org.vechain.indexer.utils.IndexerOrderUtils.topologicalOrder
import org.vechain.indexer.utils.retryOnFailure

class IndexerRunner(private val timeSource: TimeSource = TimeSource.Monotonic) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private val FAST_SYNC_PENDING_STATUSES =
            setOf(Status.NOT_INITIALISED, Status.READY_TO_FAST_SYNC, Status.FAST_SYNCING)
        private val SYNC_READY_STATUSES =
            setOf(Status.READY_TO_SYNC, Status.SYNCING, Status.FULLY_SYNCED)

        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
            proximityThreshold: Long = 500_000L,
            reshuffleInterval: Duration = 10.minutes,
            catchUpInterval: Duration = 5.minutes,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val runner = IndexerRunner(TimeSource.Monotonic)

            return scope.launch {
                runner.run(
                    indexers = indexers,
                    batchSize = blockBatchSize,
                    thorClient = thorClient,
                    proximityThreshold = proximityThreshold,
                    reshuffleInterval = reshuffleInterval,
                    catchUpInterval = catchUpInterval,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<Indexer>,
        batchSize: Int,
        thorClient: ThorClient,
        proximityThreshold: Long,
        reshuffleInterval: Duration,
        catchUpInterval: Duration,
    ): Unit = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        logger.info("Starting ${indexers.size} Indexer ${indexers.map { it.name }}")

        while (isActive) {
            try {
                catchUp(
                    indexers,
                    thorClient,
                    batchSize,
                    proximityThreshold,
                    catchUpInterval,
                )
                runWithProximityGroups(
                    indexers,
                    thorClient,
                    batchSize,
                    proximityThreshold,
                    reshuffleInterval,
                )
            } catch (e: ReorgException) {
                logger.error("Reorg detected, restarting all indexers", e)
                // Exception caught, job will complete normally and loop will restart
            }
        }
    }

    /**
     * Drives indexers toward the steady state in which all are sync-ready. Each iteration:
     * 1. Initialises non-fast-syncable indexers whose fast-syncable ancestors have all finished.
     * 2. Classifies indexers into three groups by status:
     *     - **group 1**: fast-syncable indexers still pending fast sync.
     *     - **group 2**: indexers ready for the regular sync loop.
     *     - **group 3**: non-fast-syncable indexers blocked behind a fast-syncing dependency.
     * 3. Returns once groups 1 and 3 are empty (steady state).
     * 4. Otherwise runs group 1's fast sync alongside group 2's sync, capped at [catchUpInterval]
     *    so newly-eligible indexers join on the next pass.
     */
    private suspend fun catchUp(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
        proximityThreshold: Long,
        catchUpInterval: Duration,
    ) {
        while (true) {
            initialiseUnblockedIndexers(indexers)

            val (group1, group2, group3) = classify(indexers)
            logCatchUpGroups(group1, group2, group3)

            if (group1.isEmpty() && group3.isEmpty()) {
                logger.info("All ${indexers.size} indexers caught up — entering steady-state run")
                return
            }

            coroutineScope {
                if (group1.isNotEmpty()) {
                    launch { initialiseAndSyncFor(group1, catchUpInterval) }
                }
                if (group2.isNotEmpty()) {
                    launch {
                        runWithProximityGroupsFor(
                            group2,
                            thorClient,
                            batchSize,
                            proximityThreshold,
                            catchUpInterval,
                        )
                    }
                }
            }
            // Recovery for both groups happens at the top of their respective entry points.
            // The deadline only stops new work from being started; in-flight work finishes before
            // this loop reclassifies the indexers.
        }
    }

    /**
     * Initialises any non-fast-syncable indexer whose fast-syncable ancestors have all completed
     * fast sync. Indexers still blocked by an in-flight fast sync are left untouched and will be
     * reconsidered on the next [catchUp] iteration.
     */
    internal suspend fun initialiseUnblockedIndexers(indexers: List<Indexer>) {
        val toInit = indexers.filter { it.canBeInitialisedNow() }
        if (toInit.isNotEmpty()) initialise(toInit)
    }

    internal fun Indexer.canBeInitialisedNow(): Boolean =
        this !is FastSyncableIndexer &&
            getStatus() == Status.NOT_INITIALISED &&
            !hasFastSyncingAncestor()

    internal fun Indexer.hasFastSyncingAncestor(): Boolean {
        var current = dependsOn
        while (current != null) {
            if (
                current is FastSyncableIndexer && current.getStatus() in FAST_SYNC_PENDING_STATUSES
            ) {
                return true
            }
            current = current.dependsOn
        }
        return false
    }

    internal fun classify(
        indexers: List<Indexer>
    ): Triple<List<FastSyncableIndexer>, List<Indexer>, List<Indexer>> {
        val group1 = mutableListOf<FastSyncableIndexer>()
        val group2 = mutableListOf<Indexer>()
        val group3 = mutableListOf<Indexer>()
        for (indexer in indexers) {
            val status = indexer.getStatus()
            when {
                indexer is FastSyncableIndexer && status in FAST_SYNC_PENDING_STATUSES ->
                    group1.add(indexer)
                status in SYNC_READY_STATUSES -> group2.add(indexer)
                status == Status.NOT_INITIALISED -> group3.add(indexer)
            // SHUT_DOWN: skip
            }
        }
        return Triple(group1, group2, group3)
    }

    /**
     * Initialises all indexers concurrently with retry logic (no fast sync).
     *
     * @param indexers The list of indexers to initialise.
     */
    suspend fun initialise(indexers: List<Indexer>) {
        if (indexers.isEmpty()) return
        logger.info("Initialising ${indexers.size} indexers...")
        coroutineScope {
            val tasks =
                indexers.map { indexer -> async { retryOnFailure { indexer.initialise() } } }
            tasks.awaitAll()
        }
    }

    /**
     * Initialises and fast syncs all indexers concurrently with retry logic.
     *
     * @param indexers The list of indexers to initialise and fast sync.
     */
    suspend fun initialiseAndSync(indexers: List<FastSyncableIndexer>) {
        initialiseAndSync(indexers, deadlineMark = null)
    }

    internal suspend fun initialiseAndSyncFor(
        indexers: List<FastSyncableIndexer>,
        duration: Duration,
    ) {
        val deadlineMark = timeSource.markNow() + duration
        initialiseAndSync(indexers, deadlineMark)
    }

    private suspend fun initialiseAndSync(
        indexers: List<FastSyncableIndexer>,
        deadlineMark: TimeMark?,
    ) {
        logger.info("Initialising and syncing indexers...")
        coroutineScope {
            val tasks =
                indexers.map { indexer -> async { initialiseAndSync(indexer, deadlineMark) } }
            tasks.awaitAll()
        }
    }

    /**
     * Initialises and fast syncs a single indexer with retry logic.
     *
     * Only calls [Indexer.initialise] (which performs a rollback) when the indexer has not yet been
     * initialised. On subsequent re-entry — for example after a deadline expiry or a reorg restart
     * — calls [Indexer.refreshState] instead so we don't roll back already-persisted progress.
     *
     * @param indexer The indexer to initialise and fast sync.
     */
    private suspend fun initialiseAndSync(
        indexer: FastSyncableIndexer,
        deadlineMark: TimeMark?,
    ) {
        logger.info("Initialising and syncing indexer ${indexer.name}...")
        retryOnFailure {
            if (deadlineMark?.hasNotPassedNow() == false) return@retryOnFailure
            ensureReady(indexer)
            if (indexer is LogsIndexer) {
                if (deadlineMark != null) {
                    indexer.fastSyncUntil(deadlineMark)
                } else {
                    indexer.fastSync()
                }
            } else {
                if (deadlineMark?.hasNotPassedNow() == false) return@retryOnFailure
                indexer.fastSync()
            }
        }
    }

    private fun ensureReady(indexer: Indexer) {
        if (indexer.getStatus() == Status.NOT_INITIALISED) {
            indexer.initialise()
        } else {
            indexer.refreshState()
        }
    }

    /**
     * Refreshes in-memory state for already-initialised indexers without rolling back. Falls back
     * to a full [Indexer.initialise] for any indexer still in [Status.NOT_INITIALISED].
     */
    suspend fun refreshState(indexers: List<Indexer>) {
        if (indexers.isEmpty()) return
        coroutineScope {
            val tasks =
                indexers.map { indexer -> async { retryOnFailure { ensureReady(indexer) } } }
            tasks.awaitAll()
        }
    }

    suspend fun runWithProximityGroups(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
        proximityThreshold: Long,
        reshuffleInterval: Duration,
    ) {
        // Refresh in-memory state on every entry. This recovers from mid-block cancellation (for
        // example, a reorg cancelling sibling catch-up work) and from stale in-memory state after
        // a reorg restart (handleReorg rolls back the processor but leaves currentBlockNumber and
        // previousBlock untouched). Refresh — not initialise — because already-initialised
        // indexers must not roll back persisted progress on re-entry.
        refreshState(indexers)
        while (true) {
            if (logger.isDebugEnabled) {
                logger.debug("Evaluating proximity groups for ${indexers.size} indexers")
            }
            val groups = proximityGroups(indexers, proximityThreshold)
            logProximityGroups(groups, indexers.size, proximityThreshold)
            if (groups.size <= 1) {
                // Steady state: once all indexers are close enough, there is no need to exit for
                // regrouping.
                runIndexers(indexers, thorClient, batchSize)
                return
            }
            val deadlineMark = timeSource.markNow() + reshuffleInterval
            coroutineScope {
                groups.forEach { group ->
                    launch { runIndexers(group, thorClient, batchSize, deadlineMark) }
                }
            }
            // All groups completed naturally when deadline passed; loop to reshuffle
        }
    }

    internal suspend fun runWithProximityGroupsFor(
        indexers: List<Indexer>,
        thorClient: ThorClient,
        batchSize: Int,
        proximityThreshold: Long,
        duration: Duration,
    ) {
        val deadlineMark = timeSource.markNow() + duration
        refreshState(indexers)
        if (deadlineMark.hasNotPassedNow()) {
            if (logger.isDebugEnabled) {
                logger.debug("Evaluating proximity groups for ${indexers.size} indexers")
            }
            val groups = proximityGroups(indexers, proximityThreshold)
            logProximityGroups(groups, indexers.size, proximityThreshold)
            val groupsToRun = if (groups.size <= 1) listOf(indexers) else groups
            coroutineScope {
                groupsToRun.forEach { group ->
                    launch { runIndexers(group, thorClient, batchSize, deadlineMark) }
                }
            }
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

            logExecutionGroups(executionGroups)

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
                    val initialTimestampSeconds = seedTimestampForStart(thorClient, startBlock)

                    fetcher.prefetchBlocksInOrder(
                        startBlock = startBlock,
                        maxBatchSize = batchSize,
                        deadlineMark = deadlineMark,
                        initialTimestampSeconds = initialTimestampSeconds,
                    ) { preparedBlock ->
                        groupChannels.forEach { channel -> channel.send(preparedBlock) }
                    }
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
        // `group` arrives in topological order (parents before children), so by the time we read
        // jobs[parent], it has been populated. Siblings without a dependency on each other run in
        // parallel; the awaitAll at end-of-block keeps the group block-synchronised so the next
        // prepared block is only consumed after every indexer has finished the current one.
        val groupSet = group.toSet()
        for (preparedBlock in channel) {
            coroutineScope {
                val jobs = mutableMapOf<Indexer, Deferred<Unit>>()
                for (indexer in group) {
                    val parentJob = indexer.dependsOn?.takeIf { it in groupSet }?.let { jobs[it] }
                    jobs[indexer] = async {
                        parentJob?.await()
                        processIndexerBlock(indexer, preparedBlock, clauseIndexMapping)
                    }
                }
                jobs.values.awaitAll()
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
                // Bump liveness so the health reporter doesn't flag head-synced indexers as DOWN
                // while the fetcher is gated by a slower indexer in the same proximity group.
                if (indexer is BlockIndexer) indexer.markSkipped()
                if (logger.isDebugEnabled) {
                    logger.debug(
                        "Skipping block ${block.number} for ${indexer.name} (already at $currentNumber)"
                    )
                }
            }
            else -> {
                throw IllegalStateException(
                    "Indexer ${indexer.name} is behind the current block ${block.number}"
                )
            }
        }
    }

    // Seeds the prefetcher's lastBlockTimestamp from the block immediately preceding startBlock.
    // calculateWindowSize uses lastBlockTimestamp to shrink the prefetch window near the tip; on
    // every re-entry into runIndexers (e.g., catchUp/reshuffle ticks) the BlockFetcher is fresh,
    // so without seeding the first iteration always fans out maxBatchSize parallel fetches.
    //
    // The seed is an optimisation, not a correctness requirement: on failure we return null and
    // the prefetcher falls back to its previous behaviour of starting with full parallelism.
    private suspend fun seedTimestampForStart(thorClient: ThorClient, startBlock: Long): Long? {
        if (startBlock <= 0) return null
        return try {
            thorClient.getBlock(BlockRevision.Number(startBlock - 1)).timestamp
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.warn("Failed to seed prefetch timestamp for block ${startBlock - 1}", e)
            null
        }
    }

    // Logging functions
    private fun logCatchUpGroups(
        group1: List<Indexer>,
        group2: List<Indexer>,
        group3: List<Indexer>,
    ) {
        val total = group1.size + group2.size + group3.size
        val groupSummary = buildString {
            appendLine("Catch-up groups: $total indexers")
            appendLine("  fastSyncPending (${group1.size}): ${group1.map { it.name }}")
            appendLine("  syncReady (${group2.size}): ${group2.map { it.name }}")
            appendLine("  blocked (${group3.size}): ${group3.map { it.name }}")
        }
        logger.info(groupSummary.trimEnd())
    }

    private fun logExecutionGroups(executionGroups: List<List<Indexer>>) {
        if (logger.isDebugEnabled) {
            val groupSummary = buildString {
                appendLine(
                    "Execution groups: ${executionGroups.size} groups, ${executionGroups.flatten().size} indexers"
                )
                executionGroups.forEachIndexed { i, g ->
                    appendLine("  Group ${i + 1} (${g.size} indexers): ${g.map { it.name }}")
                }
            }
            logger.debug(groupSummary.trimEnd())
        }
    }

    private fun logProximityGroups(
        groups: List<List<Indexer>>,
        totalIndexers: Int,
        proximityThreshold: Long,
    ) {
        val groupSummary = buildString {
            appendLine(
                "Proximity groups: ${groups.size} groups, $totalIndexers indexers, threshold=$proximityThreshold"
            )
            groups.forEachIndexed { i, g ->
                val blockRange =
                    "${g.minOf { it.getCurrentBlockNumber() }}..${g.maxOf { it.getCurrentBlockNumber() }}"
                appendLine(
                    "  Group ${i + 1} (${g.size} indexers, blocks $blockRange): ${g.map { it.name }}"
                )
            }
        }
        logger.info(groupSummary.trimEnd())
    }
}
