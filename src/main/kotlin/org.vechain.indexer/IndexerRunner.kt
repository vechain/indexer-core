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
import org.vechain.indexer.exception.StuckBlockException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.utils.ClauseIndexMapping
import org.vechain.indexer.utils.ClauseUtils.buildClauseListWithMapping
import org.vechain.indexer.utils.IndexerOrderUtils.proximityGroups
import org.vechain.indexer.utils.IndexerOrderUtils.topologicalOrder
import org.vechain.indexer.utils.retryOnFailure
import org.vechain.indexer.utils.retryOnFailureBounded

class IndexerRunner(private val timeSource: TimeSource = TimeSource.Monotonic) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private val FAST_SYNC_PENDING_STATUSES =
            setOf(Status.NOT_INITIALISED, Status.READY_TO_FAST_SYNC, Status.FAST_SYNCING)
        private val SYNC_READY_STATUSES =
            setOf(Status.READY_TO_SYNC, Status.SYNCING, Status.FULLY_SYNCED)

        // Caps per-block retry wall-time at ~3 minutes with the 1s→30s exponential backoff in
        // retryOnFailureBounded. Long enough to ride out Mongo failovers and brief network
        // partitions; short enough that a genuinely poisoned block escapes to the recovery path.
        private const val MAX_BLOCK_PROCESS_ATTEMPTS = 10

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

        bypassFastSyncForIndexersWithDependants(indexers)

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
            } catch (e: StuckBlockException) {
                logger.error("Stuck block recovered, restarting all indexers", e)
                // Recovery (rollback + cursor reset) already happened before the throw;
                // the loop just needs to re-enter so the indexer re-fetches the block.
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
        var firstPass = true
        while (true) {
            initialiseUnblockedIndexers(indexers)
            alignDependencyTargets(indexers)
            if (firstPass) {
                warnChildAheadOfParent(indexers)
                firstPass = false
            }

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

    /**
     * Initialises any fast-syncable indexer that has dependants and transitions it past fast sync
     * without running it. A fast-synced parent would land its dependents at the configured start
     * block while it sits thousands of blocks ahead — every dependent read of the parent's table
     * would observe a future state. Bypassing lets dependency edges run on deterministic,
     * block-by-block timelines (slower than fast sync, but the only deterministic option once
     * dependants exist).
     */
    internal suspend fun bypassFastSyncForIndexersWithDependants(indexers: List<Indexer>) {
        val withDependants = indexersWithDependants(indexers)
        val toBypass =
            indexers.filterIsInstance<FastSyncableIndexer>().filter { it in withDependants }
        if (toBypass.isEmpty()) return
        for (indexer in toBypass) {
            logger.warn(
                "Indexer {} is fast-syncable but has dependants; fast sync will be skipped to " +
                    "keep dependency ordering on a deterministic block-by-block timeline.",
                indexer.name,
            )
        }
        val toInit = toBypass.filter { it.getStatus() == Status.NOT_INITIALISED }
        if (toInit.isNotEmpty()) initialise(toInit)
        toBypass.forEach { it.bypassFastSync() }
    }

    /**
     * Rolls ancestors back to the level of a behind-descendant so the dependency chain can run in
     * lockstep from a common point.
     *
     * Alignment is one-directional: only parents are pulled down to a behind-child, never the other
     * way around. A child that is ahead of its parent is left in place; the runtime's
     * `processIndexerBlock` skip path handles the gap while the parent catches up, and the
     * `parentJob.await()` in `processGroupBlocks` re-establishes same-block ordering once they
     * meet. This is the deliberate flip from the previous (≤10.x) contract — see
     * `docs/MIGRATION-11.0.0.md`. Consumers whose `process(...)` reads parent persisted state must
     * now manage their own rollback when they resync a parent, because the library will no longer
     * cascade the rollback to dependants. [warnChildAheadOfParent] surfaces the condition at
     * startup so operators notice when a child has been left ahead.
     *
     * Alignment is computed from dependency edges, not from the minimum block of the whole
     * connected component. A low descendant can pull its ancestor chain back only as far as each
     * ancestor's configured `startBlock`, so unrelated branches are not forced below the block
     * where their parent can actually participate.
     *
     * Only indexers with a persisted last-synced block are candidates for rollback. An unpersisted
     * indexer sitting above its target is at its configured `startBlock` (either a delayed
     * dependant or a pre-dependency-start consumer) — that's a legitimate configuration, not drift,
     * and rolling it back would erase the user's intent. The runtime's skip path on
     * `processIndexerBlock` handles the start-block gap once the fetcher catches up.
     *
     * Iterates to a fixed point so processors that persist sparsely (and may land below an
     * alignment target after a rollback) still converge.
     *
     * If an indexer's processor cannot honour the rollback (typically because its retention is
     * shallower than the requested depth), its [BlockIndexer.alignToBlock] throws. Such failures
     * are collected per call and surfaced once as a single [IllegalStateException] listing every
     * indexer the operator needs to drop, so a topology change requires only one
     * intervene-and-restart cycle.
     */
    internal fun alignDependencyTargets(indexers: List<Indexer>) {
        val initialised = indexers.filter { it.getStatus() != Status.NOT_INITIALISED }
        if (initialised.size < 2) return
        val failures = mutableListOf<AlignmentFailure>()
        val failedNames = mutableSetOf<String>()
        var changed = true
        while (changed) {
            changed = false
            val targets = dependencyAlignmentTargets(initialised)
            for (indexer in initialised) {
                if (indexer.name in failedNames) continue
                val target = targets.getValue(indexer)
                val current = indexer.getCurrentBlockNumber()
                if (
                    current > target &&
                        indexer is BlockIndexer &&
                        indexer.getLastSyncedBlock() != null
                ) {
                    logger.warn(
                        "Aligning indexer {} from block {} back to {} to keep dependency " +
                            "ordering deterministic.",
                        indexer.name,
                        current,
                        target,
                    )
                    try {
                        indexer.alignToBlock(target)
                        changed = true
                    } catch (e: IllegalStateException) {
                        failures.add(AlignmentFailure(indexer.name, current, target, e))
                        failedNames.add(indexer.name)
                    }
                }
            }
        }
        if (failures.isNotEmpty()) throw alignmentFailureException(failures)
    }

    private fun dependencyAlignmentTargets(indexers: List<Indexer>): Map<Indexer, Long> {
        val indexerSet = indexers.toSet()
        val targets = indexers.associateWith { it.getCurrentBlockNumber() }.toMutableMap()

        // Per edge (child -> parent), only the parent's target is lowered toward the child, never
        // below the parent's own startBlock:
        //   parent_target := min(parent_target, max(child_target, parent.startBlock))
        // A child ahead of its parent is intentionally not pulled back — see the KDoc on
        // [alignDependencyTargets] and `docs/MIGRATION-11.0.0.md`. Iterate to a fixed point so
        // constraints propagate across multi-hop chains and across sibling subtrees that meet at
        // a shared ancestor.
        var changed = true
        while (changed) {
            changed = false
            for (child in indexers) {
                val parent = child.dependsOn?.takeIf { it in indexerSet } ?: continue

                val parentTarget = targets.getValue(parent)
                val childTarget = targets.getValue(child)
                val alignedParentTarget = minOf(parentTarget, maxOf(childTarget, parent.startBlock))
                if (alignedParentTarget < parentTarget) {
                    targets[parent] = alignedParentTarget
                    changed = true
                }
            }
        }

        return targets
    }

    /**
     * Emits a WARN for each `dependsOn` edge where a persisted child sits above its parent. Prior
     * to 11.x this condition triggered a rollback of the child to the parent's level; the new
     * contract leaves the child in place and relies on the runtime's skip path to converge once the
     * parent catches up.
     *
     * The warning is informational. It exists so operators who resync a parent notice the
     * dependants that the library is no longer cascading the rollback to. Consumers whose
     * `process(...)` reads parent persisted state must decide whether to roll the dependant back
     * manually; consumers whose dependants only need same-block ordering can ignore the warning.
     *
     * Unpersisted children above their parent are not flagged — that's a legitimate delayed-
     * dependant configuration and not drift caused by an out-of-band resync.
     */
    internal fun warnChildAheadOfParent(indexers: List<Indexer>) {
        val indexerSet = indexers.toSet()
        for (child in indexers) {
            if (child.getStatus() == Status.NOT_INITIALISED) continue
            if (child.getLastSyncedBlock() == null) continue
            val parent = child.dependsOn?.takeIf { it in indexerSet } ?: continue
            if (parent.getStatus() == Status.NOT_INITIALISED) continue
            val childBlock = child.getCurrentBlockNumber()
            val parentBlock = parent.getCurrentBlockNumber()
            if (childBlock <= parentBlock) continue
            logger.warn(
                "Indexer '{}' is at block {} while its dependency '{}' is at block {} " +
                    "(child ahead by {} blocks). The runtime will not roll '{}' back; '{}' will " +
                    "catch up and same-block ordering will resume from there. If '{}'.process(...) " +
                    "reads '{}'s persisted state and you intended that data to be invalidated, " +
                    "roll '{}' back manually before restart.",
                child.name,
                childBlock,
                parent.name,
                parentBlock,
                childBlock - parentBlock,
                child.name,
                parent.name,
                child.name,
                parent.name,
                child.name,
            )
        }
    }

    private data class AlignmentFailure(
        val indexerName: String,
        val current: Long,
        val target: Long,
        val cause: IllegalStateException,
    )

    private fun alignmentFailureException(failures: List<AlignmentFailure>): IllegalStateException {
        val message = buildString {
            appendLine("Cannot align ${failures.size} indexer(s) to their dependency target block:")
            failures.forEach {
                appendLine(
                    "  - '${it.indexerName}' is at block ${it.current}, cannot roll back to ${it.target}"
                )
            }
            append(
                "Drop persisted state for these indexers and restart to proceed. The processor's " +
                    "rollback retention is likely insufficient for this depth of realignment."
            )
        }
        val ex = IllegalStateException(message, failures.first().cause)
        failures.drop(1).forEach { ex.addSuppressed(it.cause) }
        return ex
    }

    private fun indexersWithDependants(indexers: List<Indexer>): Set<Indexer> {
        val result = mutableSetOf<Indexer>()
        for (i in indexers) {
            var current = i.dependsOn
            while (current != null) {
                result.add(current)
                current = current.dependsOn
            }
        }
        return result
    }

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
                retryOnFailureBounded(
                    maxAttempts = MAX_BLOCK_PROCESS_ATTEMPTS,
                    onGiveUp = { cause ->
                        val message =
                            "Indexer ${indexer.name} stuck at block ${block.number} after " +
                                "$MAX_BLOCK_PROCESS_ATTEMPTS attempts; rolling back and restarting."
                        logger.error(message, cause)
                        if (indexer is BlockIndexer) indexer.recoverFromStuckBlock()
                        throw StuckBlockException(message, cause)
                    },
                ) {
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
