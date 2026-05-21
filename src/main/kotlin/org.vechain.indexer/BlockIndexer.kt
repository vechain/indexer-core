package org.vechain.indexer

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.coroutines.cancellation.CancellationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.utils.IndexerUtils.ensureStatus

open class BlockIndexer(
    override val name: String,
    protected open val thorClient: ThorClient,
    private val processor: IndexerProcessor,
    override val startBlock: Long,
    private val syncLoggerInterval: Long,
    protected val eventProcessor: CombinedEventProcessor?,
    private val inspectionClauses: List<Clause>?,
    override val dependsOn: Indexer?,
) : Indexer {

    override fun getInspectionClauses(): List<Clause>? = inspectionClauses

    /** The last block that was successfully synchronised */
    private var previousBlock: BlockIdentifier? = null

    override fun getPreviousBlock(): BlockIdentifier? = previousBlock

    protected fun setPreviousBlock(value: BlockIdentifier?) {
        previousBlock = value
    }

    protected val logger: Logger = LoggerFactory.getLogger(name)

    private var status = Status.NOT_INITIALISED

    protected fun setStatus(newStatus: Status) {
        status = newStatus
    }

    override fun getStatus(): Status = status

    private var currentBlockNumber: Long = 0

    override fun getCurrentBlockNumber(): Long = currentBlockNumber

    protected fun setCurrentBlockNumber(value: Long) {
        currentBlockNumber = value
    }

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        internal set

    /**
     * Bumps [timeLastProcessed] without advancing the cursor. Called by the runner when this
     * indexer is already past the block being distributed, so health reporters that key off
     * [timeLastProcessed] don't flag it as stalled while it idles waiting for the slowest indexer
     * in its proximity group to catch up.
     */
    internal fun markSkipped() {
        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
    }

    private var lastInfoLogTime: LocalDateTime = LocalDateTime.MIN

    protected fun setLastInfoLogTime(value: LocalDateTime) {
        lastInfoLogTime = value
    }

    /** Initialises the indexer processing */
    override fun initialise() {
        val lastSyncedBlockNumber = determineStartingBlock()
        rollbackToSafeState(lastSyncedBlockNumber)
        initializeState(lastSyncedBlockNumber)
        logInitialization()
    }

    /**
     * Refreshes in-memory state from the processor without rolling back. Used to recover from
     * mid-block cancellation, where the processor's transaction committed but `currentBlockNumber`
     * had not yet been bumped — re-reading [getLastSyncedBlock] catches the in-memory cursor up to
     * persisted state.
     *
     * Only ever advances `currentBlockNumber`; never rolls it back. Some processors do not save a
     * record on every block (round-aware processors, periodic-rollup processors), so
     * [getLastSyncedBlock] can legitimately lag the in-memory cursor. Rewinding to `lastSynced + 1`
     * in that case would re-process already-processed blocks against stale in-memory state (e.g. a
     * `roundId` counter) and produce out-of-order errors.
     *
     * Reorg recovery does NOT depend on this method: [handleReorg] resets `currentBlockNumber` and
     * `previousBlock` itself after rolling back the processor.
     */
    override fun refreshState() {
        if (status == Status.NOT_INITIALISED) {
            initialise()
            return
        }
        val lastSynced = getLastSyncedBlock() ?: return
        val nextFromPersisted = lastSynced.number + 1
        if (nextFromPersisted > currentBlockNumber) {
            currentBlockNumber = nextFromPersisted
            previousBlock = lastSynced
        }
    }

    /**
     * Determines the starting block number for initialization.
     *
     * @return The last synced block number if available, otherwise the configured start block.
     */
    protected open fun determineStartingBlock(): Long {
        return getLastSyncedBlock()?.number ?: startBlock
    }

    /**
     * Rolls back to a safe state to ensure data integrity.
     *
     * Only rolls back if the block number is greater than zero.
     *
     * @param blockNumber The block number to roll back to.
     */
    protected open fun rollbackToSafeState(blockNumber: Long) {
        if (blockNumber > 0) {
            rollback(blockNumber)
        }
    }

    /**
     * Initializes the indexer state fields.
     *
     * @param blockNumber The block number to initialize from.
     */
    protected open fun initializeState(blockNumber: Long) {
        currentBlockNumber = blockNumber
        previousBlock = calculatePreviousBlock(blockNumber)
        status = Status.READY_TO_SYNC
    }

    /**
     * Calculates the previous block identifier based on the current block number.
     *
     * @param currentBlock The current block number.
     * @return The previous block identifier if it's sequential, null otherwise.
     */
    protected open fun calculatePreviousBlock(currentBlock: Long): BlockIdentifier? {
        val lastBlock = getLastSyncedBlock()
        return if (lastBlock?.id != null && lastBlock.number == currentBlock - 1L) {
            lastBlock
        } else {
            null
        }
    }

    /** Logs the initialization message. */
    protected open fun logInitialization() {
        logger.info("Initialised @ Block: $currentBlockNumber")
    }

    protected suspend fun buildIndexingResult(block: Block): IndexingResult {
        val callResults =
            inspectionClauses?.let { thorClient.inspectClauses(it, BlockRevision.Id(block.id)) }
                ?: emptyList()
        return buildIndexingResultWithCallResults(block, callResults)
    }

    protected fun buildIndexingResultWithCallResults(
        block: Block,
        callResults: List<org.vechain.indexer.thor.model.InspectionResult>
    ): IndexingResult {
        val events = eventProcessor?.processEvents(block) ?: emptyList()
        return IndexingResult.BlockResult(block, events, callResults, status)
    }

    protected fun checkIfShuttingDown() {
        // If shut down throw an error
        if (status == Status.SHUT_DOWN) {
            throw CancellationException("Indexer is shut down")
        }
    }

    override suspend fun processBlock(block: Block) {
        validateProcessingState()
        validateBlockNumber(block)
        updateSyncStatus(block)
        checkForReorg(block)

        processAndUpdateState(block)
    }

    override suspend fun processBlock(
        block: Block,
        inspectionResults: List<org.vechain.indexer.thor.model.InspectionResult>
    ) {
        validateProcessingState()
        validateBlockNumber(block)
        updateSyncStatus(block)
        checkForReorg(block)

        processAndUpdateStateWithResults(block, inspectionResults)
    }

    /**
     * Validates that the indexer is in a valid state for processing blocks.
     *
     * @throws CancellationException if the indexer is shut down.
     * @throws IllegalStateException if the indexer is not in a valid processing state.
     */
    protected open fun validateProcessingState() {
        checkIfShuttingDown()
        ensureStatus(status, setOf(Status.READY_TO_SYNC, Status.SYNCING, Status.FULLY_SYNCED))
    }

    /**
     * Validates that the block number matches the expected current block number.
     *
     * @param block The block to validate.
     * @throws IllegalStateException if the block number doesn't match.
     */
    protected open fun validateBlockNumber(block: Block) {
        if (block.number != currentBlockNumber) {
            throw IllegalStateException(
                "Block number mismatch: expected $currentBlockNumber, got ${block.number}"
            )
        }
    }

    /**
     * Processes the block and updates the indexer state.
     *
     * @param block The block to process.
     */
    protected open suspend fun processAndUpdateState(block: Block) {
        logProcessingBlock()
        process(buildIndexingResult(block))
        updateBlockState(block)
    }

    /**
     * Processes the block with pre-computed inspection results and updates the indexer state.
     *
     * @param block The block to process.
     * @param inspectionResults Pre-computed inspection results from pipelined fetch.
     */
    protected open suspend fun processAndUpdateStateWithResults(
        block: Block,
        inspectionResults: List<org.vechain.indexer.thor.model.InspectionResult>
    ) {
        logProcessingBlock()
        process(buildIndexingResultWithCallResults(block, inspectionResults))
        updateBlockState(block)
    }

    /**
     * Updates the block state after successful processing.
     *
     * @param block The processed block.
     */
    protected open fun updateBlockState(block: Block) {
        currentBlockNumber = block.number + 1
        previousBlock = BlockIdentifier(number = block.number, id = block.id)
        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
    }

    protected fun updateSyncStatus(block: Block) {
        // if the timestamp of the block is within 15 seconds of the current time, we are fully
        // synced
        val blockTime = LocalDateTime.ofEpochSecond(block.timestamp, 0, ZoneOffset.UTC)
        val now = LocalDateTime.now(ZoneOffset.UTC)
        status =
            if (Duration.between(blockTime, now).toSeconds() < 15) {
                Status.FULLY_SYNCED
            } else {
                Status.SYNCING
            }
    }

    override fun getLastSyncedBlock(): BlockIdentifier? = processor.getLastSyncedBlock()

    override fun rollback(blockNumber: Long) = processor.rollback(blockNumber)

    override suspend fun process(entry: IndexingResult) = processor.process(entry)

    /**
     * Rolls the indexer back to a target block so it lines up with the rest of its dependency
     * component. Only ever moves backwards: an indexer with no persisted state already sits at
     * [startBlock] and cannot have advanced past a sibling that did real work, so a target below
     * the current cursor is treated as a misconfiguration.
     *
     * Mirrors [handleReorg]'s state reset but takes the target explicitly instead of inferring it
     * from a detected reorg. The next [processBlock] will pick up at [currentBlockNumber] with
     * [previousBlock] re-seeded from persistence so reorg detection still works on the next block.
     *
     * Processors typically retain only a shallow rollback window (the reorg depth, not the full
     * chain). For deep alignment — e.g. when a new dependant joins a component that's already
     * synced to head — `rollback(target)` may be a no-op or only partial. This method verifies the
     * rollback actually took effect and refuses to advance with an inconsistent cursor; the
     * operator must drop the indexer's persisted state and restart.
     */
    internal fun alignToBlock(target: Long) {
        if (target == currentBlockNumber) return
        require(target < currentBlockNumber) {
            "alignToBlock can only move backwards (current=$currentBlockNumber, target=$target)"
        }
        rollback(target)
        val lastSynced = syncCursorToPersistedState()
        check(lastSynced == null || lastSynced.number < target) {
            "Indexer '$name' could not be rolled back to block $target — persisted state is " +
                "still at block ${lastSynced!!.number}. The processor's rollback retention is " +
                "likely insufficient for this depth of realignment. Drop this indexer's " +
                "persisted state and restart to proceed."
        }
    }

    /**
     * Re-seats the in-memory cursor (`currentBlockNumber`, `previousBlock`) from persisted state
     * after a rollback, and transitions out of `FULLY_SYNCED` since the tip is now further away.
     * Shared by [handleReorg], [alignToBlock], and [recoverFromStuckBlock]; the only thing each
     * caller varies is the rollback depth and any post-rollback validation it needs.
     *
     * Returns the persisted last-synced block (or `null` if none) so callers that need it for an
     * additional check don't have to query again.
     */
    private fun syncCursorToPersistedState(): BlockIdentifier? {
        val lastSynced = getLastSyncedBlock()
        if (lastSynced != null) {
            currentBlockNumber = lastSynced.number + 1
            previousBlock = lastSynced
        } else {
            currentBlockNumber = startBlock
            previousBlock = null
        }
        if (status == Status.FULLY_SYNCED) {
            status = Status.SYNCING
        }
        return lastSynced
    }

    /**
     * Recovers from a block that the runner has given up retrying. Clears the failing block's
     * partial persisted state (`rollback(currentBlockNumber)` deletes records `>= currentBlock` and
     * parks the checkpoint at `currentBlock - 1`) and re-seats the in-memory cursor. The runner
     * throws [org.vechain.indexer.exception.StuckBlockException] afterwards; the outer `run()` loop
     * catches it and restarts the indexers from this clean state.
     */
    internal fun recoverFromStuckBlock() {
        rollback(currentBlockNumber)
        syncCursorToPersistedState()
    }

    private fun logProcessingBlock() {
        if (shouldLogDebug()) {
            logger.debug(buildLogMessage())
        } else if (shouldLogInfo()) {
            logger.info(buildLogMessage())
        }
    }

    /**
     * Determines whether debug logging should be enabled.
     *
     * @return true if debug logging is enabled, false otherwise.
     */
    protected open fun shouldLogDebug(): Boolean = logger.isDebugEnabled

    /**
     * Determines whether info logging should be enabled.
     *
     * Always logs when [Status.FULLY_SYNCED]; otherwise throttles to at most one info log per
     * [syncLoggerInterval] seconds. The throttle timestamp is updated as a side effect when this
     * method returns true on the throttled path.
     *
     * @return true if info logging should occur, false otherwise.
     */
    protected open fun shouldLogInfo(): Boolean {
        if (status == Status.FULLY_SYNCED) return true
        val now = LocalDateTime.now(ZoneOffset.UTC)
        if (Duration.between(lastInfoLogTime, now).toSeconds() >= syncLoggerInterval) {
            lastInfoLogTime = now
            return true
        }
        return false
    }

    /**
     * Builds the log message for block processing.
     *
     * @return The formatted log message.
     */
    protected open fun buildLogMessage(): String {
        return "Processing %4d Blocks @ %,11d".format(1, currentBlockNumber)
    }

    internal fun checkForReorg(block: Block) {
        if (shouldCheckForReorg() && isReorgDetected(block)) {
            handleReorg(block)
        }
    }

    /**
     * Determines whether a reorg check should be performed.
     *
     * @return true if reorg checking should occur, false otherwise.
     */
    protected open fun shouldCheckForReorg(): Boolean {
        return currentBlockNumber > startBlock && previousBlock != null
    }

    /**
     * Detects if a chain reorganization has occurred.
     *
     * @param block The current block to check.
     * @return true if a reorg is detected, false otherwise.
     */
    protected open fun isReorgDetected(block: Block): Boolean {
        return previousBlock?.id?.let { it != block.parentID } == true
    }

    /**
     * Handles a detected chain reorganization.
     *
     * Rolls back persisted state and resets in-memory `currentBlockNumber` / `previousBlock` to
     * track the new persisted cursor. Without this reset the runner would retry processing the
     * reorg-detected block against a stale `previousBlock`, re-trigger the reorg check, deepen the
     * rollback by one more block, and loop without making progress.
     *
     * @param block The block where the reorg was detected.
     * @throws ReorgException always, after logging and rolling back.
     */
    protected open fun handleReorg(block: Block) {
        val message = buildReorgMessage(block)
        logger.error(message)
        rollback(currentBlockNumber - 1)
        syncCursorToPersistedState()
        throw ReorgException(message)
    }

    /**
     * Builds the reorg error message.
     *
     * @param block The block where the reorg was detected.
     * @return The formatted reorg message.
     */
    protected open fun buildReorgMessage(block: Block): String {
        return "REORG @ Block $currentBlockNumber " +
            "previousBlock=(id=${previousBlock?.id ?: "null"} number=${previousBlock?.number ?: "null"}) " +
            "block=(parentID=${block.parentID} blockNumber=${block.number} id=${block.id})"
    }

    override fun shutDown() {
        setStatus(Status.SHUT_DOWN)
        logger.info("Indexer Shut down")
    }
}
