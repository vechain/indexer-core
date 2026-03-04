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
    protected val startBlock: Long,
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

    /** Initialises the indexer processing */
    override fun initialise() {
        val lastSyncedBlockNumber = determineStartingBlock()
        rollbackToSafeState(lastSyncedBlockNumber)
        initializeState(lastSyncedBlockNumber)
        logInitialization()
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
        status = Status.INITIALISED
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
        ensureStatus(status, setOf(Status.INITIALISED, Status.SYNCING, Status.FULLY_SYNCED))
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
     * @return true if info logging should occur, false otherwise.
     */
    protected open fun shouldLogInfo(): Boolean {
        return status == Status.FULLY_SYNCED || currentBlockNumber % syncLoggerInterval == 0L
    }

    /**
     * Builds the log message for block processing.
     *
     * @return The formatted log message.
     */
    protected open fun buildLogMessage(): String {
        return "($status) Processing Block  $currentBlockNumber"
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
     * @param block The block where the reorg was detected.
     * @throws ReorgException always, after logging and rolling back.
     */
    protected open fun handleReorg(block: Block) {
        val message = buildReorgMessage(block)
        logger.error(message)
        rollback(currentBlockNumber - 1)
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
