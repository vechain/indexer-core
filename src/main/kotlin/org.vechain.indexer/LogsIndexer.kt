package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.time.TimeMark
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.thor.client.LogClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*

/**
 * **LogsIndexer**
 *
 * Handles event logs and VET transfer logs from the VeChain Thor blockchain. Supports configurable
 * filtering and processing of both events and transfer logs.
 *
 * This indexer iterates through blockchain transactions, extracts logs based on criteria, and
 * processes them accordingly.
 */
open class LogsIndexer(
    name: String,
    override val thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    syncLoggerInterval: Long,
    private val excludeVetTransfers: Boolean,
    private val blockBatchSize: Long,
    private val logFetchLimit: Long,
    private var eventCriteriaSet: List<EventCriteria>?,
    private var transferCriteriaSet: List<TransferCriteria>?,
    eventProcessor: CombinedEventProcessor?,
) :
    BlockIndexer(
        name = name,
        thorClient = thorClient,
        processor = processor,
        startBlock = startBlock,
        syncLoggerInterval = syncLoggerInterval,
        eventProcessor = eventProcessor,
        inspectionClauses = null,
        dependsOn = null,
    ),
    FastSyncableIndexer {
    private var currentBlockBatchSize: Long = blockBatchSize

    init {
        require(blockBatchSize in MIN_BLOCK_BATCH_SIZE..MAX_BLOCK_BATCH_SIZE) {
            "blockBatchSize must be between $MIN_BLOCK_BATCH_SIZE and $MAX_BLOCK_BATCH_SIZE"
        }
        require(logFetchLimit >= 1) { "logFetchLimit must be >= 1" }
    }

    protected open val logClient = LogClient(thorClient)

    override fun initializeState(blockNumber: Long) {
        // RTFS is reachable only from NOT_INITIALISED. Subsequent initialisations (e.g., after a
        // reorg restart) move the indexer directly to READY_TO_SYNC — fast sync is one-shot per
        // process lifetime.
        val firstInit = getStatus() == Status.NOT_INITIALISED
        super.initializeState(blockNumber)
        if (firstInit) {
            setStatus(Status.READY_TO_FAST_SYNC)
        }
    }

    override suspend fun fastSync() {
        runFastSync(deadlineMark = null)
    }

    internal suspend fun fastSyncUntil(deadlineMark: TimeMark) {
        runFastSync(deadlineMark)
    }

    private suspend fun runFastSync(deadlineMark: TimeMark?) {
        setStatus(Status.FAST_SYNCING)
        logger.info("Starting fast sync from block ${getCurrentBlockNumber()}")

        if (deadlineMark?.hasNotPassedNow() == false) {
            logger.info("Fast sync paused at block ${getCurrentBlockNumber()}")
            return
        }

        val finalizedBlock = thorClient.getBlock(BlockRevision.Keyword.FINALIZED)
        var completed = true

        if (getCurrentBlockNumber() < finalizedBlock.number) {
            completed =
                sync(BlockIdentifier(finalizedBlock.number, finalizedBlock.id), deadlineMark)
            // sync() processes blocks up to (finalizedBlock.number - 1); the next block to be
            // processed by the live BlockIndexer flow is finalizedBlock itself. Seed previousBlock
            // with finalizedBlock's parent so checkForReorg compares against the correct
            // predecessor instead of misidentifying the upcoming finalizedBlock as a reorg.
            if (completed) {
                setPreviousBlock(
                    BlockIdentifier(
                        number = finalizedBlock.number - 1,
                        id = finalizedBlock.parentID
                    )
                )
            }
        }

        if (completed) {
            logger.info("Fast sync complete")
            setStatus(Status.READY_TO_SYNC)
        } else {
            logger.info("Fast sync paused at block ${getCurrentBlockNumber()}")
        }
    }

    /**
     * Synchronizes logs from the current block to the target block.
     *
     * This method processes blocks in batches determined by the current adaptive block range. For
     * each batch:
     * 1. Fetches event logs (if ABIs are configured)
     * 2. Fetches transfer logs (if not excluded)
     * 3. Processes and indexes the logs
     * 4. Adjusts the next block range based on raw log count
     * 5. Updates the current block number
     *
     * The sync continues until [getCurrentBlockNumber] reaches [toBlock].number.
     *
     * @param toBlock The block cursor to sync up to.
     *
     * Note: This method is internal to allow for testing via TestableLogsIndexer.
     */
    internal suspend fun sync(
        toBlock: BlockIdentifier,
        deadlineMark: TimeMark? = null,
    ): Boolean {
        while (getCurrentBlockNumber() < toBlock.number) {
            if (deadlineMark?.hasNotPassedNow() == false) return false
            checkIfShuttingDown()
            processBatch(toBlock.number)
        }
        return true
    }

    /**
     * Processes a single batch of blocks, fetching and indexing logs.
     *
     * @param toBlockNumber The target block number for the overall sync operation.
     */
    protected open suspend fun processBatch(toBlockNumber: Long) {
        val batchEndBlock = calculateBatchEndBlock(toBlockNumber)
        logSyncStatus(getCurrentBlockNumber(), batchEndBlock, getStatus())

        val eventLogs = fetchEventLogsIfNeeded(batchEndBlock)
        val transferLogs = fetchTransferLogsIfNeeded(batchEndBlock)
        val totalFetchedLogs = eventLogs.size + transferLogs.size

        if (hasNoLogs(eventLogs, transferLogs)) {
            adjustBlockBatchSize(totalFetchedLogs)
            updateBlockNumberAndTime(batchEndBlock)
            return
        }

        processAndIndexEvents(eventLogs, transferLogs, batchEndBlock)
        adjustBlockBatchSize(totalFetchedLogs)
        updateBlockNumberAndTime(batchEndBlock)
    }

    /**
     * Calculates the end block number for the current batch.
     *
     * @param toBlockNumber The target block number for the overall sync operation.
     * @return The batch end block number (will not cross toBlockNumber).
     */
    protected open fun calculateBatchEndBlock(toBlockNumber: Long): Long {
        return minOf(getCurrentBlockNumber() + currentBlockBatchSize - 1, toBlockNumber - 1)
    }

    /**
     * Adjusts the next block range using raw log volume as backpressure.
     *
     * Raw logs are used instead of processed events so the range reacts to Thor query volume before
     * ABI filtering, business event grouping, or deduplication.
     */
    protected open fun adjustBlockBatchSize(totalFetchedLogs: Int) {
        val nextBlockBatchSize =
            when {
                totalFetchedLogs == 0 -> currentBlockBatchSize * 2
                totalFetchedLogs <= TARGET_LOGS_PER_BATCH / 2 -> increaseBlockBatchSize()
                totalFetchedLogs <= TARGET_LOGS_PER_BATCH -> currentBlockBatchSize
                else -> shrinkBlockBatchSize(totalFetchedLogs)
            }

        currentBlockBatchSize =
            nextBlockBatchSize.coerceIn(MIN_BLOCK_BATCH_SIZE, MAX_BLOCK_BATCH_SIZE)
    }

    private fun increaseBlockBatchSize(): Long {
        return currentBlockBatchSize + ((currentBlockBatchSize + 1) / 2)
    }

    private fun shrinkBlockBatchSize(totalFetchedLogs: Int): Long {
        return (currentBlockBatchSize * TARGET_LOGS_PER_BATCH / totalFetchedLogs).coerceAtLeast(
            MIN_BLOCK_BATCH_SIZE
        )
    }

    /**
     * Checks if both event logs and transfer logs are empty.
     *
     * @return true if no logs were fetched, false otherwise.
     */
    protected open fun hasNoLogs(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>
    ): Boolean {
        return eventLogs.isEmpty() && transferLogs.isEmpty()
    }

    /**
     * Fetches event logs if the event processor has ABI definitions configured.
     *
     * @param batchEndBlock The end block number for this batch.
     * @return List of event logs, or empty list if no ABIs are configured.
     */
    protected open suspend fun fetchEventLogsIfNeeded(batchEndBlock: Long): List<EventLog> {
        if (!shouldFetchEventLogs()) return emptyList()

        return logClient.fetchEventLogs(
            getCurrentBlockNumber(),
            batchEndBlock,
            logFetchLimit,
            eventCriteriaSet
        )
    }

    /**
     * Determines whether event logs should be fetched based on ABI configuration.
     *
     * @return true if event logs should be fetched, false otherwise.
     */
    protected open fun shouldFetchEventLogs(): Boolean = eventProcessor?.hasAbis() == true

    /**
     * Fetches transfer logs if VET transfers are not excluded.
     *
     * @param batchEndBlock The end block number for this batch.
     * @return List of transfer logs, or empty list if transfers are excluded.
     */
    protected open suspend fun fetchTransferLogsIfNeeded(batchEndBlock: Long): List<TransferLog> {
        if (!shouldFetchTransferLogs()) return emptyList()

        return logClient.fetchTransfers(
            getCurrentBlockNumber(),
            batchEndBlock,
            logFetchLimit,
            transferCriteriaSet
        )
    }

    /**
     * Determines whether transfer logs should be fetched.
     *
     * @return true if transfer logs should be fetched, false otherwise.
     */
    protected open fun shouldFetchTransferLogs(): Boolean = !excludeVetTransfers

    /**
     * Processes the fetched logs and creates indexed events.
     *
     * @param eventLogs The event logs to process.
     * @param transferLogs The transfer logs to process.
     * @param batchEndBlock The end block number for this batch.
     */
    protected open suspend fun processAndIndexEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>,
        batchEndBlock: Long
    ) {
        val indexedEvents = eventProcessor?.processEvents(eventLogs, transferLogs) ?: emptyList()
        process(IndexingResult.LogResult(batchEndBlock, indexedEvents, getStatus()))
    }

    /**
     * Updates the current block number and last processed time.
     *
     * @param batchEndBlock The end block number for this batch.
     */
    protected open fun updateBlockNumberAndTime(batchEndBlock: Long) {
        setCurrentBlockNumber(batchEndBlock + 1)
        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
    }

    private fun logSyncStatus(currentBlockNumber: Long, batchEndBlock: Long, status: Status) {
        val message =
            "($status) Processing ${batchEndBlock - currentBlockNumber + 1} Blocks @ $currentBlockNumber"
        if (shouldLogDebug()) {
            logger.debug(message)
        } else if (shouldLogInfo()) {
            logger.info(message)
        }
    }

    companion object {
        private const val MIN_BLOCK_BATCH_SIZE = 1L
        private const val MAX_BLOCK_BATCH_SIZE = 1_000L
        private const val TARGET_LOGS_PER_BATCH = 1_000
    }
}
