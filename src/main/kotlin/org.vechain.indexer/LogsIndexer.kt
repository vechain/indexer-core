package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
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
    pruner: Pruner?,
    prunerInterval: Long
) :
    BlockIndexer(
        name = name,
        thorClient = thorClient,
        processor = processor,
        startBlock = startBlock,
        syncLoggerInterval = syncLoggerInterval,
        eventProcessor = eventProcessor,
        inspectionClauses = null,
        pruner = pruner,
        prunerInterval = prunerInterval,
        dependsOn = null,
    ) {
    init {
        require(blockBatchSize >= 1) { "blockBatchSize must be >= 1" }
        require(logFetchLimit >= 1) { "logFetchLimit must be >= 1" }
    }

    protected open val logClient = LogClient(thorClient)

    override suspend fun fastSync() {

        setStatus(Status.FAST_SYNCING)
        logger.info("Starting fast sync from block ${getCurrentBlockNumber()}")

        val finalizedBlock = thorClient.getFinalizedBlock()

        if (getCurrentBlockNumber() < finalizedBlock.number) {
            sync(BlockIdentifier(finalizedBlock.number, finalizedBlock.id))
        }

        logger.info("Fast sync complete")

        setStatus(Status.INITIALISED)
    }

    /**
     * Synchronizes logs from the current block to the target block.
     *
     * This method processes blocks in batches determined by [blockBatchSize]. For each batch:
     * 1. Fetches event logs (if ABIs are configured)
     * 2. Fetches transfer logs (if not excluded)
     * 3. Processes and indexes the logs
     * 4. Updates the current block number
     *
     * The sync continues until [getCurrentBlockNumber] reaches [toBlock].number.
     *
     * @param toBlock The block identifier to sync up to (inclusive).
     *
     * Note: This method is internal to allow for testing via TestableLogsIndexer.
     */
    internal suspend fun sync(toBlock: BlockIdentifier) {
        while (getCurrentBlockNumber() < toBlock.number) {
            checkIfShuttingDown()
            processBatch(toBlock.number)
        }
        setPreviousBlock(toBlock)
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

        if (hasNoLogs(eventLogs, transferLogs)) {
            updateBlockNumberAndTime(batchEndBlock)
            return
        }

        processAndIndexEvents(eventLogs, transferLogs, batchEndBlock)
        updateBlockNumberAndTime(batchEndBlock)
    }

    /**
     * Calculates the end block number for the current batch.
     *
     * @param toBlockNumber The target block number for the overall sync operation.
     * @return The batch end block number (will not exceed toBlockNumber).
     */
    protected open fun calculateBatchEndBlock(toBlockNumber: Long): Long {
        return minOf(getCurrentBlockNumber() + blockBatchSize - 1, toBlockNumber)
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
        if (indexedEvents.isNotEmpty()) {
            process(IndexingResult.EventsOnly(batchEndBlock, indexedEvents))
        }
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
        logger.info("($status) Processing Blocks $currentBlockNumber - $batchEndBlock")
    }
}
