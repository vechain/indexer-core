package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.GenericEventProcessor
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.enums.LogType
import org.vechain.indexer.thor.model.*
import org.vechain.indexer.utils.matchesEventCriteria
import org.vechain.indexer.utils.matchesTransferCriteria

const val BLOCK_BATCH_SIZE = 100L //  Block batch size
const val LOG_FETCH_LIMIT = 1000L //  Limits logs per API call (pagination)

/**
 * **LogsIndexer**
 *
 * Handles event logs and VET transfer logs from the VeChain Thor blockchain. Supports configurable
 * filtering and processing of both events and transfer logs.
 *
 * This indexer iterates through blockchain transactions, extracts logs based on criteria, and
 * processes them accordingly.
 *
 * @param thorClient The Thor blockchain client instance.
 * @param startBlock The starting block number for indexing.
 * @param syncLoggerInterval Frequency of log sync status updates.
 * @param logsType The types of logs to index (EVENT and/or TRANSFER).
 * @param blockBatchSize Number of blocks fetched per batch.
 * @param logFetchLimit Maximum number of logs fetched per API call.
 * @param eventCriteriaSet Filtering criteria for event logs.
 * @param transferCriteriaSet Filtering criteria for transfer logs.
 * @param abiManager ABI manager for decoding contract events.
 * @param businessEventManager Business event manager for processing business logic.
 */
abstract class LogsIndexer(
    override val thorClient: ThorClient,
    startBlock: Long = 0L,
    private val syncLoggerInterval: Long = 1_000L,
    private val logsType: Set<LogType> = setOf(LogType.EVENT), // Default to EVENT
    private val blockBatchSize: Long = BLOCK_BATCH_SIZE,
    private val logFetchLimit: Long = LOG_FETCH_LIMIT,
    private var eventCriteriaSet: List<EventCriteria>? = null,
    private var transferCriteriaSet: List<TransferCriteria>? = null,
    final override val abiManager: AbiManager? = null,
    override val businessEventManager: BusinessEventManager? = null,
) : BlockIndexer(thorClient, startBlock, syncLoggerInterval, abiManager, businessEventManager) {
    /**
     * Processes the retrieved logs from blocks.
     *
     * This function is **abstract** and must be implemented by subclasses of `LogsIndexer`. It is
     * responsible for handling both **event logs** and **transfer logs** based on the indexer's
     * configuration.
     *
     * @param events A list of `EventLog` instances representing on-chain event logs.
     * @param transfers A list of `TransferLog` instances representing VET transfer logs.
     *
     * **Implementation Note: **
     * - Subclasses **must** override this function.
     * - Depending on the `logsType` configuration, either **or both** lists may be empty.
     * - If only `LogType.EVENT` is enabled, `transfers` will be an empty list.
     * - If only `LogType.TRANSFER` is enabled, `events` will be an empty list.
     * - Implementers should handle each type accordingly.
     */
    abstract fun processLogs(
        events: List<EventLog>,
        transfers: List<TransferLog>,
    )

    private var cachedConfiguredEvents: List<AbiElement>? = null

    /**
     * Initializes the indexer by setting the last synced block and updating status.
     *
     * @param blockNumber The block number to start from (defaults to last synced block).
     */
    private fun initialise(blockNumber: Long? = null) {
        val lastSyncedBlockNumber = blockNumber ?: getLastSyncedBlock()?.number ?: startBlock

        // To ensure data integrity roll back changes made in the last block
        rollback(lastSyncedBlockNumber)

        currentBlockNumber = lastSyncedBlockNumber
        status = Status.SYNCING
        logger.info("Initialized LogsIndexer at block: $lastSyncedBlockNumber")
    }

    /**
     * Starts the indexer and processes logs up to the latest finalized block.
     *
     * @param iterations The number of iterations to run (null for infinite).
     */
    override suspend fun start(iterations: Long?) {
        initialise()
        val finalizedBlock = thorClient.getFinalizedBlock().number

        remainingIterations = iterations

        if (currentBlockNumber < finalizedBlock) {
            syncLogs(finalizedBlock)
        }

        logger.info("Fast sync complete, switching to block indexer")
        super.start(iterations) // Switch to normal block indexing
    }

    /**
     * Synchronizes logs from the current block to the target block.
     *
     * @param toBlock The block number to sync up to.
     */
    private suspend fun syncLogs(toBlock: Long) {
        while (currentBlockNumber < toBlock) {
            try {
                if (hasNoRemainingIterations()) return
                backoffDelay()

                val batchEndBlock = minOf(currentBlockNumber + blockBatchSize, toBlock)

                // Fetch both event logs and VET transfers
                val logsBatch = fetchLogs(currentBlockNumber, batchEndBlock)

                // Log sync status
                if (
                    logger.isTraceEnabled ||
                        hasMultipleInRange(currentBlockNumber, batchEndBlock, syncLoggerInterval)
                ) {
                    logger.info("Fast Syncing @ Block Range $currentBlockNumber - $batchEndBlock")
                }

                if (logsBatch.eventLogs.isEmpty() && logsBatch.transferLogs.isEmpty()) {
                    currentBlockNumber = batchEndBlock + 1
                    timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
                    continue
                }

                // Process events and transfers
                processLogs(logsBatch.eventLogs, logsBatch.transferLogs)

                // Update last processed block
                currentBlockNumber = batchEndBlock + 1
                timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
            } catch (e: Exception) {
                logger.error("Error fetching logs at block $currentBlockNumber: ${e.message}")
                handleError()
            }
        }
    }

    /** Fetches both event logs and VET transfer logs based on `logsType`. */
    private suspend fun fetchLogs(
        fromBlock: Long,
        toBlock: Long,
    ): LogFetchResult {
        val eventLogs = mutableListOf<EventLog>()
        val transferLogs = mutableListOf<TransferLog>()

        if (logsType.contains(LogType.EVENT)) {
            eventLogs.addAll(fetchEventLogs(fromBlock, toBlock))
        }

        if (logsType.contains(LogType.TRANSFER)) {
            transferLogs.addAll(fetchTransfers(fromBlock, toBlock))
        }

        return LogFetchResult(eventLogs, transferLogs)
    }

    /** Fetches event logs from the Thor client. */
    private suspend fun fetchEventLogs(
        fromBlock: Long,
        toBlock: Long,
    ): List<EventLog> {
        val logs = mutableListOf<EventLog>()
        var offset = 0L
        while (true) {
            val response =
                thorClient.getEventLogs(
                    EventLogsRequest(
                        range = LogsRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = LogsOptions(offset = offset, limit = logFetchLimit),
                        criteriaSet = eventCriteriaSet,
                        order = "asc",
                    ),
                )

            if (response.isEmpty()) break
            logs.addAll(response)
            if (response.size < logFetchLimit) break
            offset += logFetchLimit
        }
        return logs
    }

    /** Fetches VET transfer logs from the Thor client. */
    private suspend fun fetchTransfers(
        fromBlock: Long,
        toBlock: Long,
    ): List<TransferLog> {
        val transfers = mutableListOf<TransferLog>()
        var offset = 0L

        while (true) {
            val response =
                thorClient.getVetTransfers(
                    TransferLogsRequest(
                        range = LogsRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = LogsOptions(offset = offset, limit = logFetchLimit),
                        order = "asc",
                        criteriaSet = transferCriteriaSet,
                    ),
                )

            if (response.isEmpty()) break
            transfers.addAll(response)
            if (response.size < logFetchLimit) break
            offset += logFetchLimit
        }

        return transfers
    }

    /**
     * Processes a given block by extracting and categorizing logs.
     *
     * This function iterates through all transactions in the block, analyzing both event logs and
     * transfer logs based on the specified `logsType` (EVENT and/or TRANSFER).
     * - If EVENT logging is enabled, it extracts event logs using `processEventLogs`.
     * - If TRANSFER logging is enabled, it extracts transfer logs using `processTransferLogs`.
     *
     * @param block The blockchain block containing transactions to be processed.
     */
    override fun processBlock(block: Block) {
        val eventLogs = mutableListOf<EventLog>()
        val transferLogs = mutableListOf<TransferLog>()

        for (tx in block.transactions) {
            for ((clauseIndex, output) in tx.outputs.withIndex()) {
                if (logsType.contains(LogType.EVENT)) {
                    eventLogs.addAll(processBlockEvents(output, block, tx, clauseIndex))
                }
                if (logsType.contains(LogType.TRANSFER)) {
                    transferLogs.addAll(processBlockTransfers(output, block, tx, clauseIndex))
                }
            }
        }

        if (eventLogs.isNotEmpty() || transferLogs.isNotEmpty()) {
            processLogs(eventLogs, transferLogs)
        }
    }

    /**
     * Extracts and filters event logs from a transaction output.
     *
     * This function processes all event logs found in the given transaction output. If event
     * filtering criteria (`eventCriteriaSet`) are provided, only logs matching the criteria are
     * included.
     *
     * @param output The transaction output containing event logs.
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the output.
     * @return A list of event logs that match the defined criteria (or all logs if no criteria
     *   exist).
     */
    private fun processBlockEvents(
        output: TxOutputs,
        block: Block,
        tx: Transaction,
        clauseIndex: Int,
    ): List<EventLog> {
        val eventLogs = mutableListOf<EventLog>()

        output.events.forEach { event ->
            if (eventCriteriaSet.isNullOrEmpty()) {
                eventLogs.add(event.toEventLog(block, tx, clauseIndex))
            } else {
                for (criteria in eventCriteriaSet!!) {
                    if (matchesEventCriteria(event, criteria)) {
                        eventLogs.add(event.toEventLog(block, tx, clauseIndex))
                    }
                }
            }
        }
        return eventLogs
    }

    /**
     * Extracts and filters transfer logs from a transaction output.
     *
     * This function processes all token/VET transfers found in the transaction output. If filtering
     * criteria (`transferCriteriaSet`) are provided, only transfers matching the criteria are
     * included.
     *
     * @param output The transaction output containing transfer logs.
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the output.
     * @return A list of transfer logs that match the defined criteria (or all logs if no criteria
     *   exist).
     */
    private fun processBlockTransfers(
        output: TxOutputs,
        block: Block,
        tx: Transaction,
        clauseIndex: Int,
    ): List<TransferLog> {
        val transferLogs = mutableListOf<TransferLog>()

        output.transfers.forEach { transfer ->
            if (transferCriteriaSet.isNullOrEmpty()) {
                transferLogs.add(transfer.toTransferLog(block, tx, clauseIndex))
            } else {
                for (criteria in transferCriteriaSet!!) {
                    if (matchesTransferCriteria(transfer, criteria, tx)) {
                        transferLogs.add(transfer.toTransferLog(block, tx, clauseIndex))
                    }
                }
            }
        }
        return transferLogs
    }

    /**
     * Converts a transaction event into an EventLog.
     *
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the event.
     * @return The formatted EventLog.
     */
    private fun TxEvent.toEventLog(
        block: Block,
        tx: Transaction,
        clauseIndex: Int = 0,
    ): EventLog =
        EventLog(
            address = this.address,
            topics = this.topics,
            data = this.data,
            meta =
                EventMeta(
                    blockID = block.id,
                    blockNumber = block.number,
                    blockTimestamp = block.timestamp,
                    txID = tx.id,
                    txOrigin = tx.origin,
                    clauseIndex = clauseIndex,
                ),
        )

    /**
     * Converts a transaction transfer into a TransferLog.
     *
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the transfer.
     * @return The formatted TransferLog.
     */
    private fun TxTransfer.toTransferLog(
        block: Block,
        tx: Transaction,
        clauseIndex: Int = 0,
    ): TransferLog =
        TransferLog(
            sender = this.sender,
            recipient = this.recipient,
            amount = this.amount,
            meta =
                EventMeta(
                    blockID = block.id,
                    blockNumber = block.number,
                    blockTimestamp = block.timestamp,
                    txID = tx.id,
                    txOrigin = tx.origin,
                    clauseIndex = clauseIndex,
                ),
        )

    /**
     * @param eventLogs The Thor logs to process.
     * @param criteria Filtering criteria to determine which events to process.
     * @return A list of decoded events and their associated parameters.
     * @notice Processes all events (generic and business) in a group of log events based on the
     *   provided criteria.
     * @dev Updates the filter criteria with business event names if applicable. Decodes and
     *   processes both generic and business events.
     */
    protected open fun processAllEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog> = emptyList(),
        criteria: FilterCriteria = FilterCriteria(),
    ): List<IndexedEvent> {
        val decodedEvents = processGenericEvents(eventLogs, transferLogs, criteria)
        return processBusinessEvents(
            decodedEvents,
            criteria.businessEventNames,
            criteria.removeDuplicates,
        )
    }

    /**
     * @param eventLogs The Thor event logs to process.
     * @param transferLogs The Thor transfer logs to process.
     * @param criteria Filtering criteria to determine which generic events to process.
     * @return A list of decoded generic events and their associated parameters.
     * @notice Processes generic events in a of log events based on the provided criteria.
     * @dev Requires the `AbiManager` to decode events. If not configured, skips processing and
     *   returns an empty list.
     */
    protected open fun processGenericEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog> = emptyList(),
        criteria: FilterCriteria = FilterCriteria(),
    ): List<IndexedEvent> {
        // TODO: Why do we need a pair here when the params are contained in the indexed event?

        if (abiManager == null) {
            logger.warn("ABI Manager is not configured. Skipping generic event processing.")
            return emptyList()
        }

        val eventProcessor = GenericEventProcessor(abiManager)

        if (cachedConfiguredEvents == null) {
            val updatedCriteria =
                businessEventManager?.updateCriteriaWithBusinessEvents(criteria) ?: criteria
            cachedConfiguredEvents =
                eventProcessor.getConfiguredEvents(
                    updatedCriteria.abiNames,
                    updatedCriteria.eventNames,
                )
        }

        val contractEvents =
            eventProcessor.decodeLogEvents(eventLogs, cachedConfiguredEvents!!, criteria)
        val vetTransfers = eventProcessor.decodeLogTransfers(transferLogs)

        return contractEvents + vetTransfers
    }

    /**
     * @param startBlock The start of the block range.
     * @param endBlock The end of the block range.
     * @param x The number to check for multiples.
     * @notice Determines if any multiples of `x` exist in the range `[startBlock, endBlock]`.
     */
    private fun hasMultipleInRange(
        startBlock: Long,
        endBlock: Long,
        x: Long,
    ): Boolean {
        if (x == 0L) return false // Prevent division by zero

        val firstMultiple = if (startBlock % x == 0L) startBlock else (startBlock / x + 1) * x
        return firstMultiple in startBlock..endBlock
    }
}
