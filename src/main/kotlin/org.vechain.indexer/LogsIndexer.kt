package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import org.vechain.indexer.event.AbiEventProcessor
import org.vechain.indexer.event.BusinessEventProcessor
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.client.LogClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*
import org.vechain.indexer.utils.LogsUtils

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
 * @param excludeLogEvents If true, excludes event logs from processing.
 * @param excludeVetTransfers If true, excludes VET transfer logs from processing.
 * @param blockBatchSize Number of blocks fetched per batch.
 * @param logFetchLimit Maximum number of logs fetched per API call.
 * @param eventCriteriaSet Filtering criteria for event logs.
 * @param transferCriteriaSet Filtering criteria for transfer logs.
 * @param abiEventProcessor Optional ABI event processor for decoding events.
 * @param businessEventProcessor Optional business event processor for handling business-specific
 *   events.
 */
open class LogsIndexer(
    override val thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long = 0L,
    private val syncLoggerInterval: Long = 1_000L,
    private val excludeLogEvents: Boolean = false,
    private val excludeVetTransfers: Boolean = false,
    private val blockBatchSize: Long,
    private val logFetchLimit: Long,
    private var eventCriteriaSet: List<EventCriteria>? = null,
    private var transferCriteriaSet: List<TransferCriteria>? = null,
    override val abiEventProcessor: AbiEventProcessor? = null,
    override val businessEventProcessor: BusinessEventProcessor? = null,
) :
    BlockIndexer(
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        abiEventProcessor,
        businessEventProcessor
    ) {

    private val logClient = LogClient(thorClient)

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

                // Log sync status
                if (
                    logger.isTraceEnabled ||
                        hasMultipleInRange(currentBlockNumber, batchEndBlock, syncLoggerInterval)
                ) {
                    logger.info("Fast Syncing @ Block Range $currentBlockNumber - $batchEndBlock")
                }

                // Fetch both event logs and VET transfers
                val eventLogs =
                    if (!excludeLogEvents)
                        logClient.fetchEventLogs(
                            currentBlockNumber,
                            batchEndBlock,
                            logFetchLimit,
                            eventCriteriaSet
                        )
                    else emptyList()

                val transferLogs =
                    if (!excludeVetTransfers)
                        logClient.fetchTransfers(
                            currentBlockNumber,
                            batchEndBlock,
                            logFetchLimit,
                            transferCriteriaSet
                        )
                    else emptyList()

                if (eventLogs.isEmpty() && transferLogs.isEmpty()) {
                    currentBlockNumber = batchEndBlock + 1
                    timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
                    continue
                }

                // Process events and transfers
                val indexedEvents = processEvents(eventLogs, transferLogs)
                process(indexedEvents)

                // Update last processed block
                currentBlockNumber = batchEndBlock + 1
                timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
            } catch (e: Exception) {
                logger.error("Error fetching logs at block $currentBlockNumber: ${e.message}")
                handleError()
            }
        }
    }

    /** Processes events from a block. */
    override fun processEvents(block: Block): List<IndexedEvent> {
        val eventLogs = mutableListOf<EventLog>()
        val transferLogs = mutableListOf<TransferLog>()

        for (tx in block.transactions) {
            for ((clauseIndex, output) in tx.outputs.withIndex()) {
                eventLogs.addAll(LogsUtils.extractEventLogs(output, block, tx, clauseIndex))

                if (!excludeVetTransfers) {
                    transferLogs.addAll(
                        LogsUtils.extractTransferLogs(output, block, tx, clauseIndex)
                    )
                }
            }
        }

        if (eventLogs.isEmpty() && transferLogs.isEmpty()) {
            return emptyList()
        }

        return processEvents(eventLogs, transferLogs)
    }

    /**
     * @param eventLogs The Thor logs to process.
     * @return A list of decoded events and their associated parameters.
     * @notice Processes all events (generic and business) in a group of log events based on the
     *   provided criteria.
     */
    protected open fun processEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog> = emptyList()
    ): List<IndexedEvent> {
        // If the abiEventProcessor is not set, return an empty list.
        if (abiEventProcessor == null) {
            logger.warn("AbiEventProcessor is not set, returning empty event list")
            return emptyList()
        }

        // Process the events using the abiEventProcessor.
        val contractEvents = abiEventProcessor?.decodeLogEvents(eventLogs) ?: emptyList()
        val vetTransfers = abiEventProcessor?.decodeLogTransfers(transferLogs) ?: emptyList()

        val allEvents = contractEvents + vetTransfers

        if (allEvents.isEmpty()) {
            return emptyList()
        }

        // If the businessEventProcessor is set, process the events further.
        return businessEventProcessor?.getEvents(allEvents) ?: allEvents
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
