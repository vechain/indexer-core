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
 *
 * @param thorClient The Thor blockchain client instance.
 * @param startBlock The starting block number for indexing.
 * @param syncLoggerInterval Frequency of log sync status updates.
 * @param excludeVetTransfers If true, excludes VET transfer logs from processing.
 * @param blockBatchSize Number of blocks fetched per batch.
 * @param logFetchLimit Maximum number of logs fetched per API call.
 * @param pruner Optional pruner for cleaning up old data.
 * @param eventCriteriaSet Filtering criteria for event logs.
 * @param transferCriteriaSet Filtering criteria for transfer logs.
 * @param eventProcessor Optional event processor for handling indexed events. events.
 */
open class LogsIndexer(
    name: String,
    override val thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    private val syncLoggerInterval: Long,
    private val excludeVetTransfers: Boolean,
    private val blockBatchSize: Long,
    private val logFetchLimit: Long,
    private var eventCriteriaSet: List<EventCriteria>?,
    private var transferCriteriaSet: List<TransferCriteria>?,
    override val eventProcessor: CombinedEventProcessor?,
    pruner: Pruner?,
) :
    BlockIndexer(
        name,
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        eventProcessor,
        pruner,
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
                    logger.info("($status) Processing Blocks $currentBlockNumber - $batchEndBlock")
                }

                // Fetch both event logs and VET transfers
                // Only fetch event logs if we have ABI definitions
                val eventLogs =
                    if (eventProcessor?.hasAbis() == true) {
                        logClient.fetchEventLogs(
                            currentBlockNumber,
                            batchEndBlock,
                            logFetchLimit,
                            eventCriteriaSet
                        )
                    } else emptyList()

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
                val indexedEvents =
                    eventProcessor?.processEvents(eventLogs, transferLogs) ?: emptyList()
                if (indexedEvents.isNotEmpty()) process(indexedEvents)

                // Update last processed block
                currentBlockNumber = batchEndBlock + 1
                timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
            } catch (e: Exception) {
                logger.error("Error fetching logs at block $currentBlockNumber: ${e.message}")
                handleError()
            }
        }
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
