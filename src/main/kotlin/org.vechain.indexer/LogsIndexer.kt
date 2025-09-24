package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.RestartIndexerException
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
    prunerInterval: Long,
    dependsOn: Set<Indexer>,
) :
    PreSyncIndexer(
        name,
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        eventProcessor,
        pruner,
        prunerInterval,
        dependsOn
    ) {

    protected open val logClient = LogClient(thorClient)

    /**
     * Synchronizes logs from the current block to the target block.
     *
     * @param toBlock The block number to sync up to.
     */
    override suspend fun sync(toBlock: Long) {
        while (currentBlockNumber < toBlock) {
            waitForDependenciesIfRequired()
            try {
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
                logger.error(
                    "Restarting sync due to error syncing at block $currentBlockNumber: ${e.message}"
                )
                throw RestartIndexerException()
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
