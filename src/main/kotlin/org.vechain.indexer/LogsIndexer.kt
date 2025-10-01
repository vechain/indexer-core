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

    protected open val logClient = LogClient(thorClient)

    /**
     * Synchronizes logs from the current block to the target block.
     *
     * @param toBlock The block number to sync up to.
     */
    open suspend fun sync(toBlock: Long) {
        while (currentBlockNumber < toBlock) {
            try {
                val batchEndBlock = minOf(currentBlockNumber + blockBatchSize, toBlock)

                logSyncStatus(currentBlockNumber, batchEndBlock, status)

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
                if (indexedEvents.isNotEmpty())
                    process(IndexingResult.EventsOnly(batchEndBlock, indexedEvents))

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

    private fun logSyncStatus(currentBlockNumber: Long, batchEndBlock: Long, status: Status) {
        logger.info("($status) Processing Blocks $currentBlockNumber - $batchEndBlock")
    }

    internal suspend fun fastSync() {

        val finalizedBlock = thorClient.getFinalizedBlock().number

        if (currentBlockNumber < finalizedBlock) {
            sync(finalizedBlock)
        }

        logger.info("Fast sync complete, switching to block indexer")
        // Before running reset the previousBlock
        previousBlock = null
    }
}
