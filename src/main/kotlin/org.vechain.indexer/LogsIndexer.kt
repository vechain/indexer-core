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
     * @param toBlock The block number to sync up to.
     */
    internal suspend fun sync(toBlock: BlockIdentifier) {
        while (getCurrentBlockNumber() < toBlock.number) {
            checkIfShuttingDown()
            val batchEndBlock = minOf(getCurrentBlockNumber() + blockBatchSize - 1, toBlock.number)

            logSyncStatus(getCurrentBlockNumber(), batchEndBlock, getStatus())

            // Fetch both event logs and VET transfers
            // Only fetch event logs if we have ABI definitions
            val eventLogs =
                if (eventProcessor?.hasAbis() == true) {
                    logClient.fetchEventLogs(
                        getCurrentBlockNumber(),
                        batchEndBlock,
                        logFetchLimit,
                        eventCriteriaSet
                    )
                } else emptyList()

            val transferLogs =
                if (!excludeVetTransfers)
                    logClient.fetchTransfers(
                        getCurrentBlockNumber(),
                        batchEndBlock,
                        logFetchLimit,
                        transferCriteriaSet
                    )
                else emptyList()

            if (eventLogs.isEmpty() && transferLogs.isEmpty()) {
                setCurrentBlockNumber(batchEndBlock + 1)
                timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
                continue
            }

            // Process events and transfers
            val indexedEvents =
                eventProcessor?.processEvents(eventLogs, transferLogs) ?: emptyList()
            if (indexedEvents.isNotEmpty())
                process(IndexingResult.EventsOnly(batchEndBlock, indexedEvents))

            // Update last processed block
            setCurrentBlockNumber(batchEndBlock + 1)

            timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
        }

        // Set previous block to toBlock
        setPreviousBlock(toBlock)
    }

    private fun logSyncStatus(currentBlockNumber: Long, batchEndBlock: Long, status: Status) {
        logger.info("($status) Processing Blocks $currentBlockNumber - $batchEndBlock")
    }
}
