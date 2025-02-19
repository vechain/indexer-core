package org.vechain.indexer

import kotlinx.coroutines.delay
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.GenericEventIndexer
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*

const val BLOCK_BATCH_SIZE = 1000L //  Adjusted batch size
const val LOG_FETCH_LIMIT = 1000L //  Limits logs per API call (pagination)
const val MAX_RETRIES = 5 //  Error handling retries

abstract class LogsIndexer(
    override val thorClient: ThorClient,
    startBlock: Long = 0L,
    syncLoggerInterval: Long = 1_000L,
    private val blockBatchSize: Long = BLOCK_BATCH_SIZE,
    final override val abiManager: AbiManager,
    private val criteria: FilterCriteria = FilterCriteria(),
    override val businessEventManager: BusinessEventManager? = null,
) : BlockIndexer(thorClient, startBlock, syncLoggerInterval, abiManager, businessEventManager) {
    private val genericEventIndexer = GenericEventIndexer(abiManager)

    abstract fun processLogs(events: List<Pair<IndexedEvent, GenericEventParameters>>)

    private fun initialise(blockNumber: Long? = null) {
        val lastSyncedBlockNumber = blockNumber ?: getLastSyncedBlock()?.number ?: startBlock
        currentBlockNumber = lastSyncedBlockNumber
        status = Status.SYNCING
        logger.info("Initialized LogsIndexer at block: $lastSyncedBlockNumber")
    }

    override suspend fun start(iterations: Long?) {
        initialise()
        val finalizedBlock = thorClient.getBestBlock().number - 1000
        var lastSynced = this.getLastSyncedBlock()?.number ?: startBlock

        logger.info("Starting log sync from block $lastSynced to $finalizedBlock")

        while (lastSynced < finalizedBlock) {
            lastSynced = syncLogs(lastSynced, finalizedBlock)
        }

        logger.info("Fast sync complete, switching to block indexer")
        super.start(iterations) // Switch to normal block indexing
    }

    /**
     * Fetches logs iteratively and ensures complete coverage of blocks without missing events.
     */
    private suspend fun syncLogs(
        fromBlock: Long,
        toBlock: Long,
    ): Long {
        var lastProcessedBlock = fromBlock
        var retries = 0

        val configuredEvents = genericEventIndexer.getConfiguredEvents(criteria.abiNames, criteria.eventNames)

        val eventCriteria =
            EventUtils.createEventCriteria(
                configuredEvents,
                criteria.contractAddresses,
            )

        while (lastProcessedBlock < toBlock) {
            try {
                val batchEndBlock = minOf(lastProcessedBlock + blockBatchSize, toBlock)

                //  Fetch logs for this block range
                val logsBatch = fetchEventLogs(lastProcessedBlock, batchEndBlock, eventCriteria)
                if (logsBatch.isEmpty()) {
                    lastProcessedBlock = batchEndBlock
                    continue
                }

                val decodedEvents = genericEventIndexer.decodeLogEvents(logsBatch, configuredEvents)
                if (decodedEvents.isNotEmpty()) {
                    processLogs(decodedEvents)
                }

                //  Update lastProcessedBlock **only after successful processing**
                lastProcessedBlock = batchEndBlock
                retries = 0 // Reset retries after success
            } catch (e: Exception) {
                logger.error("Error fetching logs at block $lastProcessedBlock: ${e.message}")

                if (++retries >= MAX_RETRIES) {
                    logger.error("Max retries reached. Restarting from last successful block $lastProcessedBlock")
                    restart(lastProcessedBlock) //  Restart from the **same block**, not skipping ahead
                    retries = 0
                } else {
                    delay(1000L * retries) //  Exponential backoff before retrying
                }
            }
        }
        return lastProcessedBlock
    }

    /**
     * Fetches event logs from the Thor client.
     */
    private suspend fun fetchEventLogs(
        fromBlock: Long,
        toBlock: Long,
        eventCriteria: List<EventCriteria>,
    ): List<EventLog> {
        val logs = mutableListOf<EventLog>()
        var offset = 0L
        while (true) {
            val response =
                thorClient.getEventLogs(
                    EventLogsRequest(
                        range = EventRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = EventOptions(offset = offset, limit = LOG_FETCH_LIMIT),
                        criteriaSet = eventCriteria,
                        order = "asc",
                    ),
                )

            if (response.isEmpty()) break
            logs.addAll(response)
            if (response.size < LOG_FETCH_LIMIT) break
            offset += LOG_FETCH_LIMIT
        }
        return logs
    }

    /**
     * Restarts processing from the last known good block.
     */
    private suspend fun restart(fromBlock: Long) {
        initialise(fromBlock)
        syncLogs(fromBlock, thorClient.getBestBlock().number - 1000)
    }

    /**
     * Process events in each block matching criteria.
     */
    override fun processBlock(block: Block) {
        val eventLogs = genericEventIndexer.getBlockEventsByFilters(block, criteria)
        if (eventLogs.isNotEmpty()) processLogs(eventLogs)
    }
}
