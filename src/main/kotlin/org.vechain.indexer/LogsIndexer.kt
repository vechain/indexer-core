package org.vechain.indexer

import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.GenericEventIndexer
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*

const val EVENTS_LIMIT = 1000L
const val MAX_RETRIES = 5

abstract class LogsIndexer(
    override val thorClient: ThorClient,
    startBlock: Long = 0L,
    syncLoggerInterval: Long = 1_000L,
    final override val abiManager: AbiManager,
    private val criteria: FilterCriteria = FilterCriteria(),
    override val businessEventManager: BusinessEventManager? = null, // Optional BusinessEventManager
) : BlockIndexer(thorClient, startBlock, syncLoggerInterval, abiManager, businessEventManager) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    // Generic Event Indexer for decoding events
    private val genericEventIndexer = GenericEventIndexer(abiManager)

    abstract fun processLogs(events: List<Pair<IndexedEvent, GenericEventParameters>>)

    override suspend fun start(iterations: Long?) {
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
     * Fetches logs iteratively and ensures complete coverage of blocks.
     */
    private suspend fun syncLogs(
        fromBlock: Long,
        toBlock: Long,
    ): Long {
        var lastProcessedBlock = fromBlock
        var retries = 0

        val configuredEvents =
            genericEventIndexer.getConfiguredEvents(
                criteria.abiNames,
                criteria.eventNames,
            )

        while (lastProcessedBlock < toBlock) {
            try {
                val logs = fetchEventLogs(lastProcessedBlock, toBlock, configuredEvents, criteria.contractAddresses)
                if (logs.isEmpty()) break // No more logs to fetch, exit loop

                processLogs(genericEventIndexer.decodeLogEvents(logs, configuredEvents))

                // Move forward to the last block in the logs
                lastProcessedBlock = logs.maxOfOrNull { it.meta.blockNumber } ?: lastProcessedBlock

                retries = 0 // Reset retries if successful
            } catch (e: Exception) {
                logger.error("Error fetching logs at block $lastProcessedBlock: ${e.message}")
                if (++retries >= MAX_RETRIES) {
                    logger.error("Max retries reached, skipping block $lastProcessedBlock")
                    lastProcessedBlock++
                    retries = 0
                } else {
                    delay(1000L * retries) // Exponential backoff
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
        configuredEvents: List<AbiElement>,
        addresses: List<String>,
    ): List<EventLog> {
        val logs = mutableListOf<EventLog>()
        var offset = 0L

        while (true) {
            val response =
                thorClient.getEventLogs(
                    EventLogsRequest(
                        range = EventRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = EventOptions(offset = offset, limit = EVENTS_LIMIT),
                        criteriaSet = EventUtils.createEventCriteria(configuredEvents, addresses),
                        order = "asc",
                    ),
                )

            if (response.isEmpty()) break

            logs.addAll(response)

            if (response.size < EVENTS_LIMIT) break

            offset += EVENTS_LIMIT
        }

        return logs
    }

    /**
     * Process events in each block matching criteria.
     */
    override fun processBlock(block: Block) {
        val eventLogs = genericEventIndexer.getBlockEventsByFilters(block, criteria)

        if (eventLogs.isNotEmpty()) processLogs(eventLogs)
    }
}
