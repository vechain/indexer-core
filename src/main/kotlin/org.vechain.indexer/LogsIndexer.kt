package org.vechain.indexer

import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.GenericEventIndexer
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*

const val BLOCK_BATCH_SIZE = 100L //  Block batch size
const val LOG_FETCH_LIMIT = 1000L //  Limits logs per API call (pagination)

abstract class LogsIndexer(
    override val thorClient: ThorClient,
    startBlock: Long = 0L,
    syncLoggerInterval: Long = 1_000L,
    private val blockBatchSize: Long = BLOCK_BATCH_SIZE,
    private val logFetchLimit: Long = LOG_FETCH_LIMIT,
    private var criteriaSet: List<EventCriteria>? = null,
    final override val abiManager: AbiManager? = null,
    override val businessEventManager: BusinessEventManager? = null,
) : BlockIndexer(thorClient, startBlock, syncLoggerInterval, abiManager, businessEventManager) {
    abstract fun processLogs(logs: List<EventLog>)

    private var cachedConfiguredEvents: List<AbiElement>? = null

    private fun initialise(blockNumber: Long? = null) {
        val lastSyncedBlockNumber = blockNumber ?: getLastSyncedBlock()?.number ?: startBlock
        currentBlockNumber = lastSyncedBlockNumber
        status = Status.SYNCING
        logger.info("Initialized LogsIndexer at block: $lastSyncedBlockNumber")
    }

    override suspend fun start(iterations: Long?) {
        initialise()
        val finalizedBlock = thorClient.getBestBlock().number - 1000

        remainingIterations = iterations

        if (currentBlockNumber < finalizedBlock) {
            syncLogs(finalizedBlock)
        }

        logger.info("Fast sync complete, switching to block indexer")
        super.start(iterations) // Switch to normal block indexing
    }

    /**
     * Fetches logs iteratively and ensures complete coverage of blocks without missing events.
     */
    private suspend fun syncLogs(toBlock: Long) {
        while (currentBlockNumber < toBlock) {
            try {
                if (hasNoRemainingIterations()) return
                backoffDelay()

                // Get the next batch of blocks to process
                val batchEndBlock = minOf(currentBlockNumber + blockBatchSize, toBlock)

                // Fetch logs for this block range
                val logsBatch = fetchEventLogs(currentBlockNumber, batchEndBlock)
                if (logsBatch.isEmpty()) {
                    currentBlockNumber = batchEndBlock
                    continue
                }

                processLogs(logsBatch)

                // Update lastProcessedBlock **only after successful processing**
                currentBlockNumber = batchEndBlock
            } catch (e: Exception) {
                logger.error("Error fetching logs at block $currentBlockNumber: ${e.message}")
                handleError()
            }
        }
    }

    /**
     * Fetches event logs from the Thor client.
     */
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
                        range = EventRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = EventOptions(offset = offset, limit = logFetchLimit),
                        criteriaSet = criteriaSet,
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

    /**
     * Process events in each block matching criteria.
     */
    override fun processBlock(block: Block) {
        val eventLogs = mutableListOf<EventLog>()

        for (tx in block.transactions) {
            for (output in tx.outputs) {
                output.events.forEach { event ->
                    if (criteriaSet.isNullOrEmpty()) {
                        eventLogs.add(event.toEventLog(block, tx))
                        return@forEach
                    } else {
                        for (criteria in criteriaSet!!) {
                            val isMatching =
                                listOf(
                                    criteria.address == null || event.address == criteria.address,
                                    criteria.topic0 == null || event.topics.getOrNull(0) == criteria.topic0,
                                    criteria.topic1 == null || event.topics.getOrNull(1) == criteria.topic1,
                                    criteria.topic2 == null || event.topics.getOrNull(2) == criteria.topic2,
                                    criteria.topic3 == null || event.topics.getOrNull(3) == criteria.topic3,
                                    criteria.topic4 == null || event.topics.getOrNull(4) == criteria.topic4,
                                ).all { it }

                            if (!isMatching) continue

                            eventLogs.add(event.toEventLog(block, tx))
                            return@forEach
                        }
                    }
                }
            }
        }

        if (eventLogs.isNotEmpty()) {
            processLogs(eventLogs)
        }
    }

    private fun TxEvent.toEventLog(
        block: Block,
        tx: Transaction,
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
                    clauseIndex = 0,
                ),
        )

    /**
     * @notice Processes all events (generic and business) in a group of log events based on the provided criteria.
     * @dev Updates the filter criteria with business event names if applicable.
     *      Decodes and processes both generic and business events.
     * @param eventLogs The Thor logs to process.
     * @param criteria Filtering criteria to determine which events to process.
     * @return A list of decoded events and their associated parameters.
     */
    protected open fun processAllEvents(
        eventLogs: List<EventLog>,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        val decodedEvents = processBlockGenericEvents(eventLogs, criteria)
        return processBusinessEvents(decodedEvents, criteria.businessEventNames, criteria.removeDuplicates)
    }

    /**
     * @notice Processes generic events in a of log events based on the provided criteria.
     * @dev Requires the `AbiManager` to decode events. If not configured, skips processing and returns an empty list.
     * @param logs The Thor logs to process.
     * @param criteria Filtering criteria to determine which generic events to process.
     * @return A list of decoded generic events and their associated parameters.
     */
    protected open fun processBlockGenericEvents(
        logs: List<EventLog>,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        if (abiManager == null) {
            logger.warn("ABI Manager is not configured. Skipping generic event processing.")
            return emptyList()
        }

        val eventIndexer = GenericEventIndexer(abiManager)

        if (cachedConfiguredEvents == null) {
            val updatedCriteria = businessEventManager?.updateCriteriaWithBusinessEvents(criteria) ?: criteria
            cachedConfiguredEvents = eventIndexer.getConfiguredEvents(updatedCriteria.abiNames, updatedCriteria.eventNames)
        }

        return eventIndexer.decodeLogEvents(logs, cachedConfiguredEvents!!)
    }
}
