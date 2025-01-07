package org.vechain.indexer

import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*

const val EVENTS_LIMIT = 1000L

abstract class LogsIndexer(
    override val thorClient: ThorClient,
    startBlock: Long = 0L,
    syncLoggerInterval: Long = 1_000L,
    private val criteriaSet: List<EventCriteria>,
) : BlockIndexer(thorClient, startBlock, syncLoggerInterval) {

    abstract fun processLogs(events: List<EventLog>)

    override suspend fun start(iterations: Long?) {
        // TODO:
        // - currently this logic is quite stupid and unstable. We need something more robust.
        // - index logs iteratively using `thorClient.getEventLogs`
        //      - there should be no need to handle rollbacks - we're not syncing close to best block
        //      - need to ensure we provide all logs for every block number in `processLogs`, otherwise we end up with partial block entries in the DB
        // - once we're at finalized block, stop the fast sync and start the block indexer

        val finalized = thorClient.getBestBlock().number - 1000
        val lastSynced = this.getLastSyncedBlock()

        logger.info("need to sync form ${lastSynced?.number ?: 0}")

        var offset = lastSynced?.number ?: 0
        var current = 0L

        while (current < finalized) {
            val logs = thorClient.getEventLogs(
                EventLogsRequest(
                    range = EventRange(
                        from = lastSynced?.number ?: 0,
                        to = finalized -1,
                        unit = "block",
                    ),
                    options = EventOptions(
                        offset = offset,
                        limit = EVENTS_LIMIT,
                    ),
                    criteriaSet = criteriaSet,
                    order = "acs"
                )
            ).filter { it.meta.blockNumber < finalized }

            processLogs(logs)
            if (logs.isEmpty()) { // we've reached the end of the available logs
                current = finalized
            } else {
                // this logic is incorrect. There may be more logs in the same block as the last log, excluded because they're outside the limit
                offset += logs.size
                current = logs.last().meta.blockNumber
            }
        }

        logger.info("fast sync complete, converting to a block indexer")

        super.start(iterations)
    }

    /**
     * Override the processBlock function to search events and match them based off the criteria set
     */
    override fun processBlock(block: Block) {
        val eventLogs = mutableListOf<EventLog>()

        for (tx in block.transactions) {
            for (output in tx.outputs) {
                output.events.forEach { event ->
                    for (criteria in criteriaSet) {
                        var matches = true

                        if (criteria.address != null && event.address != criteria.address) {
                            matches = false
                        }
                        if (criteria.topic0 != null) {
                            if (event.topics.isEmpty()) {
                                matches = false
                            } else if (event.topics[0] != criteria.topic0) {
                                matches = false
                            }
                        }
                        if (criteria.topic1 != null) {
                            if (event.topics.size < 2) {
                                matches = false
                            } else if (event.topics[1] != criteria.topic1) {
                                matches = false
                            }
                        }
                        if (criteria.topic2 != null) {
                            if (event.topics.size < 3) {
                                matches = false
                            } else if (event.topics[2] != criteria.topic2) {
                                matches = false
                            }
                        }
                        if (criteria.topic3 != null) {
                            if (event.topics.size < 4) {
                                matches = false
                            } else if (event.topics[3] != criteria.topic3) {
                                matches = false
                            }
                        }
                        if (criteria.topic4 != null) {
                            if (event.topics.size < 5) {
                                matches = false
                            } else if (event.topics[4] != criteria.topic4) {
                                matches = false
                            }
                        }

                        if (matches) {
                            val ev = EventLog(
                                address = event.address,
                                topics = event.topics,
                                data = event.data,
                                meta = EventMeta(
                                    blockID = block.id,
                                    blockNumber = block.number,
                                    blockTimestamp = block.timestamp,
                                    txID = tx.id,
                                    txOrigin = tx.origin,
                                    clauseIndex = 0,
                                )
                            )

                            eventLogs.add(ev)

                            return@forEach
                        }
                    }
                }
            }
        }

        if (eventLogs.isNotEmpty()) this.processLogs(eventLogs)
    }
}
