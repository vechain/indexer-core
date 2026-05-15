package org.vechain.indexer.event

import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.IndexedEventOrder
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog

open class CombinedEventProcessor
protected constructor(
    private val abiEventProcessor: AbiEventProcessor?,
    private val businessEventProcessor: BusinessEventProcessor?
) {

    companion object {
        /** Thor's hard cap on `criteriaSet` length for `/logs/event` requests. */
        const val MAX_CRITERIA = 10

        private val logger = LoggerFactory.getLogger(CombinedEventProcessor::class.java)

        fun create(
            abiBasePath: String?,
            abiEventNames: List<String>,
            abiContracts: List<String>,
            includeVetTransfers: Boolean,
            businessEventPath: String?,
            businessEventAbiBasePath: String?,
            businessEventNames: List<String>,
            businessEventContracts: List<String>,
            substitutionParams: Map<String, String>
        ): CombinedEventProcessor {

            val businessEventProcessor =
                if (businessEventPath != null) {
                    if (businessEventAbiBasePath == null) {
                        throw IllegalArgumentException(
                            "Business event ABI path must be provided if business event path is set."
                        )
                    }
                    BusinessEventProcessor(
                        businessEventBasePath = businessEventPath,
                        abiBasePath = businessEventAbiBasePath,
                        businessEventNames = businessEventNames,
                        businessEventContracts = businessEventContracts,
                        substitutionParams = substitutionParams
                    )
                } else {
                    null
                }

            val abiEventProcessor =
                if (abiBasePath != null || includeVetTransfers) {
                    AbiEventProcessor(
                        basePath = abiBasePath,
                        eventNames = abiEventNames,
                        contractAddresses = abiContracts,
                        includeVetTransfers = includeVetTransfers
                    )
                } else {
                    null
                }

            return CombinedEventProcessor(
                abiEventProcessor = abiEventProcessor,
                businessEventProcessor = businessEventProcessor,
            )
        }
    }

    /**
     * Returns whether the processor has any ABIs events loaded. This can be used to reduce
     * unnecessary calls to thor
     */
    fun hasAbis(): Boolean {
        return businessEventProcessor != null ||
            (abiEventProcessor != null && abiEventProcessor.eventAbis.isNotEmpty())
    }

    /** Returns whether any business event requires VET transfer logs. */
    fun needsVetTransfers(): Boolean = businessEventProcessor?.needsVetTransfers == true

    /**
     * Derives a Thor event-log `criteriaSet` covering every (contract, event) pair that this
     * processor can decode. Used by [org.vechain.indexer.IndexerFactory] to push ABI filtering
     * server-side when the consumer has not supplied an explicit criteria set.
     *
     * Thor caps `criteriaSet` at [MAX_CRITERIA] entries. When the full cartesian product exceeds
     * that, this method falls back progressively: address-only criteria (drop the topic0 axis),
     * then topic0-only criteria (drop addresses), then an empty list (no filter). Returns an empty
     * list when no event ABIs are loaded.
     *
     * Address-only is preferred over topic0-only because addresses pin to specific contracts, while
     * a single topic0 (e.g. `Transfer`) matches every contract that emits it — typically a much
     * larger fraction of all logs. Empirically, topic0-only OR-filters that include common
     * signatures cost roughly the same as no filter at all on Thor but with extra matching
     * overhead, so we prefer the axis the consumer narrowed deliberately.
     */
    fun deriveEventCriteria(): List<EventCriteria> {
        val abiCriteria = abiEventProcessor?.buildEventCriteria() ?: emptyList()
        val businessCriteria = businessEventProcessor?.buildEventCriteria() ?: emptyList()
        val full = (abiCriteria + businessCriteria).distinct()
        if (full.size <= MAX_CRITERIA) return full

        val addressOnly =
            full.mapNotNull { it.address }.distinct().map { EventCriteria(address = it) }
        if (addressOnly.isNotEmpty() && addressOnly.size <= MAX_CRITERIA) {
            logger.info(
                "Cartesian criteria set ({}) exceeds Thor's cap of {}; using {} address-only criteria",
                full.size,
                MAX_CRITERIA,
                addressOnly.size,
            )
            return addressOnly
        }

        val topic0Only = full.mapNotNull { it.topic0 }.distinct().map { EventCriteria(topic0 = it) }
        if (topic0Only.isNotEmpty() && topic0Only.size <= MAX_CRITERIA) {
            logger.info(
                "Cartesian criteria set ({}) exceeds Thor's cap of {}; address-only ({}) also exceeds the cap, using {} topic0-only criteria",
                full.size,
                MAX_CRITERIA,
                addressOnly.size,
                topic0Only.size,
            )
            return topic0Only
        }

        logger.warn(
            "Cannot fit derived criteria under Thor's cap of {} (cartesian: {}, address-only: {}, topic0-only: {}); falling back to no filter",
            MAX_CRITERIA,
            full.size,
            addressOnly.size,
            topic0Only.size,
        )
        return emptyList()
    }

    /**
     * @param block The block containing events to process.
     * @return A list of decoded events and their associated parameters.
     */
    fun processEvents(block: Block): List<IndexedEvent> {
        // Attempt to process events using the abiEventProcessor.
        val abiEvents = abiEventProcessor?.processEvents(block) ?: emptyList()

        // Attempt to process business events if the businessEventProcessor is set.
        val businessEvents = businessEventProcessor?.processEvents(block) ?: emptyList()

        return deduplicateEvents(abiEvents, businessEvents)
    }

    /**
     * @param eventLogs The Thor logs to process.
     * @return A list of decoded events and their associated parameters.
     * @notice Processes all events (generic and business) in a group of log events based on the
     *   provided criteria.
     */
    fun processEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog> = emptyList()
    ): List<IndexedEvent> {
        // Attempt to process events using the abiEventProcessor.
        val abiEvents = abiEventProcessor?.processEvents(eventLogs, transferLogs) ?: emptyList()

        // Attempt to process business events if the businessEventProcessor is set.
        val businessEvents =
            businessEventProcessor?.processEvents(eventLogs, transferLogs) ?: emptyList()

        return deduplicateEvents(abiEvents, businessEvents)
    }

    /**
     * If there is an abiEvent that is covered by a business event it will be removed from the
     * result
     *
     * @param abiEvents The list of ABI events to process.
     * @param businessEvents The list of business events to process.
     */
    protected fun deduplicateEvents(
        abiEvents: List<IndexedEvent>,
        businessEvents: List<IndexedEvent>
    ): List<IndexedEvent> {
        if (businessEvents.isEmpty()) {
            return IndexedEventOrder.sortChronologically(abiEvents)
        }

        // If there are no ABI events, return only business events.
        if (abiEvents.isEmpty()) {
            return IndexedEventOrder.sortChronologically(businessEvents)
        }

        // Filter out ABI events that are already covered by business events.
        // Filter based on txId and clauseIndex combination.
        val businessEventMap = businessEvents.associateBy { it.txId to it.clauseIndex }
        return IndexedEventOrder.sortChronologically(
            abiEvents.filterNot { event ->
                val key = event.txId to event.clauseIndex
                businessEventMap.containsKey(key)
            } + businessEvents
        )
    }
}
