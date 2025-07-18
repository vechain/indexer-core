package org.vechain.indexer.event

import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog

open class CombinedEventProcessor
protected constructor(
    private val abiEventProcessor: AbiEventProcessor?,
    private val businessEventProcessor: BusinessEventProcessor?
) {

    companion object {
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
                if (abiBasePath != null) {
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
            return abiEvents
        }

        // If there are no ABI events, return only business events.
        if (abiEvents.isEmpty()) {
            return businessEvents
        }

        // Filter out ABI events that are already covered by business events.
        // Filter based on txId and clauseIndex combination.
        val businessEventMap = businessEvents.associateBy { it.txId to it.clauseIndex }
        return abiEvents.filterNot { event ->
            val key = event.txId to event.clauseIndex
            businessEventMap.containsKey(key)
        } + businessEvents
    }
}
