package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.BusinessEventUtils.extractAbiEventNames
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog

open class EventProcessor protected constructor(
    private val abiEventProcessor: AbiEventProcessor?,
    private val businessEventProcessor: BusinessEventProcessor?
) {
    private val logger: Logger = LoggerFactory.getLogger(EventProcessor::class.java)

    companion object {
        fun create(
            abiFiles: List<String>,
            eventNames: List<String>,
            contractAddresses: List<String>,
            includeVetTransfers: Boolean,
            includeEvents: Boolean,
            businessEventFiles: List<String>,
            businessEventNames: List<String>,
            substitutionParams: Map<String, String>
        ): EventProcessor {
            val businessEvents =
                if (includeEvents) {
                    BusinessEventLoader.loadBusinessEvents(
                        businessEventFiles,
                        businessEventNames,
                        substitutionParams
                    )
                } else {
                    emptyList()
                }

            val businessEventProcessor =
                businessEvents
                    .takeIf { includeEvents && it.isNotEmpty() }
                    ?.let {
                        BusinessEventProcessor(
                            businessEvents = it,
                            removeDuplicates = true,
                            onlyBusinessEvents = false
                        )
                    }

            val updatedEventNames =
                if (includeEvents) {
                    (eventNames + extractAbiEventNames(businessEvents)).distinct()
                } else {
                    emptyList()
                }

            val abiEventProcessor =
                AbiEventProcessor(
                    eventAbis = AbiLoader.loadEvents(abiFiles, updatedEventNames),
                    contractAddresses = contractAddresses,
                    includeVetTransfers = includeVetTransfers
                )

            return EventProcessor(
                abiEventProcessor = abiEventProcessor,
                businessEventProcessor = businessEventProcessor
            )
        }
    }

    /**
     * @param block The block containing events to process.
     * @return A list of decoded events and their associated parameters.
     */
    fun processEvents(block: Block): List<IndexedEvent> {
        // If the abiEventProcessor is not set, return an empty list.
        if (abiEventProcessor == null) {
            logger.debug("AbiEventProcessor is not set, returning empty event list")
            return emptyList()
        }

        // Process the events using the abiEventProcessor.
        val events = abiEventProcessor.getEvents(block)

        if (events.isEmpty()) {
            logger.debug("No events found in block ${block.number}")
            return emptyList()
        }

        // If the businessEventProcessor is set, process the events further.
        return businessEventProcessor?.getEvents(events) ?: events
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
        // If the abiEventProcessor is not set, return an empty list.
        if (abiEventProcessor == null) {
            logger.debug("AbiEventProcessor is not set, returning empty event list")
            return emptyList()
        }

        // Process the events using the abiEventProcessor.
        val contractEvents = abiEventProcessor.decodeLogEvents(eventLogs)
        val vetTransfers = abiEventProcessor.decodeLogTransfers(transferLogs)

        val allEvents = contractEvents + vetTransfers

        if (allEvents.isEmpty()) {
            return emptyList()
        }

        // If the businessEventProcessor is set, process the events further.
        return businessEventProcessor?.getEvents(allEvents) ?: allEvents
    }
}
