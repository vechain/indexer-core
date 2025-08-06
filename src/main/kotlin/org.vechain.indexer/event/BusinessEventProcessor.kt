package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.BusinessEventUtils
import org.vechain.indexer.event.utils.BusinessEventUtils.containsVetTransferEvent
import org.vechain.indexer.event.utils.BusinessEventUtils.extractAbiEventNames
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog

/**
 * Processes events based on predefined business rules. Identifies and maps raw events to meaningful
 * business events.
 */
open class BusinessEventProcessor(
    businessEventBasePath: String,
    abiBasePath: String,
    businessEventNames: List<String>,
    businessEventContracts: List<String>,
    substitutionParams: Map<String, String>,
) : EventProcessor {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    // Load business events from the provided files and names.
    private val businessEvents: List<BusinessEventDefinition> =
        BusinessEventLoader.loadBusinessEvents(
            basePath = businessEventBasePath,
            eventNames = businessEventNames,
            envParams = substitutionParams,
        )

    private val abiEventProcessor: AbiEventProcessor =
        AbiEventProcessor(
            basePath = abiBasePath,
            eventNames = extractAbiEventNames(businessEvents),
            contractAddresses = businessEventContracts,
            includeVetTransfers = containsVetTransferEvent(businessEvents),
        )

    override fun processEvents(block: Block): List<IndexedEvent> {
        val events = abiEventProcessor.processEvents(block)
        return processBusinessEvents(events)
    }

    override fun processEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>,
    ): List<IndexedEvent> {
        val events = abiEventProcessor.processEvents(eventLogs, transferLogs)
        return processBusinessEvents(events)
    }

    /**
     * Filters and returns only business events from a list of raw events.
     *
     * @param events The list of indexed events and their parameters.
     * @return A list of only business events.
     */
    private fun processBusinessEvents(events: List<IndexedEvent>): List<IndexedEvent> =
        events
            .groupBy { it.blockNumber }
            .flatMap { (_, blockEvents) ->
                blockEvents
                    .groupBy { it.txId }
                    .flatMap { (_, transactionEvents) ->
                        processTransactionForBusinessEvents(transactionEvents)
                    }
            }

    /** Processes events within a single transaction to determine if they match business events. */
    private fun processTransactionForBusinessEvents(
        txEvents: List<IndexedEvent>
    ): List<IndexedEvent> {
        val remainingEvents = txEvents.toMutableList()
        val matchedBusinessEvents = mutableListOf<IndexedEvent>()

        for (definition in businessEvents) {
            if (remainingEvents.isEmpty()) break

            val groupedEvents =
                if (definition.sameClause == true) {
                    remainingEvents.groupBy { it.clauseIndex.toInt() }
                } else {
                    mapOf(0 to remainingEvents)
                }

            for ((_, group) in groupedEvents) {
                if (group.isEmpty()) continue

                // Make a local copy of group to remove matched events iteratively
                val localGroup = group.toMutableList()

                var foundMatch: Boolean
                do {
                    foundMatch = false

                    val eventsForAlias =
                        if (definition.checkAllCombinations == true) {
                            checkAllCombinations(localGroup, definition)
                        } else {
                            BusinessEventUtils.mapEventsToAliases(localGroup, definition.events)
                        }

                    if (
                        eventsForAlias.isNotEmpty() &&
                            BusinessEventUtils.validateRules(definition.rules, eventsForAlias)
                    ) {
                        val matchedEventIds = eventsForAlias.values.map { it.id }.toSet()

                        // Remove from both local group and global remainingEvents
                        localGroup.removeIf { it.id in matchedEventIds }
                        remainingEvents.removeIf { it.id in matchedEventIds }

                        matchedBusinessEvents.add(createBusinessEvent(eventsForAlias, definition))

                        foundMatch = true
                    }
                } while (foundMatch && localGroup.isNotEmpty())
            }
        }

        return matchedBusinessEvents
    }

    /** Checks all combinations of events to find valid matches based on the business definition. */
    private fun checkAllCombinations(
        group: List<IndexedEvent>,
        definition: BusinessEventDefinition,
    ): Map<String, IndexedEvent> {
        val eventCombinations =
            BusinessEventUtils.generateAllValidCombinations(
                group,
                definition.events,
                definition.maxAttempts ?: 10,
            )

        for (eventsForAlias in eventCombinations) {
            if (
                eventsForAlias.isNotEmpty() &&
                    BusinessEventUtils.validateRules(definition.rules, eventsForAlias)
            ) {
                return eventsForAlias // Return the first valid combination
            }
        }

        return emptyMap() // Return an empty map if no valid combination is found
    }

    /** Creates a business event from matched events and the [definition]. */
    private fun createBusinessEvent(
        eventsForAlias: Map<String, IndexedEvent>,
        definition: BusinessEventDefinition,
    ): IndexedEvent {
        val params = BusinessEventUtils.extractParams(definition.paramsDefinition, eventsForAlias)
        val firstEvent = eventsForAlias.values.first()
        return IndexedEvent(
            id = firstEvent.id,
            blockId = firstEvent.blockId,
            blockNumber = firstEvent.blockNumber,
            blockTimestamp = firstEvent.blockTimestamp,
            txId = firstEvent.txId,
            origin = firstEvent.origin,
            paid = firstEvent.paid,
            gasUsed = firstEvent.gasUsed,
            gasPayer = firstEvent.gasPayer,
            params = AbiEventParameters(params, definition.name),
            eventType = definition.name,
            clauseIndex = firstEvent.clauseIndex,
        )
    }
}
