package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.BusinessEventUtils

/**
 * Processes events based on predefined business rules. Identifies and maps raw events to meaningful
 * business events.
 */
class BusinessEventProcessor(
    private val businessEventManager: BusinessEventManager,
    private val removeDuplicates: Boolean,
    private val onlyBusinessEvents: Boolean
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /**
     * Processes a list of raw events, separating them into remaining events and business events.
     */
    fun getEvents(events: List<IndexedEvent>): List<IndexedEvent> {
        if (onlyBusinessEvents) {
            return getOnlyBusinessEvents(events)
        }
        val businessEvents = mutableListOf<IndexedEvent>()
        val remainingEvents = mutableListOf<IndexedEvent>()

        events
            .groupBy { it.blockNumber }
            .forEach { (blockNumber, blockEvents) ->
                try {
                    blockEvents
                        .groupBy { it.txId }
                        .forEach { (_, transactionEvents) ->
                            val matchedEvents =
                                processTransactionForBusinessEvents(transactionEvents)
                            businessEvents.addAll(matchedEvents)

                            if (!removeDuplicates || matchedEvents.isEmpty()) {
                                remainingEvents.addAll(transactionEvents)
                            }
                        }
                } catch (e: Exception) {
                    logger.error("Failed to process events in block $blockNumber: $blockEvents", e)
                    remainingEvents.addAll(blockEvents)
                }
            }

        return remainingEvents + businessEvents
    }

    /**
     * Filters and returns only business events from a list of raw events.
     *
     * @param events The list of indexed events and their parameters.
     * @return A list of only business events.
     */
    private fun getOnlyBusinessEvents(events: List<IndexedEvent>): List<IndexedEvent> {
        return events
            .groupBy { it.blockNumber }
            .flatMap { (_, blockEvents) ->
                blockEvents
                    .groupBy { it.txId }
                    .flatMap { (_, transactionEvents) ->
                        processTransactionForBusinessEvents(transactionEvents)
                    }
            }
    }

    /** Processes events within a single transaction to determine if they match business events. */
    private fun processTransactionForBusinessEvents(
        txEvents: List<IndexedEvent>
    ): List<IndexedEvent> {
        // Fetch business events based on the provided names
        val businessEvents = businessEventManager.businessEvents

        val matchedBusinessEvents = mutableListOf<IndexedEvent>()

        for (definition in businessEvents) {
            // Group events by clause index if required
            val groupedEvents =
                if (definition.sameClause == true) {
                    txEvents.groupBy { it.clauseIndex.toInt() }
                } else {
                    mapOf(0 to txEvents) // Treat all events in the transaction as a single group
                }

            var definitionMatched = false

            for ((_, group) in groupedEvents) {
                // Decide whether to use exhaustive search or basic mapping
                val eventsForAlias =
                    if (definition.checkAllCombinations == true) {
                        checkAllCombinations(group, definition)
                    } else {
                        BusinessEventUtils.mapEventsToAliases(group, definition.events)
                    }

                // Validate and process matched events
                if (
                    eventsForAlias.isNotEmpty() &&
                        BusinessEventUtils.validateRules(definition.rules, eventsForAlias)
                ) {
                    matchedBusinessEvents.add(createBusinessEvent(eventsForAlias, definition))
                    definitionMatched = true
                }
            }

            if (definitionMatched) {
                break // Break if a definition is matched
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
                definition.maxAttempts ?: 10
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
