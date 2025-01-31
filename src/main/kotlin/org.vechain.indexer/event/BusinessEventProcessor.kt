package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.BusinessEventUtils

/**
 * Processes events based on predefined business rules.
 * Identifies and maps raw events to meaningful business events.
 */
class BusinessEventProcessor(
    private val businessEventManager: BusinessEventManager,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /**
     * Processes a list of raw events, separating them into remaining events and business events.
     */
    fun processEvents(
        events: List<Pair<IndexedEvent, GenericEventParameters>>,
        businessEventNames: List<String>,
        removeDuplicates: Boolean = true,
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        val businessEvents = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()
        val remainingEvents = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()

        events.groupBy { it.first.txId }.forEach { (_, transactionEvents) ->
            try {
                val matchedEvents = processTransactionForBusinessEvents(transactionEvents, businessEventNames)

                businessEvents.addAll(matchedEvents)

                if (!removeDuplicates || matchedEvents.isEmpty()) {
                    remainingEvents.addAll(transactionEvents)
                }
            } catch (e: Exception) {
                logger.error("Failed to process transaction events: $transactionEvents", e)
                remainingEvents.addAll(transactionEvents)
            }
        }

        return remainingEvents + businessEvents
    }

    /**
     * Filters and returns only business events from a list of raw events.
     */
    fun getOnlyBusinessEvents(
        events: List<Pair<IndexedEvent, GenericEventParameters>>,
        businessEventNames: List<String>,
    ): List<Pair<IndexedEvent, GenericEventParameters>> =
        events
            .groupBy { it.first.txId }
            .flatMap { (_, transactionEvents) -> processTransactionForBusinessEvents(transactionEvents, businessEventNames) }

    /**
     * Processes events within a single transaction to determine if they match business events.
     */
    private fun processTransactionForBusinessEvents(
        txEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
        businessEventNames: List<String>,
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        // Fetch business events based on the provided names
        val businessEvents =
            if (businessEventNames.isEmpty()) {
                businessEventManager.getAllBusinessEvents()
            } else {
                businessEventManager.getBusinessEventsByNames(businessEventNames)
            }

        val matchedBusinessEvents = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()

        for ((_, definition) in businessEvents) {
            // Group events by clause index if required
            val groupedEvents =
                if (definition.sameClause == true) {
                    txEvents.groupBy { it.first.clauseIndex.toInt() }
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
                if (eventsForAlias.isNotEmpty() && BusinessEventUtils.validateRules(definition.rules, eventsForAlias)) {
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

    /**
     * Checks all combinations of events to find valid matches based on the business definition.
     */
    private fun checkAllCombinations(
        group: List<Pair<IndexedEvent, GenericEventParameters>>,
        definition: BusinessEventDefinition,
    ): Map<String, Pair<IndexedEvent, GenericEventParameters>> {
        val eventCombinations = BusinessEventUtils.generateAllValidCombinations(group, definition.events, definition.maxAttempts ?: 10)

        for (eventsForAlias in eventCombinations) {
            if (eventsForAlias.isNotEmpty() && BusinessEventUtils.validateRules(definition.rules, eventsForAlias)) {
                return eventsForAlias // Return the first valid combination
            }
        }

        return emptyMap() // Return an empty map if no valid combination is found
    }

    /**
     * Creates a business event from matched events and the [definition].
     */
    private fun createBusinessEvent(
        eventsForAlias: Map<String, Pair<IndexedEvent, GenericEventParameters>>,
        definition: BusinessEventDefinition,
    ): Pair<IndexedEvent, GenericEventParameters> {
        val params = BusinessEventUtils.extractParams(definition.paramsDefinition, eventsForAlias)
        val firstEvent = eventsForAlias.values.first().first
        return Pair(firstEvent, GenericEventParameters(params, definition.name))
    }
}
