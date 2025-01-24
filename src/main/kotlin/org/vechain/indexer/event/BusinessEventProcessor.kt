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
            transactionEvents.groupBy { it.first.clauseIndex.toInt() }.forEach { (_, clauseEvents) ->
                try {
                    val businessEvent = processClauseForBusinessEvent(clauseEvents, businessEventNames)

                    // Add the business event if found
                    businessEvent?.let { businessEvents.add(it) }

                    // Add clauseEvents to remainingEvents if businessEvent is null or duplicates are allowed
                    if (businessEvent == null || !removeDuplicates) {
                        remainingEvents.addAll(clauseEvents)
                    }
                } catch (e: Exception) {
                    // Log the error and continue processing other events
                    logger.error("Failed to process clause events: $clauseEvents", e)
                    remainingEvents.addAll(clauseEvents) // Keep unprocessed events
                }
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
            .flatMap { (_, transactionEvents) ->
                transactionEvents
                    .groupBy { it.first.clauseIndex.toInt() }
                    .mapNotNull { (_, clauseEvents) -> processClauseForBusinessEvent(clauseEvents, businessEventNames) }
            }

    /**
     * Processes events within a single clause to determine if it matches a business event.
     */
    private fun processClauseForBusinessEvent(
        clauseEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
        businessEventNames: List<String>,
    ): Pair<IndexedEvent, GenericEventParameters>? {
        val businessEvents =
            if (businessEventNames.isEmpty()) {
                businessEventManager.getAllBusinessEvents() // Get all business events
            } else {
                businessEventManager.getBusinessEventsByNames(businessEventNames) // Filter by provided names
            }

        return businessEvents
            .asSequence()
            .mapNotNull { (_, definition) -> matchClauseToBusinessEvent(clauseEvents, definition) }
            .firstOrNull()
    }

    /**
     * Matches a group of clause events to a single business event definition.
     */
    private fun matchClauseToBusinessEvent(
        clauseEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
        definition: BusinessEventDefinition,
    ): Pair<IndexedEvent, GenericEventParameters>? {
        val eventsForAlias = BusinessEventUtils.mapEventsToAliases(clauseEvents, definition.events)
        return if (eventsForAlias.isNotEmpty() && BusinessEventUtils.validateRules(definition.rules, eventsForAlias)) {
            createBusinessEvent(eventsForAlias, definition)
        } else {
            null
        }
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
