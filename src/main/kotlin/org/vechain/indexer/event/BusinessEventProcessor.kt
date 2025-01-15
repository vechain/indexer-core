package org.vechain.indexer.event

import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.utils.BusinessEventUtils

/**
 * Processes blockchain events based on predefined business rules.
 * Identifies and maps raw events to meaningful business events.
 */
class BusinessEventProcessor(
    private val businessEventManager: BusinessEventManager,
) {
    /**
     * Processes a list of raw events, separating them into remaining events and business events.
     */
    fun processEvents(events: List<Pair<IndexedEvent, GenericEventParameters>>): List<Pair<IndexedEvent, GenericEventParameters>> {
        val businessEvents = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()
        val remainingEvents = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()

        events.groupBy { it.first.txId }.forEach { (_, transactionEvents) ->
            transactionEvents.groupBy { it.first.clauseIndex.toInt() }.forEach { (_, clauseEvents) ->
                val businessEvent = processClauseForBusinessEvent(clauseEvents)
                if (businessEvent != null) {
                    businessEvents.add(businessEvent)
                } else {
                    remainingEvents.addAll(clauseEvents)
                }
            }
        }

        return remainingEvents + businessEvents
    }

    /**
     * Filters and returns only business events from a list of raw events.
     */
    fun getOnlyBusinessEvents(events: List<Pair<IndexedEvent, GenericEventParameters>>): List<Pair<IndexedEvent, GenericEventParameters>> =
        events
            .groupBy { it.first.txId }
            .flatMap { (_, transactionEvents) ->
                transactionEvents
                    .groupBy { it.first.clauseIndex.toInt() }
                    .mapNotNull { (_, clauseEvents) -> processClauseForBusinessEvent(clauseEvents) }
            }

    /**
     * Processes events within a single clause to determine if it matches a business event.
     */
    private fun processClauseForBusinessEvent(
        clauseEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
    ): Pair<IndexedEvent, GenericEventParameters>? =
        businessEventManager
            .getAllBusinessEvents()
            .asSequence()
            .mapNotNull { (_, definition) -> matchClauseToBusinessEvent(clauseEvents, definition) }
            .firstOrNull()

    /**
     * Matches a group of clause events to a single business event definition.
     */
    private fun matchClauseToBusinessEvent(
        clauseEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
        definition: BusinessEventDefinition,
    ): Pair<IndexedEvent, GenericEventParameters>? {
        val eventsForAlias = BusinessEventUtils.mapEventsToAliases(clauseEvents, definition.events)
        return if (BusinessEventUtils.validateRules(definition.rules, eventsForAlias)) {
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
