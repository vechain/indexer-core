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
        val businessEvents =
            if (businessEventNames.isEmpty()) {
                businessEventManager.getAllBusinessEvents()
            } else {
                businessEventManager.getBusinessEventsByNames(businessEventNames)
            }

        val matchedBusinessEvents = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()

        for ((_, definition) in businessEvents) {
            val groupedEvents =
                if (definition.sameClause == true) {
                    txEvents.groupBy { it.first.clauseIndex.toInt() }
                } else {
                    mapOf(0 to txEvents) // Treat the entire transaction as a single group
                }

            for ((_, group) in groupedEvents) {
                val eventsForAlias = BusinessEventUtils.mapEventsToAliases(group, definition.events)
                if (eventsForAlias.isNotEmpty() && BusinessEventUtils.validateRules(definition.rules, eventsForAlias)) {
                    matchedBusinessEvents.add(createBusinessEvent(eventsForAlias, definition))
                }
            }
        }

        return matchedBusinessEvents
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
