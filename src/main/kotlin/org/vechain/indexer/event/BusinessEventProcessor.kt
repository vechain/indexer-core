package org.vechain.indexer.event

import org.vechain.indexer.event.model.business.*
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.types.DecodedValue

/**
 * Processes blockchain events based on predefined business rules.
 * Identifies and maps raw events to meaningful business events defined by [BusinessEventDefinition].
 */
class BusinessEventProcessor(
    private val businessEventManager: BusinessEventManager,
) {
    /**
     * Processes a list of raw events, separating them into remaining events and business events.
     * Clauses with a business event only return the business event; other clauses remain as-is.
     *
     * @param events List of raw events paired with their parameters.
     * @return A pair containing remaining events and business events.
     */
    fun processEvents(events: List<Pair<IndexedEvent<*>, GenericEventParameters>>): List<Pair<IndexedEvent<*>, GenericEventParameters>> {
        val businessEvents = mutableListOf<Pair<IndexedEvent<*>, GenericEventParameters>>()
        val remainingEvents = mutableListOf<Pair<IndexedEvent<*>, GenericEventParameters>>()

        // Group events by transaction and process each clause within the transaction
        events.groupBy { it.first.txId }.forEach { (_, transactionEvents) ->
            groupEventsByClause(transactionEvents).forEach { (_, clauseEvents) ->
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
     *
     * @param events List of raw events paired with their parameters.
     * @return List of matched business events.
     */
    fun getOnlyBusinessEvents(
        events: List<Pair<IndexedEvent<*>, GenericEventParameters>>,
    ): List<Pair<IndexedEvent<*>, GenericEventParameters>> {
        val businessEvents = mutableListOf<Pair<IndexedEvent<*>, GenericEventParameters>>()

        // Group events by transaction and process each clause within the transaction
        events.groupBy { it.first.txId }.forEach { (_, transactionEvents) ->
            groupEventsByClause(transactionEvents).forEach { (_, clauseEvents) ->
                val businessEvent = processClauseForBusinessEvent(clauseEvents)
                businessEvent?.let { businessEvents.add(it) }
            }
        }

        return businessEvents
    }

    /**
     * Processes events within a single clause to determine if it matches a business event.
     *
     * @param clauseEvents Events in a single clause.
     * @return A matched business event if found, null otherwise.
     */
    private fun processClauseForBusinessEvent(
        clauseEvents: List<Pair<IndexedEvent<*>, GenericEventParameters>>,
    ): Pair<IndexedEvent<*>, GenericEventParameters>? {
        businessEventManager.getAllBusinessEvents().forEach { (_, definition) ->
            val businessEvent = matchClauseToBusinessEvent(clauseEvents, definition)
            if (businessEvent != null) {
                return businessEvent
            }
        }
        return null
    }

    /**
     * Matches a group of clause events to a single business event definition.
     *
     * @param clauseEvents Events grouped by clause.
     * @param definition The business event definition to match against.
     * @return A matched business event, or null if no match is found.
     */
    private fun matchClauseToBusinessEvent(
        clauseEvents: List<Pair<IndexedEvent<*>, GenericEventParameters>>,
        definition: BusinessEventDefinition,
    ): Pair<IndexedEvent<*>, GenericEventParameters>? {
        val eventsForAlias = mapEventsToAliases(clauseEvents, definition.events)
        return if (validateRules(definition.rules, eventsForAlias)) {
            createBusinessEvent(eventsForAlias, definition)
        } else {
            null
        }
    }

    /**
     * Groups events by their clause index. If no clause index exists, groups them under `null`.
     *
     * @param events List of events to group.
     * @return A map of clause index to grouped events.
     */
    private fun groupEventsByClause(
        events: List<Pair<IndexedEvent<*>, GenericEventParameters>>,
    ): Map<Int?, List<Pair<IndexedEvent<*>, GenericEventParameters>>> = events.groupBy { it.first.clauseIndex?.toInt() }

    /**
     * Maps events to their aliases as defined in [eventDefinitions].
     *
     * @param events List of events to map.
     * @param eventDefinitions List of event definitions.
     * @return A map of event aliases to matched events and their parameters.
     */
    private fun mapEventsToAliases(
        events: List<Pair<IndexedEvent<*>, GenericEventParameters>>,
        eventDefinitions: List<Event>,
    ): Map<String, Pair<IndexedEvent<*>, GenericEventParameters>> =
        eventDefinitions
            .mapNotNull { eventDefinition ->
                val matchedEvent =
                    events.firstOrNull {
                        it.second.getEventType() == eventDefinition.name &&
                            validateConditions(it, eventDefinition.conditions)
                    }
                matchedEvent?.let { eventDefinition.alias to it }
            }.toMap()

    /**
     * Validates the rules of a business event against matched events.
     *
     * @param rules List of rules to validate.
     * @param events Map of event aliases to matched events.
     * @return True if all rules are satisfied, false otherwise.
     */
    private fun validateRules(
        rules: List<Rule>,
        events: Map<String, Pair<IndexedEvent<*>, GenericEventParameters>>,
    ): Boolean =
        rules.all { rule ->
            val firstEvent =
                events[rule.firstEventName]
                    ?.second
                    ?.getReturnValues()
                    ?.get(rule.firstEventProperty)
                    ?.toString()
            val secondEvent =
                events[rule.secondEventName]
                    ?.second
                    ?.getReturnValues()
                    ?.get(rule.secondEventProperty)
                    ?.toString()
            firstEvent != null && secondEvent != null && rule.operator.evaluate(firstEvent, secondEvent)
        }

    /**
     * Creates a business event from matched events and the [definition].
     *
     * @param eventsForAlias Map of event aliases to matched events.
     * @param definition The business event definition.
     * @return A pair of the first matched event and the generated business event parameters.
     */
    private fun createBusinessEvent(
        eventsForAlias: Map<String, Pair<IndexedEvent<*>, GenericEventParameters>>,
        definition: BusinessEventDefinition,
    ): Pair<IndexedEvent<*>, GenericEventParameters> {
        val params = extractParams(definition.paramsDefinition, eventsForAlias)
        val firstEvent = eventsForAlias.values.first().first
        return Pair(firstEvent, GenericEventParameters(params, definition.name))
    }

    /**
     * Extracts parameters for a business event based on its [paramsDefinition].
     *
     * @param paramsDefinition List of parameter definitions.
     * @param events Map of event aliases to matched events.
     * @return A map of parameter names to their values.
     */
    private fun extractParams(
        paramsDefinition: List<ParamDefinition>,
        events: Map<String, Pair<IndexedEvent<*>, GenericEventParameters>>,
    ): Map<String, Any> =
        paramsDefinition.associate { param ->
            val value = events[param.eventName]?.second?.getReturnValues()?.get(param.name) ?: ""
            param.businessEventName to value
        }

    /**
     * Validates conditions for a single event against a list of [conditions].
     *
     * @param event The event to validate.
     * @param conditions The conditions to validate.
     * @return True if all conditions are satisfied, false otherwise.
     */
    private fun validateConditions(
        event: Pair<IndexedEvent<*>, GenericEventParameters>,
        conditions: List<Condition>,
    ): Boolean =
        conditions.all { condition ->
            val firstValue = resolveOperandValue(condition.firstOperand, condition.isFirstStatic, event)
            val secondValue = resolveOperandValue(condition.secondOperand, condition.isSecondStatic, event)
            condition.operator.evaluate(firstValue ?: "", secondValue ?: "")
        }

    /**
     * Resolves the value of an operand, either static or dynamic, for condition evaluation.
     *
     * @param operand The operand to resolve.
     * @param isStatic Whether the operand is static.
     * @param event The event containing dynamic data.
     * @return The resolved value of the operand.
     */
    private fun resolveOperandValue(
        operand: String,
        isStatic: Boolean,
        event: Pair<IndexedEvent<*>, GenericEventParameters>,
    ): String? =
        if (operand == "address") {
            event.first.address
                .lowercase()
                .trim()
        } else if (isStatic) {
            operand.lowercase().trim()
        } else {
            val value = event.second.getReturnValues()[operand]
            if (value is DecodedValue<*>) {
                value.actualValue
                    ?.toString()
                    ?.lowercase()
                    ?.trim()
            } else {
                value?.toString()?.lowercase()?.trim()
            }
        }
}
