package org.vechain.indexer.event.utils

import org.vechain.indexer.event.model.business.*
import org.vechain.indexer.event.model.generic.IndexedEvent

object BusinessEventUtils {
    /**
     * Maps events to their aliases as defined in [eventDefinitions]. Ensures that each alias gets a
     * unique event.
     */
    fun mapEventsToAliases(
        events: List<IndexedEvent>,
        eventDefinitions: List<Event>,
    ): Map<String, IndexedEvent> {
        val assignedEvents = mutableSetOf<IndexedEvent>() // Track used events
        val mappedEvents = mutableMapOf<String, IndexedEvent>() // Store results
        for (eventDefinition in eventDefinitions) {
            events
                .firstOrNull {
                    it.params.getEventType() == eventDefinition.name &&
                        validateConditions(it, eventDefinition.conditions) &&
                        it !in assignedEvents
                }
                ?.let {
                    mappedEvents[eventDefinition.alias] = it
                    assignedEvents.add(it) // Mark this event as used
                }
        }

        if (mappedEvents.size != eventDefinitions.size) {
            return emptyMap()
        }

        return mappedEvents
    }

    /**
     * Generates valid event mappings with optimized backtracking to reduce redundant checks. Note:
     * This function uses exhaustive search and may be slow for large inputs. Not used by default.
     */
    fun generateAllValidCombinations(
        events: List<IndexedEvent>,
        eventDefinitions: List<Event>,
        maxAttempts: Int = 10,
    ): List<Map<String, IndexedEvent>> {
        val validCombinations = mutableListOf<Map<String, IndexedEvent>>()
        var attempts = 0

        fun backtrack(
            eventIndex: Int,
            aliasIndex: Int,
            used: BooleanArray,
            aliasMap: MutableMap<String, IndexedEvent>,
        ) {
            if (attempts >= maxAttempts) return
            attempts++

            if (aliasIndex == eventDefinitions.size) {
                validCombinations.add(aliasMap.toMap()) // Store valid match
                return
            }

            val eventDefinition = eventDefinitions[aliasIndex]

            for (i in eventIndex until events.size) {
                if (used[i]) continue // Skip already used events
                val event = events[i]

                if (event.params.getEventType() == eventDefinition.name) {
                    used[i] = true // Mark event as used
                    aliasMap[eventDefinition.alias] = event

                    backtrack(0, aliasIndex + 1, used, aliasMap)

                    aliasMap.remove(eventDefinition.alias) // Undo selection
                    used[i] = false // Unmark event
                }
            }
        }

        backtrack(0, 0, BooleanArray(events.size), mutableMapOf())
        return validCombinations
    }

    /** Validates the rules of a business event against matched events. */
    fun validateRules(
        rules: List<Rule>,
        events: Map<String, IndexedEvent>,
    ): Boolean =
        rules.all { rule ->
            val firstValue =
                getEventValue(events[rule.firstEventName], rule.firstEventProperty)?.toString()

            val secondValue =
                getEventValue(events[rule.secondEventName], rule.secondEventProperty)?.toString()

            firstValue != null &&
                secondValue != null &&
                rule.operator.evaluate(firstValue, secondValue)
        }

    /** Extracts parameters for a business event based on its [paramsDefinition]. */
    fun extractParams(
        paramsDefinition: List<ParamDefinition>,
        events: Map<String, IndexedEvent>,
    ): Map<String, Any> =
        paramsDefinition.associate { param ->
            val value = getEventValue(events[param.eventName], param.name) ?: ""
            param.businessEventName to value
        }

    fun extractAbiEventNames(businessEvents: List<BusinessEventDefinition>): List<String> =
        businessEvents
            .flatMap { it.events.map { e -> e.name } }
            .distinct()
            .filter { it.isNotEmpty() && !it.equals("VET_TRANSFER", ignoreCase = false) }

    fun containsVetTransferEvent(
        businessEvents: List<BusinessEventDefinition>,
    ): Boolean =
        businessEvents
            .flatMap { it.events }
            .any { e -> e.name.equals("VET_TRANSFER", ignoreCase = false) }

    /** Validates conditions for a single event against a list of [conditions]. */
    private fun validateConditions(
        event: IndexedEvent,
        conditions: List<Condition>,
    ): Boolean =
        conditions.all { condition ->
            val firstValue =
                resolveOperandValue(condition.firstOperand, condition.isFirstStatic, event)
            val secondValue =
                resolveOperandValue(condition.secondOperand, condition.isSecondStatic, event)
            condition.operator.evaluate(firstValue ?: "", secondValue ?: "")
        }

    /** Resolves the value of an operand, either static or dynamic, for condition evaluation. */
    private fun resolveOperandValue(
        operand: String,
        isStatic: Boolean,
        event: IndexedEvent,
    ): String? =
        if (isStatic) {
            operand.lowercase().trim()
        } else {
            getEventValue(event, operand)?.toString()?.lowercase()?.trim()
        }

    /**
     * Gets the value for the given operand from the event. Checks `GenericEventParameters` first,
     * then falls back to `IndexedEvent`.
     */
    private fun getEventValue(
        event: IndexedEvent?,
        operand: String,
    ): Any? = event?.params?.getReturnValues()?.get(operand) ?: event?.get(operand)
}
