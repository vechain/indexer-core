package org.vechain.indexer.event.utils

import org.vechain.indexer.event.model.business.*
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.types.DecodedValue

object BusinessEventUtils {
    /**
     * Maps events to their aliases as defined in [eventDefinitions].
     */
    fun mapEventsToAliases(
        events: List<Pair<IndexedEvent, GenericEventParameters>>,
        eventDefinitions: List<Event>,
    ): Map<String, Pair<IndexedEvent, GenericEventParameters>> =
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
     */
    fun validateRules(
        rules: List<Rule>,
        events: Map<String, Pair<IndexedEvent, GenericEventParameters>>,
    ): Boolean =
        rules.all { rule ->
            val firstValue =
                events[rule.firstEventName]
                    ?.second
                    ?.getReturnValues()
                    ?.get(rule.firstEventProperty)
                    ?.toString()
            val secondValue =
                events[rule.secondEventName]
                    ?.second
                    ?.getReturnValues()
                    ?.get(rule.secondEventProperty)
                    ?.toString()
            firstValue != null && secondValue != null && rule.operator.evaluate(firstValue, secondValue)
        }

    /**
     * Extracts parameters for a business event based on its [paramsDefinition].
     */
    fun extractParams(
        paramsDefinition: List<ParamDefinition>,
        events: Map<String, Pair<IndexedEvent, GenericEventParameters>>,
    ): Map<String, Any> =
        paramsDefinition.associate { param ->
            val value = events[param.eventName]?.second?.getReturnValues()?.get(param.name) ?: ""
            param.businessEventName to value
        }

    /**
     * Validates conditions for a single event against a list of [conditions].
     */
    private fun validateConditions(
        event: Pair<IndexedEvent, GenericEventParameters>,
        conditions: List<Condition>,
    ): Boolean =
        conditions.all { condition ->
            val firstValue = resolveOperandValue(condition.firstOperand, condition.isFirstStatic, event)
            val secondValue = resolveOperandValue(condition.secondOperand, condition.isSecondStatic, event)
            condition.operator.evaluate(firstValue ?: "", secondValue ?: "")
        }

    /**
     * Resolves the value of an operand, either static or dynamic, for condition evaluation.
     */
    private fun resolveOperandValue(
        operand: String,
        isStatic: Boolean,
        event: Pair<IndexedEvent, GenericEventParameters>,
    ): String? =
        if (operand == "address") {
            event.first.address
                .lowercase()
                .trim()
        } else if (isStatic) {
            operand.lowercase().trim()
        } else {
            val value = event.second.getReturnValues()[operand]
            (value as? DecodedValue<*>)
                ?.actualValue
                ?.toString()
                ?.lowercase()
                ?.trim() ?: value?.toString()?.lowercase()?.trim()
        }
}
