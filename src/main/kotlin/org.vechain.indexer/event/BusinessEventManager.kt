package org.vechain.indexer.event

import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.generic.FilterCriteria

class BusinessEventManager(
    private val eventFiles: List<String>,
    envParams: Map<String, String> = emptyMap()
) : ResourceManager(envParams) {

    private val businessEvents = mutableMapOf<String, BusinessEventDefinition>()

    init {
        loadBusinessEvents().forEach { event ->
            businessEvents[event.name] = event
            logger.info("Loaded business event: ${event.name}")
        }
    }

    fun loadBusinessEvents(): List<BusinessEventDefinition> {
        val loadedEvents = mutableListOf<BusinessEventDefinition>()

        for (path in eventFiles) {
            val substitutedJson = readAndSubstituteJson(path) ?: continue
            try {
                val event =
                    objectMapper.readValue(substitutedJson, BusinessEventDefinition::class.java)
                loadedEvents.add(event)
            } catch (ex: Exception) {
                logger.error("❌ Error parsing file $path: ${ex.message}")
                throw ex
            }
        }
        return loadedEvents
    }

    // ...existing code (getAllBusinessEvents, getBusinessEventsByNames, etc.)...
    fun getAllBusinessEvents(): Map<String, BusinessEventDefinition> = businessEvents

    fun getBusinessEventsByNames(names: List<String>): Map<String, BusinessEventDefinition> =
        names.mapNotNull { name -> businessEvents[name]?.let { name to it } }.toMap()

    fun getBusinessGenericEventNames(names: List<String>): List<String> =
        getBusinessEventsByNames(names).flatMap { (_, businessEventDefinition) ->
            businessEventDefinition.events.map { it.name }
        }

    fun updateCriteriaWithBusinessEvents(criteria: FilterCriteria): FilterCriteria {
        if (criteria.businessEventNames.isNotEmpty()) {
            val names = getBusinessGenericEventNames(criteria.businessEventNames)
            return criteria.addBusinessEventNames(names)
        }
        return criteria
    }
}
