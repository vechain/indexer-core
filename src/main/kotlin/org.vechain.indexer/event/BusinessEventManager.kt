package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import java.io.InputStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.utils.JsonUtils

class BusinessEventManager {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val businessEvents = mutableMapOf<String, BusinessEventDefinition>()

    /**
     * @param eventFiles A map where the key is the event name, and the value is its InputStream.
     * @notice Loads business event definitions from a map of input streams.
     * @dev Each key in the map represents an event name, and its value is the corresponding JSON
     *   InputStream.
     */
    fun loadBusinessEvents(eventFiles: Map<String, InputStream>) {
        eventFiles.forEach { (eventName, inputStream) ->
            try {
                inputStream.use { stream ->
                    val businessEventDefinition =
                        JsonUtils.mapper.readValue(
                            stream,
                            object : TypeReference<BusinessEventDefinition>() {},
                        )
                    businessEvents[eventName] = businessEventDefinition
                    logger.info("Loaded business event: $eventName")
                }
            } catch (e: Exception) {
                logger.error("Failed to load business event from $eventName: ${e.message}")
            }
        }
    }

    /**
     * Retrieves all loaded business events.
     *
     * @return A map where the key is the business event name, and the value is the definition.
     */
    fun getAllBusinessEvents(): Map<String, BusinessEventDefinition> = businessEvents

    /**
     * Retrieves specific business event definitions by their names.
     *
     * @param names The list of business event names.
     * @return A map where the key is the business event name, and the value is the corresponding
     *   definition.
     */
    fun getBusinessEventsByNames(names: List<String>): Map<String, BusinessEventDefinition> =
        names.mapNotNull { name -> businessEvents[name]?.let { name to it } }.toMap()

    /**
     * Retrieves the names of the generic events within the specified business events.
     *
     * @param names The list of business event names.
     * @return A flat list of event names within the specified business events.
     */
    fun getBusinessGenericEventNames(names: List<String>): List<String> =
        getBusinessEventsByNames(names).flatMap { (_, businessEventDefinition) ->
            businessEventDefinition.events.map { it.name }
        }

    /**
     * @param criteria The original filtering criteria.
     * @return Updated filtering criteria with additional event names for business events.
     * @notice Updates the filter criteria with business event names if applicable.
     * @dev Retrieves the generic event names for the specified business event names using the
     *   `BusinessEventManager` and adds them to the criteria.
     */
    fun updateCriteriaWithBusinessEvents(criteria: FilterCriteria): FilterCriteria {
        if (criteria.businessEventNames.isNotEmpty()) {
            val names = getBusinessGenericEventNames(criteria.businessEventNames)
            return criteria.addBusinessEventNames(names)
        }
        return criteria
    }
}
