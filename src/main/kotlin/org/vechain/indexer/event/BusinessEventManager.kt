package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.springframework.core.io.Resource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.utils.JsonUtils

class BusinessEventManager {
    private val businessEvents = mutableMapOf<String, BusinessEventDefinition>()

    /**
     * Loads business events from the specified resource path into memory.
     *
     * @param resourcePath The path to the business event JSON files.
     */
    fun loadBusinessEvents(resourcePath: String) {
        val resolver = PathMatchingResourcePatternResolver()
        val resources: Array<Resource> = resolver.getResources(resourcePath)
        resources.forEach { resource ->
            try {
                val businessEventDefinition =
                    JsonUtils.mapper.readValue(
                        resource.inputStream,
                        object : TypeReference<BusinessEventDefinition>() {},
                    )
                val businessEventName = resource.filename?.removeSuffix(".json") ?: "unknown"
                businessEvents[businessEventName] = businessEventDefinition
            } catch (e: Exception) {
                println("Failed to load business event from ${resource.filename}: ${e.message}")
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
     * Retrieves a specific business event definition by its name.
     *
     * @param name The name of the business event.
     * @return The corresponding business event definition, or null if not found.
     */
    fun getBusinessEventByName(name: String): BusinessEventDefinition? = businessEvents[name]
}
