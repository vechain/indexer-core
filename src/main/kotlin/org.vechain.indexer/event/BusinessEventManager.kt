package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.utils.JsonUtils
import java.io.File

class BusinessEventManager {
    private val businessEvents = mutableMapOf<String, BusinessEventDefinition>()

    /**
     * Loads business events from the specified resource path into memory.
     *
     * @param resourcePath The path to the business event JSON files (e.g., "businessEvents").
     */
    fun loadBusinessEvents(resourcePath: String) {
        val classLoader = Thread.currentThread().contextClassLoader
        val resourceDir =
            classLoader.getResource(resourcePath)
                ?: throw IllegalArgumentException("Invalid business events directory: $resourcePath")

        val eventFiles =
            File(resourceDir.toURI()).listFiles { file -> file.extension == "json" }

        if (eventFiles.isNullOrEmpty()) {
            println("No business event files found in directory: $resourcePath")
            return
        }

        eventFiles.forEach { file ->
            try {
                val businessEventDefinition =
                    JsonUtils.mapper.readValue(
                        file.inputStream(),
                        object : TypeReference<BusinessEventDefinition>() {},
                    )
                val businessEventName = file.nameWithoutExtension
                businessEvents[businessEventName] = businessEventDefinition
            } catch (e: Exception) {
                println("Failed to load business event from ${file.name}: ${e.message}")
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
     * @return A map where the key is the business event name, and the value is the corresponding definition.
     */
    fun getBusinessEventsByNames(names: List<String>): Map<String, BusinessEventDefinition> =
        names
            .mapNotNull { name ->
                businessEvents[name]?.let { name to it }
            }.toMap()

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
}
