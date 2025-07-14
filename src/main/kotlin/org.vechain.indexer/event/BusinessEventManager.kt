package org.vechain.indexer.event

import org.vechain.indexer.event.model.business.BusinessEventDefinition

class BusinessEventManager(
    private val eventFiles: List<String>,
    private val eventNames: List<String>,
    envParams: Map<String, String> = emptyMap()
) : ResourceManager(envParams) {

    val businessEvents: List<BusinessEventDefinition>

    init {
        businessEvents = loadBusinessEvents()
    }

    fun loadBusinessEvents(): List<BusinessEventDefinition> {
        val loadedEvents = mutableListOf<BusinessEventDefinition>()

        for (path in eventFiles) {
            val substitutedJson = readAndSubstituteJson(path) ?: continue
            try {
                val event =
                    objectMapper.readValue(substitutedJson, BusinessEventDefinition::class.java)

                // Filter events by name if specified
                if (
                    eventNames.isNotEmpty() &&
                        eventNames.none { it.equals(event.name, ignoreCase = true) }
                ) {
                    continue
                }
                loadedEvents.add(event)
            } catch (ex: Exception) {
                logger.error("❌ Error parsing file $path: ${ex.message}")
                throw ex
            }
        }
        return loadedEvents
    }
}
