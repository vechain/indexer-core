package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.utils.FileScanner

object BusinessEventLoader {
    fun loadBusinessEvents(
        basePath: String,
        eventNames: List<String>,
        envParams: Map<String, String> = emptyMap()
    ): List<BusinessEventDefinition> {
        val eventFiles = FileScanner.findFiles(basePath = basePath, suffix = "json")
        val allBusinessEvents =
            JsonLoader.load(
                eventFiles,
                object : TypeReference<BusinessEventDefinition>() {},
                envParams
            )

        val matchedEvents =
            if (eventNames.isEmpty()) {
                allBusinessEvents.distinctBy { it.name.lowercase() }
            } else {
                val matched =
                    allBusinessEvents.filter { event ->
                        eventNames.any { it.equals(event.name, ignoreCase = true) }
                    }

                val unmatchedNames =
                    eventNames.filter { name ->
                        matched.none { it.name.equals(name, ignoreCase = true) }
                    }

                if (unmatchedNames.isNotEmpty()) {
                    throw IllegalArgumentException(
                        "The following event names were not found: $unmatchedNames"
                    )
                }

                matched.distinctBy { it.name.lowercase() }
            }

        return matchedEvents
    }
}
