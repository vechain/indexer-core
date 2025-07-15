package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.event.model.abi.AbiElement

object AbiLoader {
    fun loadEvents(
        abiFiles: List<String>,
        eventNames: List<String>,
        substitutionParams: Map<String, String> = emptyMap()
    ): List<AbiElement> {
        val allAbisElements =
            JsonLoader.loadAndFlatten(
                abiFiles,
                object : TypeReference<List<AbiElement>>() {},
                substitutionParams
            )

        val matchedEvents =
            if (eventNames.isEmpty()) {
                allAbisElements
                    .filter { it.type == "event" && it.name != null }
                    .distinctBy { it.name?.lowercase() }
            } else {
                val matched =
                    allAbisElements.filter { abi ->
                        abi.type == "event" &&
                            abi.name != null &&
                            eventNames.any { it.equals(abi.name, ignoreCase = true) }
                    }

                val unmatchedNames =
                    eventNames.filter { name ->
                        matched.none { it.name?.equals(name, ignoreCase = true) == true }
                    }

                if (unmatchedNames.isNotEmpty()) {
                    throw IllegalArgumentException(
                        "The following event names were not found in ABI: $unmatchedNames"
                    )
                }

                matched.distinctBy { it.name?.lowercase() }
            }

        return matchedEvents
    }
}
