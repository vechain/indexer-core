package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.utils.FileScanner

object AbiLoader {

    fun loadEvents(
        basePath: String,
        eventNames: List<String>,
        substitutionParams: Map<String, String> = emptyMap()
    ): List<AbiElement> =
        load(
            basePath = basePath,
            names = eventNames,
            typeFilter = "event",
            substitutionParams = substitutionParams
        )

    fun loadFunctions(
        basePath: String,
        functionNames: List<String>,
        substitutionParams: Map<String, String> = emptyMap()
    ): List<AbiElement> =
        load(
            basePath = basePath,
            names = functionNames,
            typeFilter = "function",
            substitutionParams = substitutionParams
        )

    fun load(
        basePath: String,
        names: List<String>,
        typeFilter: String? = null,
        substitutionParams: Map<String, String> = emptyMap()
    ): List<AbiElement> {

        val abiFiles = FileScanner.findFiles(basePath = basePath, suffix = "json")

        val allAbisElements =
            JsonLoader.loadAndFlatten(
                abiFiles,
                object : TypeReference<List<AbiElement>>() {},
                substitutionParams
            )

        val matchedEvents =
            if (names.isEmpty()) {
                allAbisElements
                    .filter { abi ->
                        (typeFilter == null || abi.type == typeFilter) && abi.name != null
                    }
                    .distinctBy { it.name?.lowercase() }
            } else {
                val matched =
                    allAbisElements.filter { abi ->
                        (typeFilter == null || abi.type == typeFilter) &&
                            abi.name != null &&
                            names.any { it.equals(abi.name, ignoreCase = true) }
                    }

                val unmatchedNames =
                    names.filter { name ->
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
