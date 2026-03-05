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

        val allAbiElements =
            JsonLoader.loadAndFlatten(
                abiFiles,
                object : TypeReference<List<AbiElement>>() {},
                substitutionParams
            )

        val matchedEvents =
            if (names.isEmpty()) {
                allAbiElements
                    .filter { abi ->
                        (typeFilter == null || abi.type == typeFilter) && abi.name != null
                    }
                    .distinctBy { generateUniqueId(it) }
            } else {
                val matched =
                    allAbiElements.filter { abi ->
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

                matched.distinctBy { generateUniqueId(it) }
            }

        return matchedEvents
    }

    /**
     * Generate an ID to uniquely identify an ABI element.
     *
     * Given the abi: `json { "anonymous": false, "inputs":
     * [ { "indexed": true, "internalType": "address", "name": "from", "type": "address" }, { "indexed": false, "internalType": "address", "name": "to", "type": "address" }, { "indexed": true, "internalType": "uint256", "name": "value", "type": "uint256" } ],
     * "name": "Transfer", "type": "event" } '
     *
     * the generated ID will be: `Transfer(indexed address,indexed address,uint256)`
     */
    fun generateUniqueId(abi: AbiElement): String {
        val name = abi.name ?: throw IllegalArgumentException("ABI element must have a name")

        val inputSignature =
            abi.inputs.joinToString(",") { input ->
                val indexedPrefix = if (input.indexed) "indexed " else ""
                "$indexedPrefix${input.type}"
            }

        return "$name($inputSignature)"
    }
}
