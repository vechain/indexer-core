package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.springframework.core.io.Resource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.vechain.indexer.event.model.AbiElement
import org.vechain.indexer.utils.JsonUtils

class AbiManager {
    private val abis = mutableMapOf<String, List<AbiElement>>()

    /**
     * Loads ABIs from the specified resource path into memory.
     *
     * @param resourcePath The path to the ABI JSON files.
     */
    fun loadAbis(resourcePath: String) {
        val resolver = PathMatchingResourcePatternResolver()
        val resources: Array<Resource> = resolver.getResources(resourcePath)
        resources.forEach { resource ->
            try {
                val abiList = JsonUtils.mapper.readValue(resource.inputStream, object : TypeReference<List<AbiElement>>() {})
                abiList.forEach { it.setSignature() } // Set signatures
                val abiName = resource.filename?.removeSuffix(".json") ?: "unknown" // Strip .json extension
                abis[abiName] = abiList
            } catch (e: Exception) {
                println("Failed to load ABI from ${resource.filename}: ${e.message}")
            }
        }
    }

    /**
     * Retrieves all loaded ABIs.
     *
     * @return A map where the key is the ABI name, and the value is the list of ABI elements.
     */
    fun getAbis(): Map<String, List<AbiElement>> = abis

    /**
     * Fetches ABIs for a given list of names and event types.
     */
    fun getEventsByNames(
        abiNames: List<String>,
        eventNames: List<String>,
    ): List<AbiElement> =
        abiNames.flatMap { abiName ->
            abis[abiName]?.filter { it.type == "event" && it.name in eventNames } ?: emptyList()
        }
}
