package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.event.model.abi.AbiElement

class AbiManager(private val abiFiles: List<String>) : ResourceManager() {

    private val abis = mutableMapOf<String, List<AbiElement>>()

    init {
        loadAbis().forEach { (fileName, abiEntries) ->
            abis[fileName] = abiEntries
            logger.info("Loaded ABI from $fileName with ${abiEntries.size} entries")
        }
    }

    fun loadAbis(): Map<String, List<AbiElement>> {
        val result = mutableMapOf<String, List<AbiElement>>()

        for (path in abiFiles) {
            val substitutedJson = readAndSubstituteJson(path) ?: continue
            try {
                val abiEntries: List<AbiElement> =
                    objectMapper.readValue(
                        substitutedJson,
                        object : TypeReference<List<AbiElement>>() {}
                    )
                val fileName = path.substringAfterLast("/").substringBeforeLast(".")
                result[fileName] = abiEntries
            } catch (ex: Exception) {
                logger.error("❌ Error parsing ABI file $path: ${ex.message}")
                throw ex
            }
        }
        return result
    }

    // ...existing code (getAbis, getEventsByNames)...
    fun getAbis(): Map<String, List<AbiElement>> = abis

    fun getEventsByNames(
        abiNames: List<String>,
        eventNames: List<String>,
    ): List<AbiElement> =
        abiNames
            .flatMap { abiName ->
                abis[abiName]?.filter {
                    it.type == "event" && (eventNames.isEmpty() || it.name in eventNames)
                } ?: emptyList()
            }
            .distinctBy { it.signature to it.inputs.count { input -> input.indexed } }
}
