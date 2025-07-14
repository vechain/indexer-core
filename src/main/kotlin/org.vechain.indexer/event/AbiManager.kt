package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.event.model.abi.AbiElement

class AbiManager(private val abiFiles: List<String>, private val eventNames: List<String>) :
    ResourceManager() {

    val allAbis: List<AbiElement>
    val eventAbis: List<AbiElement>

    init {
        allAbis = loadAbis().values.flatten()
        eventAbis =
            allAbis
                .filter { it.type == "event" && (eventNames.isEmpty() || it.name in eventNames) }
                .distinctBy { it.signature to it.inputs.count { input -> input.indexed } }
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
}
