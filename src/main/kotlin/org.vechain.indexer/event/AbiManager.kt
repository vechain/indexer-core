package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.utils.JsonUtils
import java.io.InputStream

class AbiManager {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val abis = mutableMapOf<String, List<AbiElement>>()

    /**
     * Loads ABIs from a map of input streams.
     * @param abiFiles A map where the key is the ABI name, and the value is its InputStream.
     */
    fun loadAbis(abiFiles: Map<String, InputStream>) {
        abiFiles.forEach { (abiName, inputStream) ->
            try {
                inputStream.use { stream ->
                    val abiList = JsonUtils.mapper.readValue(stream, object : TypeReference<List<AbiElement>>() {})
                    abiList.forEach { it.setSignature() }
                    abis[abiName] = abiList
                    logger.info("Loaded ABI: $abiName")
                }
            } catch (e: Exception) {
                logger.error("Failed to parse ABI file: $abiName", e)
            }
        }
    }

    /**
     * Retrieves all loaded ABIs.
     */
    fun getAbis(): Map<String, List<AbiElement>> = abis

    /**
     * Fetches ABIs for a given list of names and event types.
     */
    fun getEventsByNames(
        abiNames: List<String>,
        eventNames: List<String>,
    ): List<AbiElement> =
        abiNames
            .flatMap { abiName ->
                abis[abiName]?.filter { it.type == "event" && (eventNames.isEmpty() || it.name in eventNames) } ?: emptyList()
            }.distinctBy { it.signature to it.inputs.count { input -> input.indexed } }
}
