package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.utils.JsonUtils
import java.io.File

class AbiManager {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    private val abis = mutableMapOf<String, List<AbiElement>>()

    /**
     * Loads ABIs from a specified directory.
     *
     * @param resourcePath The path to the ABI JSON files.
     */
    fun loadAbis(resourcePath: String) {
        val classLoader = Thread.currentThread().contextClassLoader
        val resourceDir =
            classLoader.getResource(resourcePath)
                ?: throw IllegalArgumentException("Invalid ABI directory: $resourcePath")

        val abiFiles =
            File(resourceDir.toURI()).listFiles { file -> file.extension == "json" }

        if (abiFiles.isNullOrEmpty()) {
            logger.warn("No ABI files found in directory: $resourcePath")
            return
        }

        abiFiles.forEach { file ->
            try {
                val abiList = JsonUtils.mapper.readValue(file.inputStream(), object : TypeReference<List<AbiElement>>() {})
                abiList.forEach { it.setSignature() }
                val abiName = file.nameWithoutExtension
                abis[abiName] = abiList
            } catch (e: Exception) {
                logger.error("Failed to load ABI file: ${file.name}", e)
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
     *
     * @param abiNames The list of ABI names to filter by.
     * @param eventNames The list of event names to filter by.
     * @return A list of matching `AbiElement` instances.
     */
    fun getEventsByNames(
        abiNames: List<String>,
        eventNames: List<String>,
    ): List<AbiElement> =
        abiNames.flatMap { abiName ->
            abis[abiName]?.filter { it.type == "event" && (eventNames.isEmpty() || it.name in eventNames) } ?: emptyList()
        }
}
