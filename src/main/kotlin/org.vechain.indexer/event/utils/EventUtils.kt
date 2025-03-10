package org.vechain.indexer.event.utils

import org.bouncycastle.jcajce.provider.digest.Keccak
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.types.Types
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TxEvent
import org.vechain.indexer.utils.DataUtils

object EventUtils {
    /**
     * Computes the Keccak256 hash of an event's canonical signature.
     *
     * @param canonicalName The canonical signature of the event (e.g., `Transfer(address,address,uint256)`).
     *                      This should include the event name and parameter types in the correct order.
     * @return A 64-character hexadecimal string representing the event's Keccak256 hash without the `0x` prefix.
     */
    fun getEventSignature(canonicalName: String): String {
        require(canonicalName.isNotBlank()) { "Canonical name must not be blank." }
        val keccak = Keccak.Digest256()
        val hash = keccak.digest(canonicalName.toByteArray(Charsets.UTF_8))
        return hash.joinToString("") { "%02x".format(it) }
    }

    /**
     * Decodes an event based on its ABI element and topics/data.
     *
     * @param event The blockchain event to decode.
     * @param abiElement The ABI element defining the structure of the event.
     * @return A GenericEventParameters object containing decoded values and event type.
     */
    fun decodeEvent(
        event: TxEvent,
        abiElement: AbiElement,
    ): GenericEventParameters {
        val decodedParameters = mutableMapOf<String, Any?>()
        val inputs = abiElement.inputs

        // Validate inputs and topics
        val indexedInputs = inputs.filter { it.indexed }
        if (indexedInputs.size + 1 != event.topics.size) {
            throw IllegalArgumentException(
                "Mismatch between ABI indexed inputs and event topics",
            )
        }

        // Decode parameters
        var topicIndex = 1 // Skip the event signature
        var dataOffset = 0 // Offset for non-indexed parameters in the `data` field

        for (input in inputs) {
            val decodedValue =
                if (input.indexed) {
                    decodeType(event.topics[topicIndex++], input.type)
                } else {
                    val segment = extractDataSegment(event.data, dataOffset++)
                    decodeType(
                        segment,
                        input.type,
                        fullData = event.data,
                        startPosition = (dataOffset - 1) * 64,
                        input.components,
                    )
                }
            decodedParameters[input.name] = decodedValue
        }

        return GenericEventParameters(
            returnValues =
                decodedParameters
                    .filterValues { it != null }
                    .mapValues { it.value as Any },
            // Filter out nulls and cast to Any
            eventType = abiElement.name ?: "Unknown",
        )
    }

    /**
     * Decodes a value from a topic or data segment using Web3j's type system.
     *
     * @param hexValue The hex string to decode.
     * @param solidityType The Solidity type as a string (e.g., "uint256", "address").
     * @return The decoded value as an object.
     */
    private fun decodeType(
        hexValue: String,
        solidityType: String,
        fullData: String? = null, // Pass full data for dynamic types
        startPosition: Int = 0, // Start position in the data field
        components: List<InputOutput>? = null, // Components for tuple types
    ): Any =
        Types
            .values()
            .firstOrNull {
                it.isType(
                    solidityType,
                )
            }?.decode(hexValue, Any::class.java, solidityType, fullData, startPosition, components)
            ?.actualValue
            ?: throw IllegalArgumentException("Unsupported Solidity type: $solidityType")

    /**
     * Extracts a 32-byte segment of the data field based on the parameter index.
     *
     * @param data The data field of the event.
     * @param index The index of the parameter in the data field.
     * @return The extracted data segment.
     */
    private fun extractDataSegment(
        data: String,
        index: Int,
    ): String {
        val offset = index * 64
        val cleanData = DataUtils.removePrefix(data)
        if (offset + 64 > cleanData.length) {
            throw IllegalArgumentException("Data segment out of bounds for index $index")
        }
        return DataUtils.addPrefix(cleanData.substring(offset, offset + 64))
    }

    /**
     * Finds the most suitable ABI element for decoding a given event log.
     *
     * @param topics The list of topics from the event log.
     * @param configuredEvents The list of ABI elements to search from.
     * @return The best matching ABI element or null if no match is found.
     */
    fun findMatchingAbi(
        topics: List<String>,
        configuredEvents: List<AbiElement>,
    ): AbiElement? {
        if (topics.isEmpty()) return null // Avoids unnecessary processing if topics list is empty

        val eventSignature = DataUtils.removePrefix(topics[0])

        return configuredEvents.firstOrNull { abi ->
            abi.signature == eventSignature && abi.inputs.count { it.indexed } + 1 == topics.size
        }
    }

    /**
     * Validates if an event matches the provided ABIs and contract address filter.
     */
    fun isEventValid(
        event: TxEvent,
        configuredEvents: List<AbiElement>,
        contractAddresses: List<String>,
    ): Boolean {
        val matchesAbi =
            configuredEvents.any {
                it.signature == DataUtils.removePrefix(event.topics[0])
            }
        val matchesContract = contractAddresses.isEmpty() || contractAddresses.any { it.equals(event.address, ignoreCase = true) }

        return event.topics.isNotEmpty() && matchesAbi && matchesContract
    }
}
