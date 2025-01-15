package org.vechain.indexer.event.utils

import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.thor.model.TxEvent
import org.web3j.abi.EventEncoder
import org.web3j.abi.TypeDecoder
import org.web3j.abi.datatypes.AbiTypes
import org.web3j.utils.Numeric

object EventUtils {
    /**
     * Computes the Keccak256 hash of an event's canonical signature.
     *
     * @param canonicalName The canonical signature of the event (e.g., `Transfer(address,address,uint256)`).
     * @return A 64-character hexadecimal string representing the event's Keccak256 hash.
     */
    fun getEventSignature(canonicalName: String): String = Numeric.cleanHexPrefix(EventEncoder.buildEventSignature(canonicalName))

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
        val decodedParameters = mutableMapOf<String, Any>()
        val inputs = abiElement.inputs

        // Validate inputs and topics
        val indexedInputs = inputs.filter { it.indexed }
        if (indexedInputs.size + 1 != event.topics.size) {
            throw IllegalArgumentException(
                "Mismatch between ABI indexed inputs (${indexedInputs.size}) and event topics (${event.topics.size})",
            )
        }

        // Decode parameters
        var topicIndex = 1 // Skip the event signature
        var dataOffset = 0 // Offset for non-indexed parameters in the `data` field

        for (input in inputs) {
            val decodedValue =
                when {
                    input.indexed -> {
                        if (topicIndex >= event.topics.size) {
                            throw IllegalArgumentException("Missing topic for indexed parameter: ${input.name}")
                        }
                        decodeType(event.topics[topicIndex++], input.type)
                    }
                    else -> {
                        val segment = extractDataSegment(event.data, dataOffset++)
                        decodeType(segment, input.type)
                    }
                }
            decodedParameters[input.name] = decodedValue
        }

        return GenericEventParameters(decodedParameters, abiElement.name ?: "Unknown")
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
    ): Any {
        val typeClass =
            AbiTypes.getType(solidityType)
                ?: throw IllegalArgumentException("Unsupported Solidity type: $solidityType")

        return try {
            TypeDecoder.decode(hexValue, 0, typeClass).value
        } catch (e: Exception) {
            throw IllegalArgumentException("Error decoding value for type $solidityType: $hexValue", e)
        }
    }

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
        val cleanData = Numeric.cleanHexPrefix(data)
        if (offset + 64 > cleanData.length) {
            throw IllegalArgumentException("Data segment out of bounds for index $index")
        }
        return Numeric.prependHexPrefix(cleanData.substring(offset, offset + 64))
    }
}
