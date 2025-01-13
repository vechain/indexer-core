package org.vechain.indexer.event.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.AbiElement
import org.vechain.indexer.event.model.GenericEventParameters
import org.vechain.indexer.event.types.TypesUtils
import org.vechain.indexer.thor.model.TxEvent
import org.web3j.abi.EventEncoder

object EventUtils {
    val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /**
     * Computes the Keccak256 hash of an event's canonical signature, used as a unique identifier
     * in Ethereum and VeChain blockchains. Example: `Transfer(address,address,uint256)`.
     *
     * @param canonicalName The canonical signature of the event.
     * @return A 64-character hexadecimal string representing the event's Keccak256 hash.
     */
    fun getEventSignature(canonicalName: String): String = HexUtils.removePrefix(EventEncoder.buildEventSignature(canonicalName))

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

        // Validate the number of topics for indexed parameters
        val indexedInputs = inputs.filter { it.indexed }
        if (indexedInputs.size + 1 != event.topics.size) {
            logger.info("Decoding event: ${abiElement.name}")
            logger.info("Event topics: ${event.topics}")
            logger.info("Event data: ${event.data}")
            logger.info("AbiElement inputs: $inputs")
            throw IllegalArgumentException("Mismatch between ABI indexed inputs and event topics count")
        }

        var topicIndex = 1 // Skip the event signature
        var dataOffset = 0 // Offset for non-indexed parameters in the `data` field

        inputs.forEach { input ->
            val value =
                if (input.indexed) {
                    if (topicIndex >= event.topics.size) {
                        throw IllegalArgumentException("Missing topic for indexed parameter: ${input.name}")
                    }
                    val topic = event.topics[topicIndex++]
                    TypesUtils.getType(input.type).decode(topic, TypesUtils.getType(input.type).getClaas(), input.name)
                } else {
                    val segment = extractDataSegment(event.data, dataOffset++)
                    TypesUtils.getType(input.type).decode(segment, TypesUtils.getType(input.type).getClaas(), input.name)
                }

            decodedParameters[input.name] = value
        }

        return GenericEventParameters(decodedParameters, abiElement.name ?: "Unknown")
    }

    /**
     * Extracts a segment of the data field based on the parameter index.
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
        val rawSegment = HexUtils.removePrefix(data).substring(offset, offset + 64)
        return HexUtils.addPrefix(rawSegment)
    }
}
