package org.vechain.indexer.event.utils

import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.types.Types
import org.vechain.indexer.utils.DataUtils

object FunctionReturnDecoder {
    fun decode(
        rawInput: String,
        outputs: List<InputOutput>,
    ): Map<String, Any?> {
        val results = mutableMapOf<String, Any?>()

        // Remove selector (first 4 bytes = 8 hex chars), if present
        val inputData =
            DataUtils.removePrefix(rawInput).let {
                if (it.length % 64 == 8) it.substring(8) else it
            }

        val fullData = inputData
        println("Decoding input data: $inputData")
        var offset = 0

        outputs.forEach { output ->
            val typeDecoder =
                Types.values().firstOrNull { it.isType(output.type) }
                    ?: throw IllegalArgumentException("Unsupported type: ${output.type}")

            val decoded =
                typeDecoder.decode(
                    encoded = "0x" + inputData.substring(offset, offset + 64),
                    clazz = Any::class.java,
                    name = output.type,
                    fullData = fullData,
                    startPosition = offset,
                    components = output.components,
                )

            results[output.name] = decoded.actualValue
            offset += 64
        }

        return results
    }
}
