package org.vechain.indexer.event.model.abi

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.vechain.indexer.event.utils.EventUtils

@JsonIgnoreProperties(ignoreUnknown = true)
data class AbiElement(
    val name: String?,
    val type: String = "",
    val anonymous: Boolean = false,
    val stateMutability: String? = null,
    val payable: Boolean? = null,
    val constant: Boolean? = null,
    val inputs: List<InputOutput> = emptyList(),
    val outputs: List<InputOutput> = emptyList(),
    var signature: String? = null,
) {
    init {
        signature = generateSignature()
    }

    private fun generateSignature(): String? {
        val params =
            inputs.joinToString(",") { input ->
                if (input.type == "tuple") {
                    val componentTypes = input.components?.joinToString(",") { it.type } ?: ""
                    "($componentTypes)"
                } else {
                    input.type
                }
            }

        val signatureString = "$name($params)"
        return when (type) {
            "event",
            "function" -> EventUtils.getEventSignature(signatureString)
            else -> null
        }
    }
}
