package org.vechain.indexer.event.model.abi

import org.vechain.indexer.event.utils.EventUtils

/** Represents an ABI element with metadata and utility to generate its signature. */
data class AbiElement(
    val name: String?,
    val type: String,
    val anonymous: Boolean = false,
    val stateMutability: String? = null,
    val inputs: List<InputOutput> = emptyList(),
    val outputs: List<InputOutput> = emptyList(),
    var signature: String? = null,
) {
    /**
     * Sets the signature for the ABI element based on its type and inputs. For `tuple` types, it
     * recursively resolves component types.
     *
     * @return The generated signature as a string.
     */
    fun setSignature(): String {
        val params =
            inputs.joinToString(",") { input ->
                if (input.type == "tuple") {
                    // Handle tuple type with nested components
                    val componentTypes = input.components?.joinToString(",") { it.type } ?: ""
                    "($componentTypes)"
                } else {
                    input.type
                }
            }

        val signatureString = "$name($params)"
        signature =
            when (type) {
                "event",
                "function" -> EventUtils.getEventSignature(signatureString)
                else -> null
            }
        return signature ?: ""
    }
}
