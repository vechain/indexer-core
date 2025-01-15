package org.vechain.indexer.event.model.abi

import org.vechain.indexer.event.utils.EventUtils

data class AbiElement(
    val name: String?,
    val type: String, // e.g., "constructor", "event", "function", "error"
    val anonymous: Boolean = false,
    val stateMutability: String? = null,
    val inputs: List<InputOutput> = emptyList(),
    val outputs: List<InputOutput> = emptyList(),
    var signature: String? = null,
) {
    fun setSignature(): String {
        signature =
            when (type) {
                "event", "function" -> EventUtils.getEventSignature("$name(${inputs.joinToString(",") { it.type }})")
                else -> null // Constructors and errors don't need a signature
            }
        return signature ?: ""
    }
}
