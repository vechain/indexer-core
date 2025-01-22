package org.vechain.indexer.event.model.abi

data class InputOutput(
    val internalType: String? = null,
    val name: String,
    val type: String,
    val indexed: Boolean = false,
    val components: List<InputOutput>? = null,
)
