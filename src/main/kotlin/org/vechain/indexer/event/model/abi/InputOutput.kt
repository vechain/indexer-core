package org.vechain.indexer.event.model.abi

data class InputOutput(
    val internalType: String,
    val name: String,
    val type: String,
    val indexed: Boolean,
)
