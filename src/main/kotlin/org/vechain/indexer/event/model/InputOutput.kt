package org.vechain.indexer.event.model

data class InputOutput(
    val internalType: String,
    val name: String,
    val type: String,
    val indexed: Boolean,
)