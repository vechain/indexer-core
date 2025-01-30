package org.vechain.indexer.event.types

data class DecodedValue<T>(
    val stringRepresentation: String,
    val type: Class<T>,
    val actualValue: T,
    val name: String,
)
