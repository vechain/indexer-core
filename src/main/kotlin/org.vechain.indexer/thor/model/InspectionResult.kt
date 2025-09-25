package org.vechain.indexer.thor.model

data class InspectionResult(
    val data: String,
    val events: List<TxEvent>,
    val transfers: List<TxTransfer>,
    val reverted: Boolean,
    val vmError: String?,
)