package org.vechain.indexer.thor.model

data class InspectionResult(
    val data: String,
    val events: List<TxEvent>,
    val transfers: List<TxTransfer>,
    val gasUsed: Long,
    val reverted: Boolean,
    val vmError: String?,
)
