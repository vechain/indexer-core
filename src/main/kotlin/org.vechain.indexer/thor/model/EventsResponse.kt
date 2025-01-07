package org.vechain.indexer.thor.model

data class EventLog(
    val address: String,
    val topics: List<String>,
    val data: String,
    val meta: EventMeta
)

data class EventMeta(
    val blockID: String,
    val blockNumber: Long,
    val blockTimestamp: Long,
    val txID: String,
    val txOrigin: String,
    val clauseIndex: Int,
)
