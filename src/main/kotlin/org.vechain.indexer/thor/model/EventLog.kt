package org.vechain.indexer.thor.model

data class EventLog(
    val address: String,
    val topics: List<String>,
    val data: String,
    val meta: EventMeta
)
