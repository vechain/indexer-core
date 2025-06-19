package org.vechain.indexer.event.model.business

data class Event(
    val name: String = "",
    val alias: String = "",
    val conditions: List<Condition> = emptyList(),
)
