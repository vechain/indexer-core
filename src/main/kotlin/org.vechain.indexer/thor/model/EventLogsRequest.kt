package org.vechain.indexer.thor.model

data class EventLogsRequest(
    val range: EventRange?,
    val options: EventOptions?,
    val criteriaSet: List<EventCriteria>?,
    val order: String?,
)
