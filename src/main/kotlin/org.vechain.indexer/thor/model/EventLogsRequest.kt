package org.vechain.indexer.thor.model

data class EventLogsRequest(
    val range: LogsRange?,
    val options: LogsOptions?,
    val criteriaSet: List<EventCriteria>?,
    val order: String?,
)
