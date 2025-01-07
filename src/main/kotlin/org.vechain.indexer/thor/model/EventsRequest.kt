package org.vechain.indexer.thor.model

data class EventCriteria(
    val address: String? = null,
    val topic0: String? = null,
    val topic1: String? = null,
    val topic2: String? = null,
    val topic3: String? = null,
    val topic4: String? = null,
)

data class EventLogsRequest(
    val range: EventRange?,
    val options: EventOptions?,
    val criteriaSet: List<EventCriteria>?,
    val order: String?
)

data class EventRange(
    val unit: String?,
    val from: Long?,
    val to: Long?,
)


data class EventOptions(
    val offset: Long?,
    val limit: Long?,
)
