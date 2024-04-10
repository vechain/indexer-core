package org.vechain.indexer.thor.model

data class EventFilter(
    val range: Range,
    val options: Options,
    val criteriaSet: List<CriteriaSet>,
    val order: String,
)

data class Range(
    val unit: String?,
    val from: Long,
    val to: Long?,
)

data class Options(
    val offset: Int,
    val limit: Int,
)

data class CriteriaSet(
    val address: String?,
    val topic0: String?,
    val topic1: String?,
    val topic2: String?,
    val topic3: String?,
    val topic4: String?,
) {
    constructor(topic0: String) : this(null, topic0, null, null, null, null)
}
