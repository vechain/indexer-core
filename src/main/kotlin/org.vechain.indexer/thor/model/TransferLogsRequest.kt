package org.vechain.indexer.thor.model

data class TransferLogsRequest(
    val range: LogsRange?,
    val options: LogsOptions?,
    val criteriaSet: List<TransferCriteria>?,
    val order: String?,
)
