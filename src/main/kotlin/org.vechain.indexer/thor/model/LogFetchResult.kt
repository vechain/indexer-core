package org.vechain.indexer.thor.model

data class LogFetchResult(
    val eventLogs: List<EventLog>,
    val transferLogs: List<TransferLog>,
)
