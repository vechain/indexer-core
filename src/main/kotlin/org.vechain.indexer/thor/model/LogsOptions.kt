package org.vechain.indexer.thor.model

data class LogsOptions(
    val offset: Long?,
    val limit: Long?,
    val includeIndexes: Boolean = false,
)
