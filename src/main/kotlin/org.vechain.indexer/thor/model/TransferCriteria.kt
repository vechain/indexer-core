package org.vechain.indexer.thor.model

data class TransferCriteria(
    val txOrigin: String? = null,
    val sender: String? = null,
    val recipient: String? = null,
)
