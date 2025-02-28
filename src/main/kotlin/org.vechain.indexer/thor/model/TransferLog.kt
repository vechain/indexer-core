package org.vechain.indexer.thor.model

data class TransferLog(
    val sender: String,
    val recipient: String,
    val amount: String,
    val meta: EventMeta,
)
