package org.vechain.indexer.event.model.generic

data class IndexedEvent<T : EventParameters>(
    val id: String,
    val blockId: String,
    val blockNumber: Long,
    val blockTimestamp: Long,
    val txId: String,
    val origin: String?,
    val raw: RawEvent,
    val params: T,
    val address: String,
    val eventType: String,
    val clauseIndex: Long,
    val signature: String,
)
