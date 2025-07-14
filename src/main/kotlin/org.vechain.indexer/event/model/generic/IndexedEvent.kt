package org.vechain.indexer.event.model.generic

data class IndexedEvent(
    val id: String,
    val blockId: String,
    val blockNumber: Long,
    val blockTimestamp: Long,
    val txId: String,
    val origin: String?,
    val paid: String? = null,
    val gasUsed: Long? = null,
    val gasPayer: String? = null,
    val raw: RawEvent? = null,
    val params: AbiEventParameters,
    val address: String? = null,
    val eventType: String,
    val clauseIndex: Long,
    val signature: String? = null,
) {
    fun get(fieldName: String): Any? =
        when (fieldName) {
            "id" -> id
            "blockId" -> blockId
            "blockNumber" -> blockNumber
            "blockTimestamp" -> blockTimestamp
            "txId" -> txId
            "origin" -> origin
            "address" -> address
            "eventType" -> eventType
            "clauseIndex" -> clauseIndex
            "signature" -> signature
            "gasPayer" -> gasPayer
            else -> null
        }
}
