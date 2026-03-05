package org.vechain.indexer.fixtures

import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.model.generic.RawEvent

object IndexedEventFixture {
    fun create(
        id: String = "default-id",
        blockId: String = "default-block-id",
        blockNumber: Long = 1L,
        blockTimestamp: Long = 1622547800L,
        txId: String = "default-tx-id",
        origin: String? = null,
        paid: String? = null,
        gasUsed: Long? = null,
        gasPayer: String? = null,
        raw: RawEvent? = null,
        params: AbiEventParameters = AbiEventParameters(emptyMap(), "default"),
        address: String? = null,
        eventType: String = "default-event-type",
        clauseIndex: Long = 0L,
        signature: String? = null
    ): IndexedEvent {
        return IndexedEvent(
            id,
            blockId,
            blockNumber,
            blockTimestamp,
            txId,
            origin,
            paid,
            gasUsed,
            gasPayer,
            raw,
            params,
            address,
            eventType,
            clauseIndex,
            signature
        )
    }
}
