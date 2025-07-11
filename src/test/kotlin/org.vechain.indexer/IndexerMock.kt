package org.vechain.indexer

import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.client.DefaultThorClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier

class IndexerMock(
    private val mocker: IndexerResponseMocker,
    thorClientMock: ThorClient,
    abiManagerMock: AbiManager? = null,
    businessEventManagerMock: BusinessEventManager? = null,
    private val useMock: Boolean = false,
) :
    BlockIndexer(
        DefaultThorClient("notarealurl"),
        0L,
        abiManager = abiManagerMock,
        businessEventManager = businessEventManagerMock
    ) {
    override val thorClient: ThorClient = thorClientMock

    override val abiManager: AbiManager? = abiManagerMock

    override val businessEventManager: BusinessEventManager? = businessEventManagerMock

    override fun getLastSyncedBlock(): BlockIdentifier? = mocker.getLastSyncedBlock()

    override fun rollback(blockNumber: Long) {
        mocker.rollback(blockNumber)
    }

    override fun processBlock(block: Block) {
        mocker.processBlock(block)
    }

    public override fun processBlockGenericEvents(
        block: Block,
        criteria: FilterCriteria,
    ): List<IndexedEvent> =
        if (useMock) {
            mocker.processBlockGenericEvents(block, criteria)
        } else {
            super.processBlockGenericEvents(block, criteria)
        }

    public override fun processAllEvents(
        block: Block,
        criteria: FilterCriteria,
    ): List<IndexedEvent> = super.processAllEvents(block, criteria)

    public override fun processOnlyBusinessEvents(
        decodedEvents: List<IndexedEvent>,
        criteria: FilterCriteria,
    ): List<IndexedEvent> = super.processOnlyBusinessEvents(decodedEvents, criteria)
}

interface IndexerResponseMocker {
    fun getLastSyncedBlock(): BlockIdentifier?

    fun rollback(blockNumber: Long)

    fun processBlock(block: Block)

    fun processBlockGenericEvents(
        block: Block,
        criteria: FilterCriteria,
    ): List<IndexedEvent>
}
