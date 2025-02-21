package org.vechain.indexer

import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.EventLog
import java.util.*

class LogIndexerMock(
    private val mocker: LogsIndexerResponseMocker,
    blockBatchSize: Long,
    logFetchLimit: Long,
    thorClientMock: ThorClient,
    abiManagerMock: AbiManager?,
    businessEventManagerMock: BusinessEventManager?,
) : LogsIndexer(
        thorClient = thorClientMock,
        startBlock = 0,
        syncLoggerInterval = 1000,
        blockBatchSize = blockBatchSize,
        logFetchLimit = logFetchLimit,
        abiManager = abiManagerMock,
        businessEventManager = businessEventManagerMock,
    ) {
    override fun processLogs(logs: List<EventLog>) {
        mocker.processLogs(logs)
    }

    override fun getLastSyncedBlock(): BlockIdentifier? = mocker.getLastSyncedBlock()

    override fun processBlock(block: Block) {
        mocker.processBlock(block)
    }

    override fun rollback(blockNumber: Long) {}

    public override fun processBlockGenericEvents(
        logs: List<EventLog>,
        criteria: FilterCriteria,
    ): List<Pair<IndexedEvent, GenericEventParameters>> = super.processBlockGenericEvents(logs, criteria)

    public override fun processAllEvents(
        logs: List<EventLog>,
        criteria: FilterCriteria,
    ): List<Pair<IndexedEvent, GenericEventParameters>> = super.processAllEvents(logs, criteria)
}

interface LogsIndexerResponseMocker {
    fun getLastSyncedBlock(): BlockIdentifier?

    fun rollback(blockNumber: Long)

    fun processLogs(logs: List<EventLog>)

    fun processBlock(block: Block)
}
