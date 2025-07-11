package org.vechain.indexer

import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.enums.LogType
import org.vechain.indexer.thor.model.*

class LogIndexerMock(
    private val mocker: LogsIndexerResponseMocker,
    logsType: Set<LogType> = setOf(LogType.EVENT),
    blockBatchSize: Long,
    logFetchLimit: Long,
    thorClientMock: ThorClient,
    abiManagerMock: AbiManager?,
    businessEventManagerMock: BusinessEventManager?,
    eventCriteriaSet: List<EventCriteria>? = null,
    transferCriteriaSet: List<TransferCriteria>? = null,
    private val mock: Boolean = true,
) :
    LogsIndexer(
        thorClient = thorClientMock,
        startBlock = 0,
        syncLoggerInterval = 1000,
        logsType = logsType,
        blockBatchSize = blockBatchSize,
        logFetchLimit = logFetchLimit,
        abiManager = abiManagerMock,
        businessEventManager = businessEventManagerMock,
        eventCriteriaSet = eventCriteriaSet,
        transferCriteriaSet = transferCriteriaSet,
    ) {
    override fun processLogs(
        events: List<EventLog>,
        transfers: List<TransferLog>,
    ) {
        mocker.processLogs(events, transfers)
    }

    override fun getLastSyncedBlock(): BlockIdentifier? = mocker.getLastSyncedBlock()

    override fun processBlock(block: Block) {
        if (mock) {
            mocker.processBlock(block)
        } else {
            super.processBlock(block)
        }
    }

    override fun rollback(blockNumber: Long) {}

    public override fun processBlockGenericEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>,
        criteria: FilterCriteria,
    ): List<IndexedEvent> = super.processBlockGenericEvents(eventLogs, transferLogs, criteria)

    public override fun processAllEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>,
        criteria: FilterCriteria,
    ): List<IndexedEvent> = super.processAllEvents(eventLogs, transferLogs, criteria)

    public override fun processOnlyBusinessEvents(
        decodedEvents: List<IndexedEvent>,
        criteria: FilterCriteria,
    ): List<IndexedEvent> = super.processOnlyBusinessEvents(decodedEvents, criteria)
}

interface LogsIndexerResponseMocker {
    fun getLastSyncedBlock(): BlockIdentifier?

    fun rollback(blockNumber: Long)

    fun processLogs(
        events: List<EventLog>,
        transfers: List<TransferLog>,
    )

    fun processBlock(block: Block)
}
