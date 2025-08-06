package org.vechain.indexer.event

import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog

interface EventProcessor {
    fun processEvents(block: Block): List<IndexedEvent>

    fun processEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>
    ): List<IndexedEvent>
}
