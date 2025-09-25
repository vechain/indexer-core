package org.vechain.indexer

import kotlinx.coroutines.CoroutineScope
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.InspectionResult

/** The possible states the indexer can be */
enum class Status {
    /** Indexer is processing blocks */
    SYNCING,

    /** Indexing is up to date with the latest on-chain block */
    FULLY_SYNCED,

    /** A chain re-organization has been detected during processing */
    REORG,

    /** Indexer encountered an unknown exception during processing */
    ERROR,

    /** Indexer is pruning old data. Records will not be processed while in this state */
    PRUNING,
}

interface Indexer : IndexerProcessor {
    val name: String

    var status: Status

    val pruner: Pruner?

    fun startInCoroutine(scope: CoroutineScope)

    suspend fun start()
}

sealed class BlockEvent {
    data class Normal(val block: Block, val events: List<IndexedEvent>) : BlockEvent()
    data class EventsOnly(val events: List<IndexedEvent>) : BlockEvent()
    data class WithCallData(val block: Block, val callResults: List<InspectionResult>) : BlockEvent()
}

interface IndexerProcessor {
    fun getLastSyncedBlock(): BlockIdentifier?

    fun rollback(blockNumber: Long)

    fun process(event: BlockEvent)
}

interface Pruner {
    fun run(currentBlockNumber: Long)
}
