package org.vechain.indexer

import kotlinx.coroutines.CoroutineScope
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.InspectionResult

/** The possible states the indexer can be */
enum class Status {
    /** Indexer is processing blocks */
    RUNNING,

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

    val dependantIndexers: Set<Indexer>
}

sealed class IndexingResult {
    /** Represents a full block of data including all events and call results */
    data class Normal(
        val block: Block,
        val events: List<IndexedEvent>,
        val callResults: List<InspectionResult>
    ) : IndexingResult()

    /**
     * Represents a batch of events without the full block data. This is used when indexing via
     * smart contract events using a [LogsIndexer]
     */
    data class EventsOnly(val endBlock: Long, val events: List<IndexedEvent>) : IndexingResult()

    fun latestBlockNumber(): Long =
        when (this) {
            is Normal -> block.number
            is EventsOnly -> endBlock
        }

    fun events(): List<IndexedEvent> =
        when (this) {
            is Normal -> events
            is EventsOnly -> events
        }

    fun callResults(): List<InspectionResult> =
        when (this) {
            is Normal -> callResults
            is EventsOnly -> emptyList()
        }
}

interface IndexerProcessor {
    fun getLastSyncedBlock(): BlockIdentifier?

    fun rollback(blockNumber: Long)

    fun process(entry: IndexingResult)
}

interface Pruner {
    fun run(currentBlockNumber: Long)
}
