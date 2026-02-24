package org.vechain.indexer

import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult

/** The possible states the indexer can be */
enum class Status {
    /** Indexer has not been initialised */
    NOT_INITIALISED,

    /** Indexer has been initialised but not started */
    INITIALISED,

    /** Indexer is performing a fast sync to catch up to the best block */
    FAST_SYNCING,

    /** Indexer is syncing */
    SYNCING,

    /** Indexer is processing blocks */
    FULLY_SYNCED,

    /** Indexer is pruning old data. Records will not be processed while in this state */
    PRUNING,

    /** Indexer has been shut down and cannot be restarted */
    SHUT_DOWN,
}

interface Indexer : IndexerProcessor {
    // The name of the indexer
    val name: String

    // The current status of the indexer
    fun getStatus(): Status

    // The current block number being processed
    fun getCurrentBlockNumber(): Long

    // The previous block processed by this indexer. Used to detect re-orgs.
    fun getPreviousBlock(): BlockIdentifier?

    // Initialise the indexer
    fun initialise()

    // Process a block. The onReset callback should be called if the indexer needs to reset its
    // state
    suspend fun processBlock(block: Block)

    // Process a block with pre-computed inspection results (for pipelining)
    suspend fun processBlock(block: Block, inspectionResults: List<InspectionResult>) {
        // Default implementation ignores pre-computed results and calls the regular method
        processBlock(block)
    }

    // Optional pruner that can be run periodically to clean up old data
    val pruner: Pruner?

    // Optional parent indexers that this indexer depends on.
    val dependsOn: Indexer?

    // Optional inspection clauses for contract calls during block processing
    fun getInspectionClauses(): List<Clause>? = null

    fun shutDown()
}

interface FastSyncable {
    suspend fun fastSync()
}

sealed class IndexingResult {
    abstract val status: Status

    /** Represents a full block of data including all events and call results */
    data class Normal(
        val block: Block,
        val events: List<IndexedEvent>,
        val callResults: List<InspectionResult>,
        override val status: Status
    ) : IndexingResult()

    /**
     * Represents a batch of events without the full block data. This is used when indexing via
     * smart contract events using a [LogsIndexer]
     */
    data class EventsOnly(
        val endBlock: Long,
        val events: List<IndexedEvent>,
        override val status: Status
    ) : IndexingResult()

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

    suspend fun process(entry: IndexingResult)
}

interface Pruner {
    fun run(currentBlockNumber: Long)
}
