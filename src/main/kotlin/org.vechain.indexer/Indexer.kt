package org.vechain.indexer

import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult

/** The possible states an indexer can be in during its lifecycle. */
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

/**
 * Core interface for all indexers. An indexer processes blockchain data block-by-block, tracking
 * its current position and detecting chain reorganisations.
 *
 * Implementations include [BlockIndexer][org.vechain.indexer.BlockIndexer] for full block
 * processing and [LogsIndexer][org.vechain.indexer.LogsIndexer] for event-log-based processing.
 */
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

/** An [Indexer] that supports fast-syncing to quickly catch up to the finalized block. */
interface FastSyncableIndexer : Indexer {
    suspend fun fastSync()
}

/**
 * The result of processing a range of blockchain data. Produced by an indexer and consumed by an
 * [IndexerProcessor].
 *
 * There are two variants:
 * - [BlockResult]: Produced by a [BlockIndexer][org.vechain.indexer.BlockIndexer]. Contains the
 *   full block, decoded events, and any contract call inspection results.
 * - [LogResult]: Produced by a [LogsIndexer][org.vechain.indexer.LogsIndexer]. Contains only
 *   decoded events for a range of blocks.
 */
sealed class IndexingResult {
    abstract val status: Status

    /**
     * Result produced by a [BlockIndexer][org.vechain.indexer.BlockIndexer].
     *
     * Contains the full block data, decoded events, and contract call inspection results.
     */
    data class BlockResult(
        val block: Block,
        val events: List<IndexedEvent>,
        val callResults: List<InspectionResult>,
        override val status: Status
    ) : IndexingResult()

    /**
     * Result produced by a [LogsIndexer][org.vechain.indexer.LogsIndexer].
     *
     * Contains only decoded events for blocks up to [endBlock]. Does not include full block data or
     * call inspection results.
     */
    data class LogResult(
        val endBlock: Long,
        val events: List<IndexedEvent>,
        override val status: Status
    ) : IndexingResult()

    /** Returns the latest block number covered by this result. */
    fun latestBlockNumber(): Long =
        when (this) {
            is BlockResult -> block.number
            is LogResult -> endBlock
        }

    /** Returns all decoded events from this result. */
    fun events(): List<IndexedEvent> =
        when (this) {
            is BlockResult -> events
            is LogResult -> events
        }

    /**
     * Returns call inspection results. Only available on [BlockResult].
     *
     * @throws UnsupportedOperationException if called on a [LogResult]
     */
    fun callResults(): List<InspectionResult> =
        when (this) {
            is BlockResult -> callResults
            is LogResult ->
                throw UnsupportedOperationException("callResults() is not available for LogResult")
        }
}

/**
 * Processes [IndexingResult]s produced by an indexer. Implementations are responsible for
 * persisting indexed data and supporting rollbacks on chain reorganisations.
 */
interface IndexerProcessor {
    /** Returns the last block that was successfully processed, or null if none. */
    fun getLastSyncedBlock(): BlockIdentifier?

    /** Rolls back any persisted data at or after [blockNumber]. */
    fun rollback(blockNumber: Long)

    /** Processes a single [IndexingResult]. */
    suspend fun process(entry: IndexingResult)
}

/** Periodically cleans up old indexed data. Attached to an indexer via [Indexer.pruner]. */
interface Pruner {
    fun run(currentBlockNumber: Long)
}
