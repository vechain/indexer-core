package org.vechain.indexer

import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier

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
}

interface Indexer {
    var status: Status

    fun startInCoroutine(iterations: Long? = null)

    suspend fun start(iterations: Long? = null)

    fun getLastSyncedBlock(): BlockIdentifier?

    fun rollback(blockNumber: Long)

    fun processBlock(block: Block)
}
