package org.vechain.indexer

import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Clause

abstract class PreSyncIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    syncLoggerInterval: Long,
    eventProcessor: CombinedEventProcessor?,
    inspectionClauses: List<Clause>?,
    pruner: Pruner?,
    prunerInterval: Long,
    dependsOn: Set<Indexer>
) :
    BlockIndexer(
        name,
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        eventProcessor,
        inspectionClauses,
        pruner,
        prunerInterval,
        dependsOn
    ) {

    /** Starts the indexer */
    override suspend fun start() {
        initialise()
        waitForDependencies()
        val finalizedBlock = thorClient.getFinalizedBlock().number

        if (currentBlockNumber < finalizedBlock) {
            sync(finalizedBlock)
            waitForDependencies()
        }

        logger.info("Fast sync complete, switching to block indexer")
        // Before running reset the previousBlock
        previousBlock = null
        run()
    }

    protected abstract suspend fun sync(toBlock: Long)
}
