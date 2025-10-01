package org.vechain.indexer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.vechain.indexer.thor.client.ThorClient

open class IndexerCoordinator(
    private val thorClient: ThorClient,
    private val batchSize: Int = 1,
) {
    companion object {
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: Collection<BlockIndexer>,
            blockBatchSize: Int = 1,
            maxBlocks: Long? = null,
        ): Job {
            if (indexers.isEmpty()) {
                return scope.launch {}
            }

            val blockIndexers = indexers.toList()
            val indexerCoordinator = IndexerCoordinator(thorClient, batchSize = blockBatchSize)

            return scope.launch {
                indexerCoordinator.run(
                    indexers = blockIndexers,
                    maxBlocks = maxBlocks,
                )
            }
        }
    }

    suspend fun run(
        indexers: List<BlockIndexer>,
        maxBlocks: Long? = null,
    ) = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        val executionOrder = CoordinatorSupport.topologicalOrder(indexers)
        CoordinatorSupport.prepareIndexers(indexers)

        var nextBlockNumber = indexers.minOf { it.currentBlockNumber }
        var blocksProcessed = 0L

        val stream =
            PrefetchingBlockStream(
                scope = this,
                batchSize = batchSize,
                currentBlockProvider = { nextBlockNumber },
                thorClient = thorClient,
            )

        try {
            while (maxBlocks == null || blocksProcessed < maxBlocks) {
                val block =
                    try {
                        stream.next()
                    } catch (e: Exception) {
                        indexers.forEach { indexer ->
                            indexer.logBlockFetchError(nextBlockNumber, e)
                            indexer.handleError()
                        }
                        nextBlockNumber = indexers.minOf { it.currentBlockNumber }
                        stream.reset()
                        continue
                    }
                var shouldRetryBlock = false

                for (indexer in executionOrder) {
                    indexer.restartIfNeeded()
                    indexer.processBlock(block) {
                        if (!shouldRetryBlock) {
                            nextBlockNumber = indexers.minOf { it.currentBlockNumber }
                            stream.reset()
                            shouldRetryBlock = true
                        }
                    }

                    if (indexer.status == Status.ERROR || indexer.status == Status.REORG) {
                        if (!shouldRetryBlock) {
                            nextBlockNumber = indexers.minOf { it.currentBlockNumber }
                            stream.reset()
                            shouldRetryBlock = true
                        }
                        break
                    }
                }

                if (shouldRetryBlock) {
                    continue
                }

                nextBlockNumber = block.number + 1
                blocksProcessed++
            }
        } finally {
            stream.close()
        }
    }
}
