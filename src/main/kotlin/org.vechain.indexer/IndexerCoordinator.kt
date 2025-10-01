package org.vechain.indexer

import java.util.ArrayDeque
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
        dependencies: Map<BlockIndexer, Set<BlockIndexer>> = emptyMap(),
        maxBlocks: Long? = null,
    ) = coroutineScope {
        require(indexers.isNotEmpty()) { "At least one indexer is required" }

        val dependencyMap = CoordinatorSupport.buildDependencyMap(indexers, dependencies)
        val dependantsMap = CoordinatorSupport.buildDependantsMap(dependencyMap)
        val executionOrder = topologicalOrder(indexers, dependencyMap)
        CoordinatorSupport.prepareIndexers(indexers, dependencyMap, dependantsMap)

        var nextBlockNumber = indexers.minOf { it.currentBlockNumber }
        var blocksProcessed = 0L

        val stream =
            PrefetchingBlockStream(
                scope = this,
                batchSize = batchSize,
                currentBlockProvider = { nextBlockNumber },
                thorClient = thorClient,
            )

        indexers.forEach { it.attachBlockStream(stream) }

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

    private fun topologicalOrder(
        indexers: List<BlockIndexer>,
        dependencies: Map<BlockIndexer, Set<BlockIndexer>>,
    ): List<BlockIndexer> {
        val inDegree = indexers.associateWith { dependencies[it]?.size ?: 0 }.toMutableMap()
        val dependents =
            CoordinatorSupport.buildDependantsMap(dependencies)
                .mapValues { it.value.toMutableSet() }
                .toMutableMap()

        val queue = ArrayDeque<BlockIndexer>()
        inDegree.filterValues { it == 0 }.keys.forEach(queue::add)

        val ordered = mutableListOf<BlockIndexer>()
        while (queue.isNotEmpty()) {
            val current = queue.removeFirst()
            ordered += current
            dependents[current]?.forEach { dependent ->
                val remaining = inDegree.getValue(dependent) - 1
                inDegree[dependent] = remaining
                if (remaining == 0) {
                    queue.add(dependent)
                }
            }
        }

        if (ordered.size != indexers.size) {
            throw IllegalArgumentException("Dependency graph contains a cycle")
        }

        return ordered
    }
}
