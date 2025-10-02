package org.vechain.indexer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.vechain.indexer.exception.RestartIndexerException

internal object CoordinatorSupport {

    suspend fun prepareIndexers(
        scope: CoroutineScope,
        indexers: List<BlockIndexer>,
    ) {
        val jobs =
            indexers.map { indexer ->
                scope.launch {
                    indexer.initialise()

                    if (indexer is LogsIndexer) {
                        while (true) {
                            try {
                                indexer.fastSync()
                                break
                            } catch (_: RestartIndexerException) {
                                indexer.handleError()
                                indexer.restartIfNeeded()
                            }
                        }
                    }

                    indexer.logStartingState()
                }
            }
        jobs.joinAll()
    }

    /**
     * Order the indexers topologically based on their dependencies. The returned grouped lists
     * represent dependency tiers that can be processed in parallel with other tiers. If there is a
     * circular dependency, an exception is thrown.
     */
    fun topologicalOrder(indexers: List<BlockIndexer>): List<List<BlockIndexer>> {
        val ordered = mutableListOf<BlockIndexer>()
        val indexerSet = indexers.toSet()
        val visitState = mutableMapOf<BlockIndexer, VisitState>()
        val depthMap = mutableMapOf<BlockIndexer, Int>()

        fun visit(indexer: BlockIndexer): Int =
            when (visitState[indexer]) {
                VisitState.VISITED -> depthMap.getValue(indexer)
                VisitState.VISITING -> {
                    throw IllegalStateException(
                        "Circular dependency detected involving indexer ${indexer.name}",
                    )
                }
                null -> {
                    visitState[indexer] = VisitState.VISITING

                    val dependency = indexer.dependsOn
                    val depth =
                        if (dependency != null) {
                            require(dependency is BlockIndexer) {
                                "Dependency ${dependency.name} for ${indexer.name} is not a block indexer"
                            }
                            require(dependency in indexerSet) {
                                "Dependency ${dependency.name} for ${indexer.name} is not part of the provided indexers"
                            }

                            visit(dependency) + 1
                        } else {
                            0
                        }

                    visitState[indexer] = VisitState.VISITED
                    depthMap[indexer] = depth
                    ordered.add(indexer)
                    depth
                }
            }

        indexers.forEach { visit(it) }

        val depthToIndexers = mutableMapOf<Int, MutableList<BlockIndexer>>()
        ordered.forEach { indexer ->
            val depth = depthMap.getValue(indexer)
            depthToIndexers.getOrPut(depth) { mutableListOf() }.add(indexer)
        }

        return depthToIndexers.toSortedMap().values.map { it.toList() }
    }

    private enum class VisitState {
        VISITING,
        VISITED,
    }
}
