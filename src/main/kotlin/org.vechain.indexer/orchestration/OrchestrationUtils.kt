package org.vechain.indexer.orchestration

import org.vechain.indexer.Indexer

internal object OrchestrationUtils {

    /**
     * Order the indexers topologically based on their dependencies. The returned grouped lists
     * represent dependency tiers that can be processed in parallel with other tiers. If there is a
     * circular dependency, an exception is thrown.
     */
    fun topologicalOrder(indexers: List<Indexer>): List<List<Indexer>> {
        val ordered = mutableListOf<Indexer>()
        val indexerSet = indexers.toSet()
        val visitState = mutableMapOf<Indexer, VisitState>()
        val depthMap = mutableMapOf<Indexer, Int>()

        fun visit(indexer: Indexer): Int =
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

        val depthToIndexers = mutableMapOf<Int, MutableList<Indexer>>()
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
