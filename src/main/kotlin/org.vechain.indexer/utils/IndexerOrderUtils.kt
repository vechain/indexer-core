package org.vechain.indexer.utils

import org.vechain.indexer.Indexer

internal object IndexerOrderUtils {

    /**
     * Orders indexers into groups based on their dependencies. Each group contains indexers that
     * are ordered sequentially (dependencies before dependents). Groups can be processed in
     * parallel with each other.
     *
     * The algorithm creates a single group that contains all indexers in topological order,
     * ensuring that: 1. Dependencies always appear before their dependents 2. The order is stable
     * and deterministic 3. Circular dependencies are detected and rejected
     *
     * @param indexers The list of indexers to order
     * @return A list containing a single group with all indexers in dependency order
     * @throws IllegalStateException if a circular dependency is detected
     * @throws IllegalArgumentException if a dependency is not in the provided indexers list
     */
    fun topologicalOrder(indexers: List<Indexer>): List<List<Indexer>> {
        if (indexers.isEmpty()) return emptyList()

        val indexerSet = indexers.toSet()
        val visitState = mutableMapOf<Indexer, VisitState>()
        val ordered = mutableListOf<Indexer>()

        fun visit(indexer: Indexer) {
            when (visitState[indexer]) {
                VisitState.VISITED -> return
                VisitState.VISITING -> {
                    throw IllegalStateException(
                        "Circular dependency detected involving indexer ${indexer.name}",
                    )
                }
                null -> {
                    visitState[indexer] = VisitState.VISITING

                    val dependency = indexer.dependsOn
                    if (dependency != null) {
                        require(dependency in indexerSet) {
                            "Dependency ${dependency.name} for ${indexer.name} is not part of the provided indexers"
                        }
                        visit(dependency)
                    }

                    visitState[indexer] = VisitState.VISITED
                    ordered.add(indexer)
                }
            }
        }

        // Visit all indexers in the order they were provided
        indexers.forEach { visit(it) }

        // Return all indexers in a single group, properly ordered
        return listOf(ordered)
    }

    private enum class VisitState {
        VISITING,
        VISITED,
    }
}
