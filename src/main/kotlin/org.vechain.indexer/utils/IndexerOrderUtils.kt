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

    /**
     * Groups indexers by block number proximity. Indexers within [threshold] blocks of each other
     * are placed in the same group. When a dependency spans groups, only the dependent indexer is
     * moved to its dependency's group (not the entire group). Each group is topologically ordered
     * internally.
     *
     * @param indexers The list of indexers to group
     * @param threshold Maximum block gap allowed within a group
     * @return A list of groups, each topologically ordered
     */
    fun proximityGroups(indexers: List<Indexer>, threshold: Long): List<List<Indexer>> {
        if (indexers.isEmpty()) return emptyList()

        // Sort by current block number
        val sorted = indexers.sortedBy { it.getCurrentBlockNumber() }

        // Initial grouping by proximity
        val groups = mutableListOf<MutableList<Indexer>>()
        var currentGroup = mutableListOf(sorted[0])
        groups.add(currentGroup)

        for (i in 1 ..< sorted.size) {
            val gap = sorted[i].getCurrentBlockNumber() - sorted[i - 1].getCurrentBlockNumber()
            if (gap > threshold) {
                currentGroup = mutableListOf()
                groups.add(currentGroup)
            }
            currentGroup.add(sorted[i])
        }

        // Move dependent indexers to their dependency's group
        val indexerToGroup = mutableMapOf<Indexer, Int>()
        groups.forEachIndexed { idx, group -> group.forEach { indexerToGroup[it] = idx } }

        var moved = true
        while (moved) {
            moved = false
            for (indexer in indexers) {
                val dep = indexer.dependsOn ?: continue
                val indexerGroupIdx = indexerToGroup[indexer] ?: continue
                val depGroupIdx = indexerToGroup[dep] ?: continue
                if (indexerGroupIdx != depGroupIdx) {
                    // Move only the dependent indexer to its dependency's group
                    groups[indexerGroupIdx].remove(indexer)
                    groups[depGroupIdx].add(indexer)
                    indexerToGroup[indexer] = depGroupIdx
                    moved = true
                }
            }
        }

        // Remove empty groups, topologically order each group
        return groups.filter { it.isNotEmpty() }.map { group -> topologicalOrder(group).flatten() }
    }

    private enum class VisitState {
        VISITING,
        VISITED,
    }
}
