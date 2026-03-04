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
     * Groups indexers by proximity of their current block numbers, preserving dependency chains.
     *
     * Indexers within [threshold] blocks of each other share a group. Cross-group dependency chains
     * are extracted and re-merged into the closest group (or kept standalone). Each group is
     * topologically ordered internally.
     */
    fun proximityGroups(indexers: List<Indexer>, threshold: Long): List<List<Indexer>> {
        if (indexers.isEmpty()) return emptyList()

        // Phase A: Initial proximity grouping by current block number
        val sorted = indexers.sortedBy { it.getCurrentBlockNumber() }
        val groups = mutableListOf<MutableSet<Indexer>>()
        var currentGroup = mutableSetOf(sorted.first())
        for (i in 1 ..< sorted.size) {
            val gap = sorted[i].getCurrentBlockNumber() - sorted[i - 1].getCurrentBlockNumber()
            if (gap > threshold) {
                groups.add(currentGroup)
                currentGroup = mutableSetOf()
            }
            currentGroup.add(sorted[i])
        }
        groups.add(currentGroup)

        // Phase B: Extract cross-group dependency chains
        val indexerSet = indexers.toSet()
        val indexerToGroup = mutableMapOf<Indexer, MutableSet<Indexer>>()
        for (group in groups) {
            for (indexer in group) {
                indexerToGroup[indexer] = group
            }
        }

        val extracted = mutableListOf<MutableSet<Indexer>>()
        val alreadyExtracted = mutableSetOf<Indexer>()

        for (indexer in indexers) {
            val dep = indexer.dependsOn ?: continue
            val indexerGroup = indexerToGroup[indexer] ?: continue
            val depGroup = indexerToGroup[dep] ?: continue
            if (indexerGroup === depGroup) continue
            if (indexer in alreadyExtracted) continue

            // Collect full chain: walk up through dependsOn
            val chain = mutableSetOf<Indexer>()
            var current: Indexer? = indexer
            while (current != null && current in indexerSet) {
                chain.add(current)
                current = current.dependsOn
            }
            // Collect downstream dependents
            val dependentMap = indexers.filter { it.dependsOn != null }.groupBy { it.dependsOn!! }
            val queue = ArrayDeque(chain.toList())
            while (queue.isNotEmpty()) {
                val node = queue.removeFirst()
                dependentMap[node]?.forEach { dependent ->
                    if (chain.add(dependent)) queue.addLast(dependent)
                }
            }

            // Remove from original groups
            for (member in chain) {
                indexerToGroup[member]?.remove(member)
                alreadyExtracted.add(member)
            }
            extracted.add(chain)
        }

        // Phase C: Re-merge extracted chains into closest group
        for (chain in extracted) {
            val chainMin = chain.minOf { it.getCurrentBlockNumber() }
            var closestGroup: MutableSet<Indexer>? = null
            var closestGap = Long.MAX_VALUE
            for (group in groups) {
                if (group.isEmpty()) continue
                val groupMin = group.minOf { it.getCurrentBlockNumber() }
                val gap = kotlin.math.abs(chainMin - groupMin)
                if (gap <= threshold && gap < closestGap) {
                    closestGap = gap
                    closestGroup = group
                }
            }
            if (closestGroup != null) {
                closestGroup.addAll(chain)
            } else {
                groups.add(chain)
            }
        }

        // Filter empty groups and topologically order each
        return groups.filter { it.isNotEmpty() }.map { topologicalOrder(it.toList()).flatten() }
    }

    private enum class VisitState {
        VISITING,
        VISITED,
    }
}
