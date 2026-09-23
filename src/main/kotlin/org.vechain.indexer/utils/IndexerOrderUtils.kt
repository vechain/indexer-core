package org.vechain.indexer.utils

import org.vechain.indexer.Indexer

internal object IndexerOrderUtils {

    /**
     * Orders indexers into groups based on their dependencies. Each group contains indexers that
     * are ordered sequentially (dependencies before dependents). Independent indexers (no shared
     * dependency chain) are placed in separate groups so they can be processed in parallel.
     *
     * @param indexers The list of indexers to order
     * @return A list of groups, each containing indexers in dependency order
     * @throws IllegalStateException if a circular dependency is detected
     * @throws IllegalArgumentException if a dependency is not in the provided indexers list
     */
    fun topologicalOrder(indexers: List<Indexer>): List<List<Indexer>> {
        if (indexers.isEmpty()) return emptyList()

        val ordered = dependencySort(indexers)
        val components = connectedComponents(indexers)

        // Group by component, preserving topological order from `ordered`
        val groups = mutableMapOf<Indexer, MutableList<Indexer>>()
        for (indexer in ordered) {
            groups.getOrPut(components.getValue(indexer)) { mutableListOf() }.add(indexer)
        }

        return groups.values.toList()
    }

    /**
     * Topologically sorts indexers so dependencies appear before dependents. Also validates that
     * all dependencies exist in the provided list and detects circular dependencies.
     *
     * @param indexers The list of indexers to sort
     * @return Indexers in dependency-first order
     * @throws IllegalStateException if a circular dependency is detected
     * @throws IllegalArgumentException if a dependency is not in the provided indexers list
     */
    fun dependencySort(indexers: List<Indexer>): List<Indexer> {
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

        indexers.forEach { visit(it) }
        return ordered
    }

    /**
     * Partitions indexers into connected components based on dependency relationships. Indexers
     * that share a dependency chain belong to the same component. Independent indexers each get
     * their own component.
     *
     * @param indexers The list of indexers to partition
     * @return A map from each indexer to its component root
     */
    fun connectedComponents(indexers: List<Indexer>): Map<Indexer, Indexer> {
        val union = IndexerUnion().apply { linkDependencies(indexers) }
        return indexers.associateWith { union.find(it) }
    }

    /** Groups indexers that must advance together — see `docs/IndexerOverview.md`. */
    fun proximityGroups(indexers: List<Indexer>, threshold: Long): List<List<Indexer>> {
        if (indexers.isEmpty()) return emptyList()

        val union = IndexerUnion().apply { linkDependencies(indexers) }

        // A dependency component advances from its lowest member, so that block is its position.
        val components =
            indexers
                .groupBy { union.find(it) }
                .values
                .map { it.first() to it.minOf { i -> i.getCurrentBlockNumber() } }
                .sortedBy { it.second }
        for (i in 1 ..< components.size) {
            val gap = components[i].second - components[i - 1].second
            if (gap <= threshold) union.merge(components[i].first, components[i - 1].first)
        }

        val sorted = indexers.sortedBy { it.getCurrentBlockNumber() }
        return sorted.groupBy { union.find(it) }.values.map { topologicalOrder(it).flatten() }
    }

    /** Union-find over indexers; absent indexers are implicitly their own root. */
    private class IndexerUnion {
        private val parent = mutableMapOf<Indexer, Indexer>()

        fun find(x: Indexer): Indexer {
            var root = x
            while (parent.getOrDefault(root, root) != root) root = parent.getValue(root)
            var curr = x
            while (curr != root) {
                val next = parent.getOrDefault(curr, curr)
                parent[curr] = root
                curr = next
            }
            return root
        }

        fun merge(a: Indexer, b: Indexer) {
            parent[find(a)] = find(b)
        }

        fun linkDependencies(indexers: List<Indexer>) {
            val present = indexers.toSet()
            for (indexer in indexers) {
                indexer.dependsOn?.takeIf { it in present }?.let { merge(indexer, it) }
            }
        }
    }

    private enum class VisitState {
        VISITING,
        VISITED,
    }
}
