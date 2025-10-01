package org.vechain.indexer

internal object CoordinatorSupport {
    fun buildDependencyMap(
        indexers: List<BlockIndexer>,
        externalDependencies: Map<BlockIndexer, Set<BlockIndexer>> = emptyMap(),
    ): Map<BlockIndexer, Set<BlockIndexer>> {
        val indexerSet = indexers.toSet()
        return indexers.associateWith { indexer ->
            val declared = indexer.dependantIndexers.filterIsInstance<BlockIndexer>().toSet()
            val external = externalDependencies[indexer] ?: emptySet()
            val combined = declared + external

            val unknown = combined - indexerSet
            require(unknown.isEmpty()) {
                "Dependencies ${unknown.map { it.name }} are not part of the provided indexers"
            }

            combined
        }
    }

    fun buildDependantsMap(
        dependencies: Map<BlockIndexer, Set<BlockIndexer>>,
    ): Map<BlockIndexer, Set<BlockIndexer>> {
        val dependants = mutableMapOf<BlockIndexer, MutableSet<BlockIndexer>>()
        dependencies.keys.forEach { dependants.putIfAbsent(it, mutableSetOf()) }
        dependencies.values.flatten().forEach { dependants.putIfAbsent(it, mutableSetOf()) }

        dependencies.forEach { (dependent, upstream) ->
            upstream.forEach { prerequisite -> dependants.getValue(prerequisite).add(dependent) }
        }

        return dependants.mapValues { it.value.toSet() }
    }

    suspend fun prepareIndexers(
        indexers: List<BlockIndexer>,
        dependencyMap: Map<BlockIndexer, Set<BlockIndexer>>,
        dependantsMap: Map<BlockIndexer, Set<BlockIndexer>>,
    ) {
        indexers.forEach { indexer ->
            if (indexer is LogsIndexer) {
                val hasDependencies = dependencyMap[indexer]?.isNotEmpty() == true
                val hasDependants = dependantsMap[indexer]?.isNotEmpty() == true
                if (hasDependencies || hasDependants) {
                    indexer.disableFastSync()
                }
            }

            indexer.initialise()

            if (indexer is LogsIndexer && indexer.isFastSyncEnabled()) {
                indexer.fastSyncIfEnabled()
            }

            indexer.logStartingState()
        }
    }
}
