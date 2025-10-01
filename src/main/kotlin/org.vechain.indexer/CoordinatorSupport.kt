package org.vechain.indexer

internal object CoordinatorSupport {

    suspend fun prepareIndexers(
        indexers: List<BlockIndexer>,
    ) {
        indexers.forEach { indexer ->
            indexer.initialise()

            if (indexer is LogsIndexer) {
                indexer.fastSync()
            }

            indexer.logStartingState()
        }
    }

    /**
     * Order the indexers topologically based on their dependencies. If there is a circular
     * dependency, an exception is thrown.
     */
    fun topologicalOrder(indexers: List<BlockIndexer>): List<BlockIndexer> {
        val ordered = mutableListOf<BlockIndexer>()
        val indexerSet = indexers.toSet()
        val visitState = mutableMapOf<BlockIndexer, VisitState>()

        fun visit(indexer: BlockIndexer) {
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
                        require(dependency is BlockIndexer) {
                            "Dependency ${dependency.name} for ${indexer.name} is not a block indexer"
                        }
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

    private enum class VisitState {
        VISITING,
        VISITED,
    }
}
