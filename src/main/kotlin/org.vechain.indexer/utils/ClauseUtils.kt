package org.vechain.indexer.utils

import org.vechain.indexer.Indexer
import org.vechain.indexer.thor.model.Clause

/**
 * Maps each indexer to the indices of its clauses in the combined clause list. Used to extract the
 * correct inspection results for each indexer from the batched response.
 */
typealias ClauseIndexMapping = Map<Indexer, List<Int>>

internal object ClauseUtils {

    /**
     * Builds a combined list of unique clauses from all indexers and creates a mapping from each
     * indexer to the indices of its clauses in the combined list.
     *
     * This is necessary because Thor returns inspection results in the same order as the clauses
     * sent, so we need to track which results belong to which indexer.
     */
    fun buildClauseListWithMapping(
        indexers: List<Indexer>
    ): Pair<List<Clause>, ClauseIndexMapping> {
        val allClauses = mutableListOf<Clause>()
        val clauseToIndex = mutableMapOf<Clause, Int>()
        val indexerToIndices = mutableMapOf<Indexer, List<Int>>()

        for (indexer in indexers) {
            val clauses = indexer.getInspectionClauses()?.takeIf { it.isNotEmpty() } ?: continue
            val indices = mutableListOf<Int>()

            for (clause in clauses) {
                val existingIndex = clauseToIndex[clause]
                if (existingIndex != null) {
                    indices.add(existingIndex)
                } else {
                    val newIndex = allClauses.size
                    allClauses.add(clause)
                    clauseToIndex[clause] = newIndex
                    indices.add(newIndex)
                }
            }

            indexerToIndices[indexer] = indices
        }

        return Pair(allClauses, indexerToIndices)
    }
}
