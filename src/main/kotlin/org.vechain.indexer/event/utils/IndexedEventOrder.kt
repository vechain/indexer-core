package org.vechain.indexer.event.utils

import org.vechain.indexer.event.model.generic.IndexedEvent

object IndexedEventOrder {
    fun sortChronologically(events: List<IndexedEvent>): List<IndexedEvent> =
        events
            .withIndex()
            .sortedWith { left, right ->
                comparePosition(left.value, right.value).takeIf { it != 0 }
                    ?: left.index.compareTo(right.index)
            }
            .map { it.value }

    private fun comparePosition(
        left: IndexedEvent,
        right: IndexedEvent,
    ): Int {
        left.blockNumber
            .compareTo(right.blockNumber)
            .takeIf { it != 0 }
            ?.let {
                return it
            }

        compareKnown(left.txIndex, right.txIndex)
            ?.takeIf { it != 0 }
            ?.let {
                return it
            }

        val sameKnownTransaction = left.txIndex != null && left.txIndex == right.txIndex
        val sameTransactionId = left.txId == right.txId
        if (sameKnownTransaction || sameTransactionId) {
            left.clauseIndex
                .compareTo(right.clauseIndex)
                .takeIf { it != 0 }
                ?.let {
                    return it
                }
            compareKnown(left.logIndex, right.logIndex)
                ?.takeIf { it != 0 }
                ?.let {
                    return it
                }
        }

        return 0
    }

    private fun compareKnown(
        left: Long?,
        right: Long?,
    ): Int? = if (left != null && right != null) left.compareTo(right) else null
}
