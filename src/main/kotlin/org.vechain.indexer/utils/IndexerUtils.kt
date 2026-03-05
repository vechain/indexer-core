package org.vechain.indexer.utils

import org.vechain.indexer.Status

object IndexerUtils {
    fun ensureStatus(status: Status, allowed: Set<Status>) {
        if (status !in allowed) {
            throw IllegalStateException("Invalid status: $status")
        }
    }
}
