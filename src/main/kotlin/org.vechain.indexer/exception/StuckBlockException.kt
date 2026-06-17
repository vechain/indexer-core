package org.vechain.indexer.exception

/** Thrown when a block cannot be processed after the bounded retry budget is exhausted. */
class StuckBlockException(message: String, cause: Throwable) : Exception(message, cause)
