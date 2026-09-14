package org.vechain.indexer.exception

/**
 * Thrown when a block range holds more logs than Thor's `options.offset` cap allows us to page
 * through. The range must be narrowed and the fetch retried; [logsFetched] is the number of logs
 * read before the cap was reached, which is a lower bound on the range's true log count.
 */
class LogPaginationLimitException(
    val fromBlock: Long,
    val toBlock: Long,
    val logsFetched: Int,
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)
