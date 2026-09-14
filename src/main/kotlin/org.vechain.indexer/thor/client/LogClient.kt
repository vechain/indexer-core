package org.vechain.indexer.thor.client

import org.vechain.indexer.exception.LogPaginationLimitException
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.EventLogsRequest
import org.vechain.indexer.thor.model.LogsOptions
import org.vechain.indexer.thor.model.LogsRange
import org.vechain.indexer.thor.model.TransferCriteria
import org.vechain.indexer.thor.model.TransferLog
import org.vechain.indexer.thor.model.TransferLogsRequest

class LogClient(
    private val thorClient: ThorClient,
    private val maxOffset: Long = MAX_OFFSET,
) {

    /** Fetches event logs from the Thor client. */
    suspend fun fetchEventLogs(
        fromBlock: Long,
        toBlock: Long,
        logFetchLimit: Long,
        eventCriteriaSet: List<EventCriteria>? = null
    ): List<EventLog> =
        fetchPaged(fromBlock, toBlock, logFetchLimit) { offset ->
            thorClient.getEventLogs(
                EventLogsRequest(
                    range = LogsRange(from = fromBlock, to = toBlock, unit = "block"),
                    options =
                        LogsOptions(offset = offset, limit = logFetchLimit, includeIndexes = true),
                    criteriaSet = eventCriteriaSet,
                    order = "asc",
                ),
            )
        }

    /** Fetches VET transfer logs from the Thor client. */
    suspend fun fetchTransfers(
        fromBlock: Long,
        toBlock: Long,
        logFetchLimit: Long,
        transferCriteriaSet: List<TransferCriteria>? = null
    ): List<TransferLog> =
        fetchPaged(fromBlock, toBlock, logFetchLimit) { offset ->
            thorClient.getVetTransfers(
                TransferLogsRequest(
                    range = LogsRange(from = fromBlock, to = toBlock, unit = "block"),
                    options =
                        LogsOptions(offset = offset, limit = logFetchLimit, includeIndexes = true),
                    order = "asc",
                    criteriaSet = transferCriteriaSet,
                ),
            )
        }

    /**
     * Pages through a log endpoint until it runs out of results.
     *
     * Thor caps `options.offset`, so a range dense enough to push past [maxOffset] cannot be read
     * in full at this width. That raises [LogPaginationLimitException] rather than returning a
     * silently truncated list, leaving the caller to narrow the range and retry.
     */
    private suspend fun <T> fetchPaged(
        fromBlock: Long,
        toBlock: Long,
        logFetchLimit: Long,
        fetchPage: suspend (offset: Long) -> List<T>,
    ): List<T> {
        val logs = mutableListOf<T>()
        var offset = 0L
        while (true) {
            val page =
                try {
                    fetchPage(offset)
                } catch (e: Exception) {
                    // A node capped lower than [maxOffset] rejects the request instead of us
                    // pre-empting it; translate that into the same signal.
                    if (OFFSET_CAP_ERROR.containsMatchIn(e.message.orEmpty())) {
                        throw paginationLimit(fromBlock, toBlock, logs.size, e)
                    }
                    throw e
                }
            if (page.isEmpty()) break
            logs.addAll(page)
            if (page.size < logFetchLimit) break
            offset += logFetchLimit
            if (offset > maxOffset) throw paginationLimit(fromBlock, toBlock, logs.size)
        }
        return logs
    }

    private fun paginationLimit(
        fromBlock: Long,
        toBlock: Long,
        logsFetched: Int,
        cause: Exception? = null,
    ) =
        LogPaginationLimitException(
            fromBlock,
            toBlock,
            logsFetched,
            "Blocks $fromBlock..$toBlock hold more logs than Thor's offset cap allows paging " +
                "through ($logsFetched read so far); narrow the block range or the criteria set",
            cause,
        )

    companion object {
        /** Thor's default ceiling on `options.offset` for the log endpoints. */
        const val MAX_OFFSET = 100_000L

        private val OFFSET_CAP_ERROR = Regex("offset exceeds the maximum", RegexOption.IGNORE_CASE)
    }
}
