package org.vechain.indexer.thor.client

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
) {

    /** Fetches event logs from the Thor client. */
    suspend fun fetchEventLogs(
        fromBlock: Long,
        toBlock: Long,
        logFetchLimit: Long,
        eventCriteriaSet: List<EventCriteria>? = null
    ): List<EventLog> {
        val logs = mutableListOf<EventLog>()
        var offset = 0L
        while (true) {
            val response =
                thorClient.getEventLogs(
                    EventLogsRequest(
                        range = LogsRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = LogsOptions(offset = offset, limit = logFetchLimit),
                        criteriaSet = eventCriteriaSet,
                        order = "asc",
                    ),
                )

            if (response.isEmpty()) break
            logs.addAll(response)
            if (response.size < logFetchLimit) break
            offset += logFetchLimit
        }
        return logs
    }

    /** Fetches VET transfer logs from the Thor client. */
    suspend fun fetchTransfers(
        fromBlock: Long,
        toBlock: Long,
        logFetchLimit: Long,
        transferCriteriaSet: List<TransferCriteria>? = null
    ): List<TransferLog> {
        val transfers = mutableListOf<TransferLog>()
        var offset = 0L

        while (true) {
            val response =
                thorClient.getVetTransfers(
                    TransferLogsRequest(
                        range = LogsRange(from = fromBlock, to = toBlock, unit = "block"),
                        options = LogsOptions(offset = offset, limit = logFetchLimit),
                        order = "asc",
                        criteriaSet = transferCriteriaSet,
                    ),
                )

            if (response.isEmpty()) break
            transfers.addAll(response)
            if (response.size < logFetchLimit) break
            offset += logFetchLimit
        }

        return transfers
    }
}
