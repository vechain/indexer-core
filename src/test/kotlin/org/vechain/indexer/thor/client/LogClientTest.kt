package org.vechain.indexer.thor.client

import io.mockk.coEvery
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLogsRequest
import org.vechain.indexer.thor.model.LogsOptions
import org.vechain.indexer.thor.model.LogsRange
import org.vechain.indexer.thor.model.TransferCriteria
import org.vechain.indexer.thor.model.TransferLogsRequest
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo

@ExtendWith(MockKExtension::class)
class LogClientTest {
    @MockK private lateinit var thorClient: ThorClient

    @Test
    fun `fetchEventLogs requests index metadata`() = runTest {
        val request = slot<EventLogsRequest>()
        val criteria = listOf(EventCriteria(address = "0xabc"))
        coEvery { thorClient.getEventLogs(capture(request)) } returns emptyList()

        val result = LogClient(thorClient).fetchEventLogs(1, 2, 50, criteria)

        expectThat(result).isEmpty()
        expect {
            that(request.captured.range).isEqualTo(LogsRange(from = 1, to = 2, unit = "block"))
            that(request.captured.options)
                .isEqualTo(LogsOptions(offset = 0, limit = 50, includeIndexes = true))
            that(request.captured.criteriaSet).isEqualTo(criteria)
            that(request.captured.order).isEqualTo("asc")
        }
    }

    @Test
    fun `fetchTransfers requests index metadata`() = runTest {
        val request = slot<TransferLogsRequest>()
        val criteria = listOf(TransferCriteria(sender = "0xabc"))
        coEvery { thorClient.getVetTransfers(capture(request)) } returns emptyList()

        val result = LogClient(thorClient).fetchTransfers(3, 4, 25, criteria)

        expectThat(result).isEmpty()
        expect {
            that(request.captured.range).isEqualTo(LogsRange(from = 3, to = 4, unit = "block"))
            that(request.captured.options)
                .isEqualTo(LogsOptions(offset = 0, limit = 25, includeIndexes = true))
            that(request.captured.criteriaSet).isEqualTo(criteria)
            that(request.captured.order).isEqualTo("asc")
        }
    }
}
