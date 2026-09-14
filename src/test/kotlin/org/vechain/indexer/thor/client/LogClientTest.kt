package org.vechain.indexer.thor.client

import io.mockk.coEvery
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.exception.LogPaginationLimitException
import org.vechain.indexer.fixtures.EventLogFixtures
import org.vechain.indexer.fixtures.TransferLogFixtures
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLogsRequest
import org.vechain.indexer.thor.model.LogsOptions
import org.vechain.indexer.thor.model.LogsRange
import org.vechain.indexer.thor.model.TransferCriteria
import org.vechain.indexer.thor.model.TransferLogsRequest
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo

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

    @Test
    fun `fetchEventLogs raises the pagination limit once the offset cap is passed`() = runTest {
        val page = List(10) { EventLogFixtures.LOGS_STRINGS.first() }
        coEvery { thorClient.getEventLogs(any()) } returns page

        val thrown =
            runCatching { LogClient(thorClient, maxOffset = 30).fetchEventLogs(1, 500, 10) }
                .exceptionOrNull()

        expectThat(thrown).isA<LogPaginationLimitException>().and {
            get { fromBlock }.isEqualTo(1)
            get { toBlock }.isEqualTo(500)
            get { logsFetched }.isGreaterThanOrEqualTo(30)
        }
    }

    @Test
    fun `fetchTransfers raises the pagination limit once the offset cap is passed`() = runTest {
        val page = List(10) { TransferLogFixtures.LOGS_VET_TRANSFER.first() }
        coEvery { thorClient.getVetTransfers(any()) } returns page

        val thrown =
            runCatching { LogClient(thorClient, maxOffset = 30).fetchTransfers(1, 500, 10) }
                .exceptionOrNull()

        expectThat(thrown).isA<LogPaginationLimitException>()
    }

    @Test
    fun `fetchEventLogs translates a node-side offset rejection into the pagination limit`() =
        runTest {
            coEvery { thorClient.getEventLogs(any()) } throws
                RuntimeException(
                    "Thor /logs/event returned 403: options.offset exceeds the maximum " +
                        "allowed value of 100000"
                )

            val thrown =
                runCatching { LogClient(thorClient).fetchEventLogs(1, 500, 10) }.exceptionOrNull()

            expectThat(thrown).isA<LogPaginationLimitException>()
        }

    @Test
    fun `fetchEventLogs lets an unrelated failure through untouched`() = runTest {
        coEvery { thorClient.getEventLogs(any()) } throws RuntimeException("Thor /logs/event 500")

        val thrown =
            runCatching { LogClient(thorClient).fetchEventLogs(1, 500, 10) }.exceptionOrNull()

        expectThat(thrown).isA<RuntimeException>()
        expectThat(thrown is LogPaginationLimitException).isEqualTo(false)
    }

    @Test
    fun `fetchEventLogs pages right up to the cap without complaint`() = runTest {
        val full = List(10) { EventLogFixtures.LOGS_STRINGS.first() }
        coEvery { thorClient.getEventLogs(any()) } returnsMany
            listOf(full, full, full, full.take(3))

        val result = LogClient(thorClient, maxOffset = 30).fetchEventLogs(1, 500, 10)

        expectThat(result).hasSize(33)
    }
}
