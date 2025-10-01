package org.vechain.indexer

import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.RestartIndexerException
import org.vechain.indexer.fixtures.IndexedEventFixture.create
import org.vechain.indexer.thor.client.LogClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.EventMeta
import org.vechain.indexer.thor.model.TransferLog
import strikt.api.expectThrows

@ExtendWith(MockKExtension::class)
class LogsIndexerTest {

    @MockK private lateinit var processor: IndexerProcessor

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var logClient: LogClient

    @MockK private lateinit var eventProcessor: CombinedEventProcessor

    @MockK private lateinit var pruner: Pruner

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
    }

    @Nested
    inner class SyncTests {
        @Test
        fun `sync should fetch and process logs in batches`() = runTest {
            val eventLogs = listOf(buildEventLog("0x1"), buildEventLog("0x2"))
            val transferLogs = listOf(buildTransferLog("0x3"), buildTransferLog("0x4"))

            coEvery { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 3L, any(), any()) } returns eventLogs
            coEvery { logClient.fetchTransfers(0L, 3L, any(), any()) } returns transferLogs
            every { eventProcessor.processEvents(eventLogs, transferLogs) } returns
                eventLogs.map { create(it.address) }
            every { processor.process(any()) } just runs

            val indexer = TestableLogsIndexer(thorClient, logClient, processor, eventProcessor)
            indexer.testSync(3L)

            coVerify(exactly = 1) { logClient.fetchEventLogs(0L, 3L, any(), any()) }
            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 3L, any(), any()) }
            verify(exactly = 1) { processor.process(any()) }
        }

        @Test
        fun `If an error is thrown, a restart should be triggered`() = runTest {
            val toBlock = 10L

            // Mock processor and eventProcessor
            coEvery { logClient.fetchEventLogs(0L, 10L, any()) } throws
                RuntimeException("Simulated error")

            // Run the sync
            val indexer = TestableLogsIndexer(thorClient, logClient, processor, eventProcessor)

            // Expect a RestartIndexerException to be thrown
            expectThrows<RestartIndexerException> { indexer.testSync(toBlock) }
        }

        @Test
        fun `correct currBlockNumber should be set after sync`() = runTest {
            val eventLogs = listOf(buildEventLog("0x1"), buildEventLog("0x2"))
            val transferLogs = listOf(buildTransferLog("0x3"), buildTransferLog("0x4"))

            coEvery { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 5L, any(), any()) } returns eventLogs
            coEvery { logClient.fetchTransfers(0L, 5L, any(), any()) } returns transferLogs
            every { eventProcessor.processEvents(eventLogs, transferLogs) } returns
                eventLogs.map { create(it.address) }
            every { processor.process(any()) } just runs

            val indexer = TestableLogsIndexer(thorClient, logClient, processor, eventProcessor)
            indexer.testSync(5L)

            assert(indexer.currentBlockNumber == 6L)

            coEvery { logClient.fetchEventLogs(6L, 10L, any(), any()) } returns eventLogs
            coEvery { logClient.fetchTransfers(6L, 10L, any(), any()) } returns transferLogs

            indexer.testSync(10L)

            assert(indexer.currentBlockNumber == 11L)
        }
    }

    private fun buildEventLog(address: String): EventLog =
        EventLog(
            address = address,
            topics = listOf("0xTopic1", "0xTopic2"),
            data = "0xData",
            meta =
                EventMeta(
                    blockID = "0x1",
                    blockNumber = 1L,
                    blockTimestamp = 1234567890L,
                    txID = "0xTransaction",
                    txOrigin = "0xOrigin",
                    clauseIndex = 1
                )
        )

    private fun buildTransferLog(sender: String): TransferLog =
        TransferLog(
            sender = sender,
            recipient = "0xRecipient",
            amount = "0x001",
            meta =
                EventMeta(
                    blockID = "0x1",
                    blockNumber = 1L,
                    blockTimestamp = 1234567890L,
                    txID = "0xTransaction",
                    txOrigin = "0xOrigin",
                    clauseIndex = 1
                )
        )

    private class TestableLogsIndexer(
        thorClient: ThorClient,
        override val logClient: LogClient,
        processor: IndexerProcessor,
        eventProcessor: CombinedEventProcessor,
    ) :
        LogsIndexer(
            name = "test",
            thorClient = thorClient,
            processor = processor,
            startBlock = 1L,
            syncLoggerInterval = 1L,
            excludeVetTransfers = false,
            blockBatchSize = 5,
            logFetchLimit = 100,
            eventCriteriaSet = null,
            transferCriteriaSet = null,
            eventProcessor = eventProcessor,
            pruner = null,
            prunerInterval = 10_000L,
        ) {

        var iterations: Long? = null

        suspend fun start(iterations: Long) {
            this.iterations = iterations
            super.start()
        }

        override suspend fun run() {
            runLoop(iterations)
        }

        suspend fun testSync(toBlock: Long) {
            sync(toBlock)
        }
    }
}
