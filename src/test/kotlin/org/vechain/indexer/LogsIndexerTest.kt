package org.vechain.indexer

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.fixtures.EventLogFixtures
import org.vechain.indexer.fixtures.IndexedEventFixture
import org.vechain.indexer.fixtures.TransferLogFixtures
import org.vechain.indexer.thor.client.LogClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*
import strikt.api.expect
import strikt.assertions.isEqualTo
import strikt.assertions.isNull

internal class TestableLogsIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    syncLoggerInterval: Long,
    excludeVetTransfers: Boolean,
    blockBatchSize: Long,
    logFetchLimit: Long,
    eventCriteriaSet: List<EventCriteria>?,
    transferCriteriaSet: List<TransferCriteria>?,
    eventProcessor: CombinedEventProcessor?,
    pruner: Pruner?,
    prunerInterval: Long,
    val mockLogClient: LogClient? = null,
) :
    LogsIndexer(
        name = name,
        thorClient = thorClient,
        processor = processor,
        startBlock = startBlock,
        syncLoggerInterval = syncLoggerInterval,
        excludeVetTransfers = excludeVetTransfers,
        blockBatchSize = blockBatchSize,
        logFetchLimit = logFetchLimit,
        eventCriteriaSet = eventCriteriaSet,
        transferCriteriaSet = transferCriteriaSet,
        eventProcessor = eventProcessor,
        pruner = pruner,
        prunerInterval = prunerInterval,
    ) {

    override val logClient: LogClient
        get() = mockLogClient ?: super.logClient

    suspend fun publicSync(toBlock: BlockIdentifier) {
        super.sync(toBlock)
    }

    fun publicSetStatus(newStatus: Status) {
        super.setStatus(newStatus)
    }

    fun publicSetCurrentBlockNumber(value: Long) {
        super.setCurrentBlockNumber(value)
    }

    fun publicSetPreviousBlock(block: BlockIdentifier?) {
        super.setPreviousBlock(block)
    }
}

@ExtendWith(MockKExtension::class)
internal class LogsIndexerTest {
    @MockK private lateinit var processor: IndexerProcessor

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var eventProcessor: CombinedEventProcessor

    @MockK private lateinit var logClient: LogClient

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
        every { processor.rollback(any()) } just Runs
    }

    @Nested
    inner class FastSync {
        private lateinit var indexer: TestableLogsIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                spyk(
                    TestableLogsIndexer(
                        name = "TestLogsIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        syncLoggerInterval = 1L,
                        excludeVetTransfers = false,
                        blockBatchSize = 10L,
                        logFetchLimit = 100L,
                        eventCriteriaSet = null,
                        transferCriteriaSet = null,
                        eventProcessor = eventProcessor,
                        pruner = null,
                        prunerInterval = 10000L,
                        mockLogClient = logClient,
                    )
                )
            // Default mocking to prevent failures when publicSync is not explicitly mocked
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(any(), any(), any(), any()) } returns emptyList()
            coEvery { logClient.fetchEventLogs(any(), any(), any(), any()) } returns emptyList()
        }

        @Test
        fun `should sync to finalized block when current block is behind`() = runBlocking {
            val finalizedBlock = buildBlock(num = 100L)
            coEvery { thorClient.getFinalizedBlock() } returns finalizedBlock

            indexer.publicSetCurrentBlockNumber(50L)
            indexer.fastSync()

            expect {
                that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                that(indexer.getPreviousBlock())
                    .isEqualTo(BlockIdentifier(number = 100L, id = "0x100"))
                that(indexer.getCurrentBlockNumber()).isEqualTo(100L)
            }
        }

        @Test
        fun `should skip sync when current block is ahead of finalized block`() = runBlocking {
            val finalizedBlock = buildBlock(num = 100L)
            coEvery { thorClient.getFinalizedBlock() } returns finalizedBlock

            indexer.publicSetCurrentBlockNumber(150L)
            indexer.fastSync()

            expect {
                that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                that(indexer.getPreviousBlock()).isNull()
                that(indexer.getCurrentBlockNumber()).isEqualTo(150L)
            }
        }

        @Test
        fun `if sync skipped should retain the existing value for previousBlock`() = runBlocking {
            val finalizedBlock = buildBlock(num = 100L)
            coEvery { thorClient.getFinalizedBlock() } returns finalizedBlock

            indexer.publicSetCurrentBlockNumber(150L)
            indexer.publicSetPreviousBlock(BlockIdentifier(number = 75L, id = "0x75"))
            indexer.fastSync()

            expect {
                that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                that(indexer.getPreviousBlock())
                    .isEqualTo(BlockIdentifier(number = 75L, id = "0x75"))
                that(indexer.getCurrentBlockNumber()).isEqualTo(150L)
            }
        }

        @Test
        fun `should skip sync when current block equals finalized block`() = runBlocking {
            val finalizedBlock = buildBlock(num = 100L)
            coEvery { thorClient.getFinalizedBlock() } returns finalizedBlock

            indexer.publicSetCurrentBlockNumber(100L)
            indexer.fastSync()

            expect {
                that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                that(indexer.getPreviousBlock()).isNull()
                that(indexer.getCurrentBlockNumber()).isEqualTo(100L)
            }
        }
    }

    @Nested
    inner class Sync {
        private lateinit var indexer: TestableLogsIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                spyk(
                    TestableLogsIndexer(
                        name = "TestLogsIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        syncLoggerInterval = 1L,
                        excludeVetTransfers = false,
                        blockBatchSize = 10L,
                        logFetchLimit = 100L,
                        eventCriteriaSet = null,
                        transferCriteriaSet = null,
                        eventProcessor = eventProcessor,
                        pruner = null,
                        prunerInterval = 10000L,
                        mockLogClient = logClient,
                    )
                )
            // Default mocking to prevent failures
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(any(), any(), any(), any()) } returns emptyList()
            coEvery { logClient.fetchEventLogs(any(), any(), any(), any()) } returns emptyList()
        }

        @Test
        fun `should process event logs and transfer logs in batches`() = runBlocking {
            val eventLogs = EventLogFixtures.LOGS_STRINGS
            val transferLogs = TransferLogFixtures.LOGS_VET_TRANSFER
            val indexedEvents = listOf(IndexedEventFixture.create())

            every { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 9L, 100L, null) } returns eventLogs
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns transferLogs
            every {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            } returns indexedEvents
            every { processor.process(any()) } just Runs

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            coVerify(exactly = 1) { logClient.fetchEventLogs(0L, 9L, 100L, null) }
            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 9L, 100L, null) }
            verify(exactly = 1) {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            }
            verify(exactly = 1) {
                processor.process(match { it is IndexingResult.EventsOnly && it.endBlock == 9L })
            }
            expect { that(indexer.getCurrentBlockNumber()).isEqualTo(10L) }
        }

        @Test
        fun `should only fetch event logs when eventProcessor has ABIs`() = runBlocking {
            val eventLogs = listOf<EventLog>()

            every { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 9L, 100L, null) } returns eventLogs
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns emptyList()
            every {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            coVerify(exactly = 1) { logClient.fetchEventLogs(0L, 9L, 100L, null) }
        }

        @Test
        fun `should not fetch event logs when eventProcessor has no ABIs`() = runBlocking {
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            coVerify(exactly = 0) { logClient.fetchEventLogs(any(), any(), any(), any()) }
        }

        @Test
        fun `should exclude VET transfers when excludeVetTransfers is true`() = runBlocking {
            val indexerWithoutTransfers =
                spyk(
                    TestableLogsIndexer(
                        name = "TestLogsIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        syncLoggerInterval = 1L,
                        excludeVetTransfers = true,
                        blockBatchSize = 10L,
                        logFetchLimit = 100L,
                        eventCriteriaSet = null,
                        transferCriteriaSet = null,
                        eventProcessor = eventProcessor,
                        pruner = null,
                        prunerInterval = 10000L,
                        mockLogClient = logClient,
                    )
                )
            every { eventProcessor.hasAbis() } returns false

            indexerWithoutTransfers.publicSetCurrentBlockNumber(0L)
            indexerWithoutTransfers.publicSync(BlockIdentifier(10L, "0x10"))

            coVerify(exactly = 0) { logClient.fetchTransfers(any(), any(), any(), any()) }
        }

        @Test
        fun `should include VET transfers when excludeVetTransfers is false`() = runBlocking {
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 9L, 100L, null) }
        }

        @Test
        fun `should skip processing when both eventLogs and transferLogs are empty`() =
            runBlocking {
                every { eventProcessor.hasAbis() } returns true
                coEvery { logClient.fetchEventLogs(0L, 9L, 100L, null) } returns emptyList()
                coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns emptyList()

                indexer.publicSetCurrentBlockNumber(0L)
                indexer.publicSync(BlockIdentifier(10L, "0x10"))

                verify(exactly = 0) { processor.process(any()) }
                coVerify(exactly = 0) { eventProcessor.processEvents(any(), any()) }
                expect { that(indexer.getCurrentBlockNumber()).isEqualTo(10L) }
            }

        @Test
        fun `should update current block number after processing`() = runBlocking {
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            expect { that(indexer.getCurrentBlockNumber()).isEqualTo(10L) }
        }

        @Test
        fun `should use blockBatchSize to determine batch end block`() = runBlocking {
            val indexerWithLargeBatch =
                spyk(
                    TestableLogsIndexer(
                        name = "TestLogsIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        syncLoggerInterval = 1L,
                        excludeVetTransfers = false,
                        blockBatchSize = 50L,
                        logFetchLimit = 100L,
                        eventCriteriaSet = null,
                        transferCriteriaSet = null,
                        eventProcessor = eventProcessor,
                        pruner = null,
                        prunerInterval = 10000L,
                        mockLogClient = logClient,
                    )
                )
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(0L, 49L, 100L, null) } returns emptyList()

            indexerWithLargeBatch.publicSetCurrentBlockNumber(0L)
            indexerWithLargeBatch.publicSync(BlockIdentifier(100L, "0x100"))

            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 49L, 100L, null) }
        }

        @Test
        fun `should not exceed toBlock when calculating batch end`() = runBlocking {
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(0L, 7L, 100L, null) } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(7L, "0x7"))

            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 7L, 100L, null) }
            expect { that(indexer.getCurrentBlockNumber()).isEqualTo(8L) }
        }

        @Test
        fun `should process indexed events when they are not empty`() = runBlocking {
            val eventLogs = EventLogFixtures.LOGS_STRINGS
            val transferLogs = TransferLogFixtures.LOGS_VET_TRANSFER
            val indexedEvents = listOf(IndexedEventFixture.create(), IndexedEventFixture.create())

            every { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 9L, 100L, null) } returns eventLogs
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns transferLogs
            every {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            } returns indexedEvents
            every { processor.process(any()) } just Runs

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            verify(exactly = 1) {
                processor.process(
                    match {
                        it is IndexingResult.EventsOnly &&
                            it.endBlock == 9L &&
                            it.events == indexedEvents
                    }
                )
            }
        }

        @Test
        fun `should not process when indexed events are empty`() = runBlocking {
            val eventLogs = listOf<EventLog>()
            val transferLogs = listOf<TransferLog>()

            every { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 9L, 100L, null) } returns eventLogs
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns transferLogs
            every {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(10L, "0x10"))

            verify(exactly = 0) { processor.process(any()) }
        }

        @Test
        fun `should process multiple batches`() = runBlocking {
            every { eventProcessor.hasAbis() } returns false
            coEvery { logClient.fetchTransfers(0L, 9L, 100L, null) } returns emptyList()
            coEvery { logClient.fetchTransfers(10L, 19L, 100L, null) } returns emptyList()
            coEvery { logClient.fetchTransfers(20L, 26L, 100L, null) } returns emptyList()

            indexer.publicSetCurrentBlockNumber(0L)
            indexer.publicSync(BlockIdentifier(26L, "0x26"))

            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 9L, 100L, null) }
            coVerify(exactly = 1) { logClient.fetchTransfers(10L, 19L, 100L, null) }
            coVerify(exactly = 1) { logClient.fetchTransfers(20L, 26L, 100L, null) }
            expect { that(indexer.getCurrentBlockNumber()).isEqualTo(27L) }
        }
    }
}
