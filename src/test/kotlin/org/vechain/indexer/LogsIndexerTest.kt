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
import strikt.assertions.isGreaterThan
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

    suspend fun publicProcessBatch(toBlockNumber: Long) {
        super.processBatch(toBlockNumber)
    }

    fun publicCalculateBatchEndBlock(toBlockNumber: Long): Long {
        return super.calculateBatchEndBlock(toBlockNumber)
    }

    fun publicHasNoLogs(eventLogs: List<EventLog>, transferLogs: List<TransferLog>): Boolean {
        return super.hasNoLogs(eventLogs, transferLogs)
    }

    suspend fun publicFetchEventLogsIfNeeded(batchEndBlock: Long): List<EventLog> {
        return super.fetchEventLogsIfNeeded(batchEndBlock)
    }

    fun publicShouldFetchEventLogs(): Boolean {
        return super.shouldFetchEventLogs()
    }

    suspend fun publicFetchTransferLogsIfNeeded(batchEndBlock: Long): List<TransferLog> {
        return super.fetchTransferLogsIfNeeded(batchEndBlock)
    }

    fun publicShouldFetchTransferLogs(): Boolean {
        return super.shouldFetchTransferLogs()
    }

    suspend fun publicProcessAndIndexEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>,
        batchEndBlock: Long
    ) {
        super.processAndIndexEvents(eventLogs, transferLogs, batchEndBlock)
    }

    fun publicUpdateBlockNumberAndTime(batchEndBlock: Long) {
        super.updateBlockNumberAndTime(batchEndBlock)
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

    @Nested
    inner class HelperMethods {
        private lateinit var indexer: TestableLogsIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
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
        }

        @Test
        fun `calculateBatchEndBlock should return minimum of batch size and target`() {
            indexer.publicSetCurrentBlockNumber(0L)

            val result1 = indexer.publicCalculateBatchEndBlock(100L)
            expect { that(result1).isEqualTo(9L) } // 0 + 10 - 1 = 9

            indexer.publicSetCurrentBlockNumber(50L)
            val result2 = indexer.publicCalculateBatchEndBlock(55L)
            expect { that(result2).isEqualTo(55L) } // min(59, 55) = 55
        }

        @Test
        fun `hasNoLogs should return true when both lists are empty`() {
            val result1 = indexer.publicHasNoLogs(emptyList(), emptyList())
            expect { that(result1).isEqualTo(true) }

            val result2 = indexer.publicHasNoLogs(EventLogFixtures.LOGS_STRINGS, emptyList())
            expect { that(result2).isEqualTo(false) }

            val result3 =
                indexer.publicHasNoLogs(emptyList(), TransferLogFixtures.LOGS_VET_TRANSFER)
            expect { that(result3).isEqualTo(false) }

            val result4 =
                indexer.publicHasNoLogs(
                    EventLogFixtures.LOGS_STRINGS,
                    TransferLogFixtures.LOGS_VET_TRANSFER
                )
            expect { that(result4).isEqualTo(false) }
        }

        @Test
        fun `shouldFetchEventLogs should return true when eventProcessor has ABIs`() {
            every { eventProcessor.hasAbis() } returns true
            expect { that(indexer.publicShouldFetchEventLogs()).isEqualTo(true) }

            every { eventProcessor.hasAbis() } returns false
            expect { that(indexer.publicShouldFetchEventLogs()).isEqualTo(false) }
        }

        @Test
        fun `shouldFetchTransferLogs should return true when not excluded`() = runBlocking {
            val indexerWithTransfers =
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
            expect { that(indexerWithTransfers.publicShouldFetchTransferLogs()).isEqualTo(true) }

            val indexerWithoutTransfers =
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
            expect {
                that(indexerWithoutTransfers.publicShouldFetchTransferLogs()).isEqualTo(false)
            }
        }

        @Test
        fun `fetchEventLogsIfNeeded should fetch when ABIs are configured`() = runBlocking {
            val eventLogs = EventLogFixtures.LOGS_STRINGS
            every { eventProcessor.hasAbis() } returns true
            coEvery { logClient.fetchEventLogs(0L, 10L, 100L, null) } returns eventLogs

            indexer.publicSetCurrentBlockNumber(0L)
            val result = indexer.publicFetchEventLogsIfNeeded(10L)

            expect { that(result).isEqualTo(eventLogs) }
            coVerify(exactly = 1) { logClient.fetchEventLogs(0L, 10L, 100L, null) }
        }

        @Test
        fun `fetchEventLogsIfNeeded should return empty list when no ABIs`() = runBlocking {
            every { eventProcessor.hasAbis() } returns false

            indexer.publicSetCurrentBlockNumber(0L)
            val result = indexer.publicFetchEventLogsIfNeeded(10L)

            expect { that(result).isEqualTo(emptyList()) }
            coVerify(exactly = 0) { logClient.fetchEventLogs(any(), any(), any(), any()) }
        }

        @Test
        fun `fetchTransferLogsIfNeeded should fetch when not excluded`() = runBlocking {
            val transferLogs = TransferLogFixtures.LOGS_VET_TRANSFER
            coEvery { logClient.fetchTransfers(0L, 10L, 100L, null) } returns transferLogs

            indexer.publicSetCurrentBlockNumber(0L)
            val result = indexer.publicFetchTransferLogsIfNeeded(10L)

            expect { that(result).isEqualTo(transferLogs) }
            coVerify(exactly = 1) { logClient.fetchTransfers(0L, 10L, 100L, null) }
        }

        @Test
        fun `fetchTransferLogsIfNeeded should return empty list when excluded`() = runBlocking {
            val indexerWithoutTransfers =
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

            indexerWithoutTransfers.publicSetCurrentBlockNumber(0L)
            val result = indexerWithoutTransfers.publicFetchTransferLogsIfNeeded(10L)

            expect { that(result).isEqualTo(emptyList()) }
            coVerify(exactly = 0) { logClient.fetchTransfers(any(), any(), any(), any()) }
        }

        @Test
        fun `processAndIndexEvents should process when events are not empty`() = runBlocking {
            val eventLogs = EventLogFixtures.LOGS_STRINGS
            val transferLogs = TransferLogFixtures.LOGS_VET_TRANSFER
            val indexedEvents = listOf(IndexedEventFixture.create())

            every {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            } returns indexedEvents
            every { processor.process(any()) } just Runs

            indexer.publicProcessAndIndexEvents(eventLogs, transferLogs, 10L)

            verify(exactly = 1) {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            }
            verify(exactly = 1) {
                processor.process(match { it is IndexingResult.EventsOnly && it.endBlock == 10L })
            }
        }

        @Test
        fun `processAndIndexEvents should not process when events are empty`() = runBlocking {
            val eventLogs = listOf<EventLog>()
            val transferLogs = listOf<TransferLog>()

            every {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            } returns emptyList()

            indexer.publicProcessAndIndexEvents(eventLogs, transferLogs, 10L)

            verify(exactly = 1) {
                eventProcessor.processEvents(any<List<EventLog>>(), any<List<TransferLog>>())
            }
            verify(exactly = 0) { processor.process(any()) }
        }

        @Test
        fun `updateBlockNumberAndTime should increment block number`() {
            indexer.publicSetCurrentBlockNumber(10L)
            val timeBefore = indexer.timeLastProcessed

            indexer.publicUpdateBlockNumberAndTime(19L)

            expect {
                that(indexer.getCurrentBlockNumber()).isEqualTo(20L)
                that(indexer.timeLastProcessed).isGreaterThan(timeBefore)
            }
        }
    }
}
