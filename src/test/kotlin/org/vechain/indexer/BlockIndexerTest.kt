package org.vechain.indexer

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.fixtures.IndexedEventFixture
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan

internal class TestableBlockIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    syncLoggerInterval: Long,
    eventProcessor: CombinedEventProcessor?,
    inspectionClauses: List<Clause>?,
    pruner: Pruner?,
    prunerInterval: Long,
    dependsOn: Indexer?,
) :
    BlockIndexer(
        name = name,
        thorClient = thorClient,
        processor = processor,
        startBlock = startBlock,
        syncLoggerInterval = syncLoggerInterval,
        eventProcessor = eventProcessor,
        inspectionClauses = inspectionClauses,
        pruner = pruner,
        prunerInterval = prunerInterval,
        dependsOn = dependsOn,
    ) {
    suspend fun publicBuildIndexingResult(block: Block): IndexingResult {
        return super.buildIndexingResult(block)
    }

    fun publicUpdateSyncStatus(block: Block) {
        super.updateSyncStatus(block)
    }

    fun publicRunPruner() {
        super.runPruner()
    }

    fun publicSetStatus(newStatus: Status) {
        super.setStatus(newStatus)
    }

    fun publicSetCurrentBlockNumber(value: Long) {
        super.setCurrentBlockNumber(value)
    }
}

@ExtendWith(MockKExtension::class)
internal class BlockIndexerTest {
    @MockK private lateinit var processor: IndexerProcessor

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var eventProcessor: CombinedEventProcessor

    @MockK private lateinit var pruner: Pruner

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
        every { processor.rollback(any()) } just Runs
    }

    @Nested
    inner class Initialisation {
        private lateinit var indexer: BlockIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                BlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 10000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )
        }

        @Test
        fun `Should initialise with rolling back last synced block if no start block provided`() =
            runBlocking {
                every { processor.getLastSyncedBlock() } returns
                    BlockIdentifier(number = 100L, id = "0x100") andThen
                    BlockIdentifier(number = 99L, id = "0x99")

                indexer.initialise()

                // Verify the rollback is performed once
                verify(exactly = 1) { processor.rollback(100L) }
                verify(exactly = 2) { processor.getLastSyncedBlock() }

                expect {
                    // Verify the status is SYNCING
                    that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                    // previousBlock should equal the second last synced block returned
                    that(indexer.getPreviousBlock())
                        .isEqualTo(BlockIdentifier(number = 99L, id = "0x99"))
                    // currentBlockNumber should equal the last synced block number
                    that(indexer.getCurrentBlockNumber()).isEqualTo(100L)
                }
            }

        @Test
        fun `Should initialise with startBlock when no last synced block `() = runBlocking {
            every { processor.getLastSyncedBlock() } returns null

            indexer.initialise()

            verify(exactly = 2) {
                processor.getLastSyncedBlock()
            } // Verify the rollback is performed once
            verify(exactly = 1) { processor.rollback(0L) }

            expect {
                // Verify the status is SYNCING
                that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                // getLastSyncedBlock should be called once
                // previousBlock should equal null when no last synced block found
                that(indexer.getPreviousBlock()).isEqualTo(null)
                // currentBlockNumber should equal the start block when no last synced block found
                that(indexer.getCurrentBlockNumber()).isEqualTo(0L)
            }
        }

        @Test
        fun `Previous block should be null if last synced block doesn't match current block`() {
            every { processor.getLastSyncedBlock() } returns
                BlockIdentifier(number = 100L, id = "0x100") andThen
                BlockIdentifier(number = 98L, id = "0x98")

            indexer.initialise()

            expect {
                // Verify the status is SYNCING
                that(indexer.getStatus()).isEqualTo(Status.INITIALISED)
                // previousBlock should equal null when last synced block number doesn't match
                // current block number - 1
                that(indexer.getPreviousBlock()).isEqualTo(null)
                // currentBlockNumber should equal the last synced block number
                that(indexer.getCurrentBlockNumber()).isEqualTo(100L)
            }
        }
    }

    @Nested
    inner class BuildIndexingResults {

        @Test
        fun `Should call to inspectClauses when inspection clauses provided`() {
            runBlocking {
                val block = buildBlock(num = 1L)
                val clauses = listOf(Clause(to = "0xabc", value = "abi", data = "0xdata"))
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        eventProcessor = null,
                        pruner = null,
                        prunerInterval = 10000L,
                        syncLoggerInterval = 1L,
                        inspectionClauses = clauses,
                        dependsOn = null,
                    )

                coEvery { thorClient.inspectClauses(clauses, block.id) } returns emptyList()

                indexer.publicBuildIndexingResult(block)

                coVerify(exactly = 1) { thorClient.inspectClauses(clauses, block.id) }
            }
        }

        @Test
        fun `Should not call to inspectClauses when no inspection clauses provided`() {
            runBlocking {
                val block = buildBlock(num = 1L)
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        eventProcessor = null,
                        pruner = null,
                        prunerInterval = 10000L,
                        syncLoggerInterval = 1L,
                        inspectionClauses = null,
                        dependsOn = null,
                    )

                coEvery { thorClient.inspectClauses(any(), any()) } returns emptyList()

                indexer.publicBuildIndexingResult(block)

                coVerify(exactly = 0) { thorClient.inspectClauses(any(), any()) }
            }
        }

        @Test
        fun `Should include inspected events in indexing result`() {
            runBlocking {
                val block = buildBlock(num = 1L)
                val clauses = listOf(Clause(to = "0xabc", value = "abi", data = "0xdata"))
                val callResults =
                    listOf(
                        InspectionResult(
                            data = "0xdata",
                            events = emptyList(),
                            transfers = emptyList(),
                            reverted = false,
                            vmError = null,
                        )
                    )
                val expectedResult =
                    IndexingResult.Normal(
                        block = block,
                        events = emptyList(),
                        callResults = callResults,
                    )
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        eventProcessor = null,
                        pruner = null,
                        prunerInterval = 10000L,
                        syncLoggerInterval = 1L,
                        inspectionClauses = clauses,
                        dependsOn = null,
                    )

                coEvery { thorClient.inspectClauses(clauses, block.id) } returns callResults

                val result = indexer.publicBuildIndexingResult(block)

                expect { that(result).isEqualTo(expectedResult) }
            }
        }

        @Test
        fun `Should call to processEvents when event processor provided`() {
            runBlocking {
                val block = buildBlock(num = 1L)
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        eventProcessor = eventProcessor,
                        pruner = null,
                        prunerInterval = 10000L,
                        syncLoggerInterval = 1L,
                        inspectionClauses = null,
                        dependsOn = null,
                    )
                val indexedEvents = listOf(IndexedEventFixture.create())

                coEvery { eventProcessor.processEvents(block) } returns indexedEvents

                val result = indexer.publicBuildIndexingResult(block)

                coVerify(exactly = 1) { eventProcessor.processEvents(block) }

                expect {
                    that(result.events()).isEqualTo(indexedEvents)
                    that((result as IndexingResult.Normal).block).isEqualTo(block)
                }
            }
        }

        @Test
        fun `Should do both`() {
            runBlocking {
                val block = buildBlock(num = 1L)
                val clauses = listOf(Clause(to = "0xabc", value = "abi", data = "0xdata"))
                val callResults =
                    listOf(
                        InspectionResult(
                            data = "0xdata",
                            events = emptyList(),
                            transfers = emptyList(),
                            reverted = false,
                            vmError = null,
                        )
                    )
                val indexedEvents = listOf(IndexedEventFixture.create())
                val expectedResult =
                    IndexingResult.Normal(
                        block = block,
                        events = indexedEvents,
                        callResults = callResults,
                    )
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        startBlock = 0L,
                        eventProcessor = eventProcessor,
                        pruner = null,
                        prunerInterval = 10000L,
                        syncLoggerInterval = 1L,
                        inspectionClauses = clauses,
                        dependsOn = null,
                    )

                coEvery { thorClient.inspectClauses(clauses, block.id) } returns callResults
                coEvery { eventProcessor.processEvents(block) } returns indexedEvents

                val result = indexer.publicBuildIndexingResult(block)

                coVerify(exactly = 1) { thorClient.inspectClauses(clauses, block.id) }
                coVerify(exactly = 1) { eventProcessor.processEvents(block) }

                expect { that(result).isEqualTo(expectedResult) }
            }
        }
    }

    @Nested
    inner class UpdateSyncStatus {
        @Test
        fun `Should become fully synced if block is within the last 15 seconds`() {
            val currTime = LocalDateTime.now(ZoneOffset.UTC).toEpochSecond(ZoneOffset.UTC)
            val block = buildBlock(num = 123L, timestamp = currTime)
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 123L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 10000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            indexer.publicUpdateSyncStatus(block)

            expect { that(indexer.getStatus()).isEqualTo(Status.FULLY_SYNCED) }
        }

        @Test
        fun `Should be SYNCING if block is older than 15 seconds`() {
            val currTime = LocalDateTime.now(ZoneOffset.UTC).toEpochSecond(ZoneOffset.UTC)
            val block = buildBlock(num = 123L, timestamp = currTime - 16L)
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 123L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 10000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            indexer.publicUpdateSyncStatus(block)

            expect { that(indexer.getStatus()).isEqualTo(Status.SYNCING) }
        }
    }

    @Nested
    inner class RunPruner {
        @Test
        fun `Should only run pruner if fully synced`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    eventProcessor = null,
                    pruner = pruner,
                    prunerInterval = 1L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            every { pruner.run(any()) } just Runs

            // Not fully synced, should not run pruner
            indexer.publicRunPruner()
            verify(exactly = 0) { pruner.run(any()) }

            // Manually set status to SYNCING, should not run pruner
            indexer.publicSetStatus(Status.SYNCING)
            indexer.publicRunPruner()
            verify(exactly = 0) { pruner.run(any()) }

            // Manually set status to NOT_INITIALISED, should not run pruner
            indexer.publicSetStatus(Status.NOT_INITIALISED)
            indexer.publicRunPruner()
            verify(exactly = 0) { pruner.run(any()) }

            // Manually set status to INITIALISED, should not run pruner
            indexer.publicSetStatus(Status.INITIALISED)
            indexer.publicRunPruner()
            verify(exactly = 0) { pruner.run(any()) }

            // Manually set status to PRUNING, should not run pruner
            indexer.publicSetStatus(Status.PRUNING)
            indexer.publicRunPruner()
            verify(exactly = 0) { pruner.run(any()) }

            // Manually set status to FULLY_SYNCED, should run pruner
            indexer.publicSetStatus(Status.FULLY_SYNCED)
            indexer.publicRunPruner()
            verify(exactly = 1) { pruner.run(any()) }
        }
    }

    @Nested
    inner class ProcessBlock {
        @Test
        fun `should throw when status is not allowed`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 1000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )
            val block = buildBlock(num = 0L)

            val exception =
                assertThrows<IllegalStateException> { runBlocking { indexer.processBlock(block) } }

            val errorMessage: String? = exception.message

            expectThat(errorMessage).isEqualTo("Invalid status: ${Status.NOT_INITIALISED}")
        }

        @Test
        fun `should throw when block number mismatch occurs`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 1000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            indexer.publicSetStatus(Status.INITIALISED)
            indexer.publicSetCurrentBlockNumber(1L)
            val block = buildBlock(num = 0L)

            assertThrows<IllegalStateException> { runBlocking { indexer.processBlock(block) } }
        }

        @Test
        fun `should process block and update state`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 1000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            val block = buildBlock(num = 0L)
            indexer.publicSetStatus(Status.INITIALISED)
            indexer.publicSetCurrentBlockNumber(block.number)
            val previousTime = indexer.timeLastProcessed

            every { processor.process(any()) } just Runs

            runBlocking { indexer.processBlock(block) }

            verify(exactly = 1) {
                processor.process(
                    match {
                        it is IndexingResult.Normal &&
                            it.block == block &&
                            it.events.isEmpty() &&
                            it.callResults.isEmpty()
                    },
                )
            }

            expect {
                that(indexer.getStatus()).isEqualTo(Status.SYNCING)
                that(indexer.getPreviousBlock())
                    .isEqualTo(BlockIdentifier(number = block.number, id = block.id))
                that(indexer.getCurrentBlockNumber()).isEqualTo(block.number + 1)
                that(indexer.timeLastProcessed).isGreaterThan(previousTime)
            }
        }

        @Test
        fun `if shut down throw`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    eventProcessor = null,
                    pruner = null,
                    prunerInterval = 1000L,
                    syncLoggerInterval = 1L,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            val block = buildBlock(num = 0L)
            indexer.publicSetStatus(Status.SHUT_DOWN)
            indexer.publicSetCurrentBlockNumber(block.number)

            val exception =
                assertThrows<CancellationException> { runBlocking { indexer.processBlock(block) } }

            val errorMessage: String? = exception.message

            expectThat(errorMessage).isEqualTo("Indexer is shut down")
        }
    }
}
