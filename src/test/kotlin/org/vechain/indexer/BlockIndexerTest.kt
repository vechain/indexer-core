package org.vechain.indexer

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.fixtures.IndexedEventFixture
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult
import org.vechain.indexer.thor.model.TxEvent
import org.vechain.indexer.thor.model.TxTransfer
import strikt.api.expect
import strikt.assertions.isEqualTo
import kotlin.String

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
) : BlockIndexer(
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
}

@ExtendWith(MockKExtension::class)
internal class BlockIndexerTest {
    @MockK
    private lateinit var processor: IndexerProcessor

    @MockK
    private lateinit var thorClient: ThorClient

    @MockK
    private lateinit var eventProcessor: CombinedEventProcessor

    @MockK
    private lateinit var pruner: Pruner

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

            verify(exactly = 2) { processor.getLastSyncedBlock() }// Verify the rollback is performed once
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
                val callResults = listOf(InspectionResult(
                    data = "0xdata",
                    events = emptyList(),
                    transfers = emptyList(),
                    reverted = false,
                    vmError = null,
                ))
                val expectedResult = IndexingResult.Normal(
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

                expect {
                    that(result).isEqualTo(expectedResult)
                }
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
                val indexedEvents = listOf(
                    IndexedEventFixture.create()
                )

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
                val callResults = listOf(InspectionResult(
                    data = "0xdata",
                    events = emptyList(),
                    transfers = emptyList(),
                    reverted = false,
                    vmError = null,
                ))
                val indexedEvents = listOf(
                    IndexedEventFixture.create()
                )
                val expectedResult = IndexingResult.Normal(
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

                expect {
                    that(result).isEqualTo(expectedResult)
                }
            }
        }
    }

    @Nested
    inner class EnsureStatus {

    }
}
