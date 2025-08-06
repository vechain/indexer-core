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
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.fixtures.IndexedEventFixture.create
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.message

@ExtendWith(MockKExtension::class)
internal class BlockIndexerTest {
    @MockK private lateinit var processor: IndexerProcessor

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var eventProcessor: CombinedEventProcessor

    @MockK private lateinit var pruner: Pruner

    private val getBlockNumberSlot = slot<Long>()
    private val processBlockSlot = slot<Block>()
    private val matchedEventsSlot = slot<List<IndexedEvent>>()

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
        every { processor.rollback(any()) } just Runs
    }

    @Nested
    inner class Start {
        private lateinit var indexer: TestableBlockIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                )
        }

        @Test
        fun `Start indexer should initialise with rolling back last synced block`() = runBlocking {
            val indexerIterationsNumber = 1L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            every { processor.getLastSyncedBlock() } returns
                BlockIdentifier(number = 100L, id = "0x100") andThen
                BlockIdentifier(number = 99L, id = "0x99")
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the rollback is performed once
                verify(exactly = 1) { processor.rollback(100L) }
            }
        }

        @Test
        fun `Start indexer should initialise with rolling back the startBlock when no last synced block found`() =
            runBlocking {
                val indexerIterationsNumber = 1L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
                every { processor.getLastSyncedBlock() } returns null
                every { processor.process(any(), any()) } just Runs

                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join()

                expect {
                    // Verify the status is SYNCING
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Verify the rollback is performed once
                    verify(exactly = 1) { processor.rollback(0L) }
                }
            }

        @Test
        fun `Start indexer should process blocks - one block`() = runBlocking {
            val indexerIterationsNumber = 1L
            val block = buildBlock(0L)

            coEvery { thorClient.getBlock(0L) } coAnswers { block }
            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(emptyList(), block) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the correct number of processing of blocks
                verify(exactly = 1) { processor.rollback(0L) }
                verify(exactly = 1) { processor.process(emptyList(), block) }
            }
        }

        @Test
        fun `Start indexer should process blocks - multiple blocks`() = runBlocking {
            val indexerIterationsNumber = 100L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the correct number of processing of blocks
                verify(exactly = indexerIterationsNumber.toInt()) {
                    processor.process(any(), any())
                }
            }
        }

        @Test
        fun `Start indexer should perform post process blocks`() = runBlocking {
            val indexerIterationsNumber = 1L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify post-processing increments the current block number
                that(indexer.currentBlockNumber).isEqualTo(indexerIterationsNumber)
            }
        }
    }

    @Nested
    inner class StartInCoroutine {
        private lateinit var indexer: TestableBlockIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                )
        }

        @Test
        fun `Start indexer should run in a coroutine`() = runBlocking {
            val indexerIterationsNumber = 1L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the correct number of processing of blocks
                verify(exactly = 1) { processor.process(any(), any()) }
            }
        }
    }

    @Nested
    inner class IndexerRestart {
        private lateinit var indexer: TestableBlockIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                )
        }

        @Test
        fun `Indexer should restart at current block when unknown exception is thrown`() =
            runBlocking {
                val finalBlock = BlockIdentifier(number = 99L, id = "0x99")
                val errorBlockNumber = 100L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
                every { processor.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    finalBlock
                var calledAlready = false
                every { processor.process(any(), capture(processBlockSlot)) } answers
                    {
                        if (
                            !calledAlready && processBlockSlot.captured.number == errorBlockNumber
                        ) {
                            calledAlready = true
                            throw Exception("Unknown exception")
                        }
                    }

                // Run the indexer for another two iterations after the error block
                val job = launch { indexer.start(errorBlockNumber + 2) }
                job.join()

                expect {
                    // Indexer should have advanced processing after successfully restarting
                    // processing of faulty block
                    that(indexer.currentBlockNumber).isEqualTo(errorBlockNumber + 1)
                    // Indexer should switch back to SYNCING status error detection
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Indexer should restart & rollback processing at the error block number
                    verify(exactly = 1) { processor.rollback(errorBlockNumber) }
                }
            }

        @Test
        fun `Indexer should restart at current block when thor node rate limit is hit`() =
            runBlocking {
                val finalBlock = BlockIdentifier(number = 99L, id = "0x99")
                val tooManyRequestsBlockNumber = 1L

                var rateLimitedAlready = false
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        if (
                            !rateLimitedAlready &&
                                getBlockNumberSlot.captured == tooManyRequestsBlockNumber
                        ) {
                            rateLimitedAlready = true
                            throw Exception("Too Many Requests")
                        }
                        buildBlock(getBlockNumberSlot.captured)
                    }
                every { processor.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    finalBlock

                every { processor.process(any(), any()) } just Runs

                // Run the indexer for another two iterations after the rate limited block number
                val job = launch { indexer.start(tooManyRequestsBlockNumber + 2) }
                job.join()

                expect {
                    // Indexer should have advanced processing after successfully restarting
                    // processing of faulty block
                    that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber + 1)
                    // Indexer should switch back to SYNCING status error detection
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Indexer should restart & rollback processing at the error block number
                    verify(exactly = 1) { processor.rollback(tooManyRequestsBlockNumber) }
                }
            }

        @Test
        fun `Indexer should restart at block previous to current block when a re-organization is detected`() =
            runBlocking {
                val finalBlock = BlockIdentifier(number = 98L, id = "0x98")
                val reorgBlockNumber = 100L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        // At block 100, the parent id is invalid
                        val parentId =
                            if (getBlockNumberSlot.captured == reorgBlockNumber) {
                                "0x02321321"
                            } else {
                                "0x${maxOf(getBlockNumberSlot.captured - 1, 0)}"
                            }
                        buildBlock(getBlockNumberSlot.captured, parentId)
                    }

                every { processor.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    finalBlock
                every { processor.process(any(), any()) } just Runs

                // Run indexer for a few iterations more after re-organization detected
                val job = launch { indexer.start(reorgBlockNumber + 2) }
                job.join()

                expect {
                    // Indexer should have advanced processing after successfully restarting
                    // processing of faulty block
                    that(indexer.currentBlockNumber).isEqualTo(reorgBlockNumber)
                    // Indexer should switch back to SYNCING status after re-organization detection
                    expectThat(indexer.status).isEqualTo(Status.SYNCING)
                    // The re-organization at block reorgBlockNumber should trigger a rollback of
                    // block
                    // (reorgBlockNumber - 1) data
                    verify(exactly = 1) { processor.rollback(reorgBlockNumber - 1) }
                }
            }
    }

    @Nested
    inner class IndexerStatus {

        private lateinit var indexer: TestableBlockIndexer

        @BeforeEach
        fun setupIndexer() {
            indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                )
        }

        @Test
        fun `Indexer starting & processing block is at the SYNCING status`() = runBlocking {
            val iterations = 100L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(iterations) }
            job.join()

            expect {
                // Current block should correspond to number of iterations of indexer run
                that(indexer.currentBlockNumber).isEqualTo(iterations)
                // Status should be SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // First initialise should roll back to start block
                verify(exactly = 1) { processor.rollback(0L) }
                // Number of processed blocks should correspond to current block number
                verify(exactly = indexer.currentBlockNumber.toInt()) {
                    processor.process(any(), any())
                }
            }
        }

        @Test
        fun `Indexer should switch to FULLY_SYNCED status when block not found`() = runBlocking {
            val blockNotFound = BlockIdentifier(number = 99L, id = "0x99")

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    if (getBlockNumberSlot.captured >= blockNotFound.number) {
                        throw BlockNotFoundException("Block not found")
                    }
                    buildBlock(getBlockNumberSlot.captured)
                }
            coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(blockNotFound.number) }
            every { processor.getLastSyncedBlock() } returns null andThen null andThen blockNotFound
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(blockNotFound.number + 1) }
            job.join()

            expect {
                // The current block remains at the one not found
                that(indexer.currentBlockNumber).isEqualTo(blockNotFound.number)
                // Status should switch to FULLY_SYNCED
                that(indexer.status).isEqualTo(Status.FULLY_SYNCED)
            }
        }

        @Test
        fun `Indexer should ensure whether it is FULLY_SYNCED and switch back to SYNCING`() =
            runBlocking {
                val iterations = 101L
                val blockNotFound = BlockIdentifier(number = 99L, id = "0x99")

                // Block is not found the first time indexer tries to fetch it
                var calledAlready = false
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        if (!calledAlready && getBlockNumberSlot.captured == blockNotFound.number) {
                            calledAlready = true
                            throw BlockNotFoundException("Block not found")
                        }
                        buildBlock(getBlockNumberSlot.captured)
                    }
                // Simulate a gap between last synced and current best block from chain
                coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(iterations) }
                every { processor.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    blockNotFound
                every { processor.process(any(), any()) } just Runs

                // Iterations + (1 iteration) where block is not found to trigger the FULLY_SYNCED
                // status
                val job = launch { indexer.start(iterations + 1) }
                job.join()

                expect {
                    // Current block number should match the number of iterations after we run
                    // indexer (iterations + 1) number of times
                    that(indexer.currentBlockNumber).isEqualTo(iterations)
                    // Status should switch back to syncing after we detect indexer is behind best
                    // on-chain block
                    that(indexer.status).isEqualTo(Status.SYNCING)
                }
            }

        @Test
        fun `Indexer should switch to REORG status upon chain re-organization`() = runBlocking {
            val reorgBlock = 100L
            val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

            // Simulate re-organization by detecting invalid parent block id at reorgBlock
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    // At reorgBlock, the parent id is invalid
                    val parentId =
                        if (getBlockNumberSlot.captured == reorgBlock) {
                            "0x02321321"
                        } else {
                            "0x${maxOf(getBlockNumberSlot.captured - 1, 0)}"
                        }
                    buildBlock(getBlockNumberSlot.captured, parentId)
                }

            every { processor.getLastSyncedBlock() } returns
                null andThen
                null andThen
                lastSyncedBlock
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(reorgBlock + 1) }
            job.join()

            expect {
                // The current block number should match the re-organization block
                that(indexer.currentBlockNumber).isEqualTo(reorgBlock)
                // The indexer status should switch to REORG
                that(indexer.status).isEqualTo(Status.REORG)
            }
        }

        @Test
        fun `Indexer should not trigger a REORG when previous block is null`() = runBlocking {
            // Simulate re-organization by detecting invalid parent block id at reorgBlock
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }

            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(1L) }
            job.join()

            expect {
                // The current block number should match the re-organization block
                that(indexer.currentBlockNumber).isEqualTo(1L)
                // The indexer status should remain SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
            }
        }

        @Test
        fun `Indexer should switch to ERROR status upon unknown exception thrown`() = runBlocking {
            val unknownExceptionBlock = 100L
            val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            every { processor.getLastSyncedBlock() } returns
                null andThen
                null andThen
                lastSyncedBlock
            // Exception is thrown when processing block unknownExceptionBlock
            every { processor.process(any(), capture(processBlockSlot)) } answers
                {
                    if (processBlockSlot.captured.number == unknownExceptionBlock) {
                        throw Exception("Unknown exception")
                    }
                }

            val job = launch { indexer.start(unknownExceptionBlock + 1) }
            job.join()

            expect {
                // The current block number should match the exception block
                that(indexer.currentBlockNumber).isEqualTo(unknownExceptionBlock)
                // The indexer status should switch to ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
            }
        }

        @Test
        fun `Indexer should switch to ERROR status upon rate limit exception when fetching block`() =
            runBlocking {
                val tooManyRequestsBlockNumber = 100L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

                // Exception is thrown when attempting to fetch block tooManyRequestsBlockNumber
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        if (getBlockNumberSlot.captured != tooManyRequestsBlockNumber) {
                            buildBlock(getBlockNumberSlot.captured)
                        } else {
                            throw Exception("Too Many Requests")
                        }
                    }

                every { processor.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    lastSyncedBlock
                every { processor.process(any(), capture(processBlockSlot)) } just Runs

                val job = launch { indexer.start(tooManyRequestsBlockNumber + 1) }
                job.join()

                expect {
                    // The current block number should match the exception block
                    that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber)
                    // The indexer status should switch to ERROR
                    that(indexer.status).isEqualTo(Status.ERROR)
                }
            }
    }

    @Nested
    inner class ProcessEvents {
        @Test
        fun `should call processEvents when eventProcessor is not null and pass the result to the process function`() =
            runBlocking {
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        thorClient = thorClient,
                        processor = processor,
                        eventProcessor = eventProcessor,
                        startBlock = 0L,
                    )

                val block = buildBlock(0L)
                val matchedEvents = listOf(create(id = "test1"))

                coEvery { thorClient.getBlock(0L) } coAnswers { block }
                every { processor.getLastSyncedBlock() } returns null
                every { eventProcessor.processEvents(block) } returns matchedEvents
                every { processor.process(matchedEvents, block) } just Runs

                val job = launch { indexer.start(1) }
                job.join()

                expect {
                    // Verify the status is SYNCING
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Verify processEvents was called with the correct block
                    verify(exactly = 1) { eventProcessor.processEvents(block) }
                    // Verify processor.process was called with the matched events and block
                    verify(exactly = 1) { processor.process(matchedEvents, block) }
                }
            }

        @Test
        fun `Should move to ERROR status if processEvents throws an exception`() = runBlocking {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    eventProcessor = eventProcessor,
                    startBlock = 0L,
                )

            val block = buildBlock(0L)

            coEvery { thorClient.getBlock(0L) } coAnswers { block }
            every { processor.getLastSyncedBlock() } returns null
            every { eventProcessor.processEvents(block) } throws Exception("Processing error")

            val job = launch { indexer.start(1) }
            job.join()

            expect {
                // Verify the status is ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
                // Verify processEvents was called with the correct block
                verify(exactly = 1) { eventProcessor.processEvents(block) }
            }
        }
    }

    @Nested
    inner class PrunerTest {
        @Test
        fun `pruner should run at the specified interval`() = runBlocking {
            val prunerInterval = 2L
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    pruner = pruner,
                    prunerInterval = prunerInterval,
                )

            var blockNotFoundThrown = false
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    if (!blockNotFoundThrown && getBlockNumberSlot.captured == 1L) {
                        blockNotFoundThrown = true
                        throw BlockNotFoundException("Block not found")
                    }
                    buildBlock(getBlockNumberSlot.captured)
                }
            coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(1L) }
            every { pruner.run(any()) } just Runs
            every { processor.getLastSyncedBlock() } returns null
            every { processor.process(any(), any()) } just Runs

            val job = launch { indexer.start(6) }
            job.join()

            expect {
                // Verify that the pruner was called at the specified intervals
                verify(exactly = 2) { pruner.run(any()) }
            }
        }

        @Test
        fun `pruner doesn't run if status isn't fully synced`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.SYNCING,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            every { pruner.run(any()) } just Runs

            indexer.publicRunPruner()

            expect {
                // Verify that the pruner was not called when status is not FULLY_SYNCED
                verify(exactly = 0) { pruner.run(any()) }
            }
        }

        @Test
        fun `pruner runs with status FULLY_SYNCED`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.FULLY_SYNCED,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            every { pruner.run(any()) } just Runs

            indexer.publicRunPruner()

            expect {
                // Verify that the pruner was called when status is FULLY_SYNCED
                verify(exactly = 1) { pruner.run(any()) }
            }
        }

        // check all other status'
        @Test
        fun `pruner does not run when status is ERROR`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.ERROR,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            every { pruner.run(any()) } just Runs

            indexer.publicRunPruner()

            expect {
                // Verify that the pruner was not called when status is ERROR
                verify(exactly = 0) { pruner.run(any()) }
            }
        }

        @Test
        fun `pruner does not run when status is REORG`() {
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.REORG,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            every { pruner.run(any()) } just Runs

            indexer.publicRunPruner()

            expect {
                // Verify that the pruner was not called when status is REORG
                verify(exactly = 0) { pruner.run(any()) }
            }
        }

        @Test
        fun `pruner runs based on the interval set`() {
            val prunerInterval = 3L
            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    startBlock = 0L,
                    status = Status.FULLY_SYNCED,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = prunerInterval
                )

            every { pruner.run(any()) } just Runs

            // Simulate multiple calls to publicRunPruner
            for (i in 0..8) {
                indexer.publicRunPruner()
                indexer.incrementBlockNumber()
            }

            expect {
                // Verify that the pruner was called only once at the specified interval
                verify(exactly = 3) { pruner.run(any()) }
                // Status should remain FULLY_SYNCED
                that(indexer.status).isEqualTo(Status.FULLY_SYNCED)
            }
        }
    }

    @Nested
    inner class InitialiseTest {
        @Test
        fun `initialise should rollback to last synced block if available`() {
            every { processor.getLastSyncedBlock() } returns
                BlockIdentifier(number = 10L, id = "0x10")
            every { processor.rollback(10L) } just Runs

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.ERROR,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            indexer.publicInitialise()

            expect {
                that(indexer.currentBlockNumber).isEqualTo(10L)
                that(indexer.status).isEqualTo(Status.SYNCING)
                verify(exactly = 1) { processor.rollback(10L) }
            }
        }

        @Test
        fun `initialise should rollback to start block if no last synced block is found`() {
            every { processor.getLastSyncedBlock() } returns null
            every { processor.rollback(0L) } just Runs

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.ERROR,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L,
                    startBlock = 0L
                )

            indexer.publicInitialise()

            expect {
                that(indexer.currentBlockNumber).isEqualTo(0L)
                that(indexer.status).isEqualTo(Status.SYNCING)
                verify(exactly = 1) { processor.rollback(0L) }
            }
        }

        @Test
        fun `initialise should set previousBlock when last synced block is previous to start`() {
            val lastBlock = BlockIdentifier(number = 9L, id = "0x09")
            every { processor.getLastSyncedBlock() } returns lastBlock
            every { processor.rollback(10L) } just Runs

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.ERROR,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L,
                    startBlock = 0L
                )

            indexer.publicInitialise(10L)

            expectThat(indexer.readPreviousBlock()).isEqualTo(lastBlock)
        }
    }

    @Nested
    inner class RestartTest {
        @Test
        fun `restart should call initialise with current block when status is ERROR`() {
            every { processor.getLastSyncedBlock() } returns null
            every { processor.rollback(10L) } just Runs

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.ERROR,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )
            indexer.overwriteCurrentBlockNumber(10L)

            indexer.publicRestart()

            expectThat(indexer.currentBlockNumber).isEqualTo(10L)
        }

        @Test
        fun `restart should call initialise with current block minus one when status is REORG`() {
            every { processor.getLastSyncedBlock() } returns null
            every { processor.rollback(9L) } just Runs

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.REORG,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )
            indexer.overwriteCurrentBlockNumber(10L)

            indexer.publicRestart()

            expectThat(indexer.currentBlockNumber).isEqualTo(9L)
        }

        @Test
        fun `restart should call initialise with no parameter when status is SYNCING`() {
            every { processor.getLastSyncedBlock() } returns null
            every { processor.rollback(0L) } just Runs

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.SYNCING,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            indexer.publicRestart()

            expectThat(indexer.currentBlockNumber).isEqualTo(0L)
        }
    }

    @Nested
    inner class PostProcessTest {
        @Test
        fun `postProcessBlock should increment current block number and set previous block`() =
            runBlocking {
                val block = buildBlock(5L, parentId = "0x4")

                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        status = Status.SYNCING,
                        thorClient = thorClient,
                        processor = processor,
                        pruner = pruner,
                        prunerInterval = 1L
                    )

                indexer.overwriteCurrentBlockNumber(5L)

                indexer.publicPostProcessBlock(block)

                expect {
                    that(indexer.currentBlockNumber).isEqualTo(6L)
                    that(indexer.readPreviousBlock())
                        .isEqualTo(BlockIdentifier(number = 5L, id = block.id))
                }
            }

        @Test
        fun `postProcessBlock should update backoff period and change status to SYNCING when behind`() =
            runBlocking {
                val block = buildBlock(20L)

                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        status = Status.FULLY_SYNCED,
                        thorClient = thorClient,
                        processor = processor,
                        pruner = pruner,
                        prunerInterval = 1L
                    )

                coEvery { thorClient.getBestBlock() } returns buildBlock(21L)

                indexer.overwriteCurrentBlockNumber(20L)
                indexer.publicPostProcessBlock(block)

                expect {
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    that(indexer.currentBlockNumber).isEqualTo(21L)
                }
            }

        @Test
        fun `should throw an IllegalStateException if block number does not match currentBlockNumber`() {
            val block = buildBlock(10L)

            val indexer =
                TestableBlockIndexer(
                    name = "TestBlockIndexer",
                    status = Status.SYNCING,
                    thorClient = thorClient,
                    processor = processor,
                    pruner = pruner,
                    prunerInterval = 1L
                )

            indexer.overwriteCurrentBlockNumber(5L)

            expectThrows<IllegalStateException> {
                    runBlocking { indexer.publicPostProcessBlock(block) }
                }
                .message
                .isEqualTo("Block number mismatch: expected 5, got 10")
        }
    }

    @Nested
    inner class EnsureFullySyncedTest {
        @Test
        fun `should not change status if not fully synced`() {
            runBlocking {
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        status = Status.SYNCING,
                        thorClient = thorClient,
                        processor = processor,
                        pruner = pruner,
                        prunerInterval = 1L
                    )

                indexer.publicEnsureFullySynced()

                expectThat(indexer.status).isEqualTo(Status.SYNCING)
            }
        }

        @Test
        fun `should not change status if fully synced and latest block is not ahead`() {
            runBlocking {
                val indexer =
                    TestableBlockIndexer(
                        name = "TestBlockIndexer",
                        status = Status.FULLY_SYNCED,
                        thorClient = thorClient,
                        processor = processor,
                        pruner = pruner,
                        prunerInterval = 1L
                    )

                indexer.overwriteCurrentBlockNumber(100L)
                coEvery { thorClient.getBestBlock() } returns buildBlock(100L)

                indexer.publicEnsureFullySynced()

                expectThat(indexer.status).isEqualTo(Status.FULLY_SYNCED)
            }

            @Test
            fun `should switch to SYNCING if fully synced but behind best block`() {
                runBlocking {
                    val indexer =
                        TestableBlockIndexer(
                            name = "TestBlockIndexer",
                            status = Status.FULLY_SYNCED,
                            thorClient = thorClient,
                            processor = processor,
                            pruner = pruner,
                            prunerInterval = 1L
                        )

                    indexer.overwriteCurrentBlockNumber(100L)
                    coEvery { thorClient.getBestBlock() } returns buildBlock(105L)

                    indexer.publicEnsureFullySynced()

                    expectThat(indexer.status).isEqualTo(Status.SYNCING)
                }
            }
        }
    }
}

class TestableBlockIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    override var status: Status = Status.SYNCING,
    eventProcessor: CombinedEventProcessor? = null,
    startBlock: Long = 0L,
    pruner: Pruner? = null,
    prunerInterval: Long = 1L,
    syncLoggerInterval: Long = 100L,
) :
    BlockIndexer(
        name = name,
        thorClient = thorClient,
        processor = processor,
        startBlock = startBlock,
        eventProcessor = eventProcessor,
        pruner = pruner,
        prunerInterval = prunerInterval,
        syncLoggerInterval = syncLoggerInterval,
    ) {

    var iterations: Long? = null

    suspend fun start(iterations: Long) {
        this.iterations = iterations
        super.start()
    }

    override suspend fun run() {
        val max = iterations
        var count = 0L

        while (max == null || count < max) {
            runOnce()
            count++
        }
    }

    fun publicRunPruner() {
        super.runPruner()
    }

    fun incrementBlockNumber() {
        currentBlockNumber += 1
    }

    fun publicInitialise(blockNumber: Long? = null) {
        super.initialise(blockNumber)
    }

    fun readPreviousBlock(): BlockIdentifier? {
        return previousBlock
    }

    fun publicRestart() {
        super.restart()
    }

    fun overwriteCurrentBlockNumber(value: Long) {
        currentBlockNumber = value
    }

    suspend fun publicPostProcessBlock(block: Block) {
        super.postProcessBlock(block)
    }

    // Expose ensureFullySynced for testing
    suspend fun publicEnsureFullySynced() {
        super.ensureFullySynced()
    }
}
