package org.vechain.indexer

import io.mockk.MockKAnnotations
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import kotlin.collections.emptyList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.exception.RestartIndexerException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(MockKExtension::class)
internal class ChannelIndexerTest {

    @MockK private lateinit var processor: IndexerProcessor

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var eventProcessor: CombinedEventProcessor

    @MockK private lateinit var pruner: Pruner

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
    }

    private class TestableChannelIndexer(
        thorClient: ThorClient,
        processor: IndexerProcessor,
        eventProcessor: CombinedEventProcessor,
        dependsOn: Set<Indexer> = emptySet()
    ) :
        ChannelIndexer(
            name = "test",
            thorClient = thorClient,
            processor = processor,
            startBlock = 1L,
            syncLoggerInterval = 10_000L,
            eventProcessor = eventProcessor,
            pruner = null,
            prunerInterval = 10_000L,
            batchSize = 2,
            dependsOn = dependsOn
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

        suspend fun testSync(toBlock: Long) {
            sync(toBlock)
        }
    }

    @Nested
    inner class SyncTests {

        @Test
        fun `sync should call processEvents in strictly increasing block number order`() = runTest {
            val maxBlock = 100L
            val capturedBlocks = mutableListOf<Block>()

            // Mock block fetching to return synthetic blocks
            coEvery { thorClient.getBlock(any()) } answers
                {
                    buildBlock(it.invocation.args[0] as Long)
                }

            // Mock processor and eventProcessor
            every { processor.process(any(), any()) } just runs
            every { eventProcessor.processEvents(capture(capturedBlocks)) } returns emptyList()

            // Run the sync
            val indexer = TestableChannelIndexer(thorClient, processor, eventProcessor)
            indexer.testSync(maxBlock)

            // Extract block numbers
            val numbers = capturedBlocks.map { it.number }

            // Assert correct count
            assert(numbers.size == maxBlock.toInt() + 1) {
                "Expected $maxBlock calls to processEvents, but got ${numbers.size}"
            }

            // Assert strictly increasing order
            for (i in 1 until numbers.size) {
                assert(numbers[i] == numbers[i - 1] + 1) {
                    "Block numbers not strictly increasing: ${numbers[i - 1]} → ${numbers[i]}"
                }
            }
        }

        @Test
        fun `sync should handle errors being thrown by getBlocks - should retry and recover`() =
            runTest {
                val maxBlock = 10L
                val capturedBlocks = mutableListOf<Block>()

                // Mock block fetching to throw an exception for the first 5 blocks
                coEvery { thorClient.getBlock(any()) } answers
                    {
                        val blockNumber = it.invocation.args[0] as Long
                        // Randomly throw exceptions 20% of the time
                        val randomNumber = (0..4).random()
                        if (randomNumber == 0) {
                            throw RuntimeException("Simulated error fetching block $blockNumber")
                        }
                        buildBlock(blockNumber)
                    }

                // Mock processor and eventProcessor
                every { processor.process(any(), any()) } just runs
                every { eventProcessor.processEvents(capture(capturedBlocks)) } returns emptyList()

                // Run the sync
                val indexer = TestableChannelIndexer(thorClient, processor, eventProcessor)
                indexer.testSync(maxBlock)

                // Assert that all blocks were processed
                assert(capturedBlocks.size.toLong() == maxBlock + 1) {
                    "Expected ${maxBlock + 1} blocks to be processed, but got ${capturedBlocks.size}"
                }

                // Assert that the last processed block is the maximum block number
                assert(capturedBlocks.last().number == maxBlock) {
                    "Expected last processed block to be $maxBlock, but got ${capturedBlocks.last().number}"
                }
            }

        @Test
        fun `If an error is thrown, a restart should be triggered`() = runTest {
            val maxBlock = 10L
            val capturedBlocks = mutableListOf<Block>()

            // Mock block fetching to return synthetic blocks
            coEvery { thorClient.getBlock(any()) } answers
                {
                    buildBlock(it.invocation.args[0] as Long)
                }

            // Mock processor and eventProcessor
            every { processor.process(any(), any()) } just runs
            every { eventProcessor.processEvents(capture(capturedBlocks)) } throws
                RuntimeException("Simulated error")

            // Run the sync
            val indexer = TestableChannelIndexer(thorClient, processor, eventProcessor)

            // Expect a RestartIndexerException to be thrown
            expectThrows<RestartIndexerException> { indexer.testSync(maxBlock) }

            // Assert that no blocks were processed due to the error
            assert(capturedBlocks.size == 1) {
                "Expected processEvents to be called once, but was called ${capturedBlocks.size} times"
            }
        }
    }

    @Nested
    inner class DependencyTests {
        @BeforeEach
        fun setup() {
            every { processor.rollback(any()) } just Runs
        }

        @Test
        fun `should wait for dependencies to be fully synced before starting`() = runBlocking {
            val dependencyIndexer = mockk<Indexer>()
            every { dependencyIndexer.status } returns Status.SYNCING
            every { processor.getLastSyncedBlock() } answers
                {
                    BlockIdentifier(number = 100L, id = "0x100")
                }
            coEvery { thorClient.getFinalizedBlock() } coAnswers { buildBlock(1L) }
            coEvery { thorClient.getBlock(any()) } coAnswers { buildBlock(100L) }
            every { processor.process(any(), any()) } just Runs
            coEvery { eventProcessor.processEvents(any<Block>()) } coAnswers { emptyList() }

            val indexer =
                TestableChannelIndexer(
                    thorClient,
                    processor,
                    eventProcessor,
                    dependsOn = setOf(dependencyIndexer)
                )

            val job = launch { indexer.start(1L) }

            // Give some time to ensure start() is waiting
            delay(100L)
            expectThat(indexer.status).isEqualTo(Status.PENDING_DEPENDENCY)

            // Change dependency status to FULLY_SYNCED and verify indexer starts
            every { dependencyIndexer.status } returns Status.FULLY_SYNCED
            delay(100L)
            expectThat(indexer.status).isEqualTo(Status.SYNCING)

            job.cancel()
        }

        @Test
        fun `if already fully synced, should wait for dependencies if they are no longer fully synced`() =
            runBlocking {
                val dependencyIndexer = mockk<Indexer>()
                every { dependencyIndexer.status } returns Status.FULLY_SYNCED
                every { processor.getLastSyncedBlock() } answers
                    {
                        BlockIdentifier(number = 100L, id = "0x100")
                    }
                coEvery { thorClient.getFinalizedBlock() } coAnswers { buildBlock(1L) }
                every { processor.process(any(), any()) } just Runs

                coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(99L) }
                // Throw  BlockNotFoundException here so the indexer starts in FULLY_SYNCED status
                coEvery { thorClient.getBlock(any()) } coAnswers
                    {
                        throw BlockNotFoundException("Block not found")
                    }

                val indexer =
                    TestableChannelIndexer(
                        thorClient,
                        processor,
                        eventProcessor,
                        dependsOn = setOf(dependencyIndexer)
                    )

                val job1 = launch { indexer.start(1L) }

                // Indexer should start and be in FULLY_SYNCED status
                delay(100L)
                expectThat(indexer.status).isEqualTo(Status.FULLY_SYNCED)

                // Change dependency status to SYNCING and verify indexer waits
                every { dependencyIndexer.status } returns Status.SYNCING

                val job2 = launch { indexer.start(1L) }
                delay(100L)
                expectThat(indexer.status).isEqualTo(Status.PENDING_DEPENDENCY)

                // Change dependency status back to FULLY_SYNCED and verify indexer resumes
                every { dependencyIndexer.status } returns Status.FULLY_SYNCED
                val job3 = launch { indexer.start(1L) }
                delay(100L)
                expectThat(indexer.status).isEqualTo(Status.FULLY_SYNCED)

                job1.cancel()
                job2.cancel()
                job3.cancel()
            }

        @Test
        fun `should handle multiple dependencies with different statuses`() = runBlocking {
            val dependency1 = mockk<Indexer>()
            val dependency2 = mockk<Indexer>()
            every { dependency1.status } returns Status.FULLY_SYNCED
            every { dependency2.status } returns Status.SYNCING

            every { processor.getLastSyncedBlock() } answers
                {
                    BlockIdentifier(number = 100L, id = "0x100")
                }
            coEvery { thorClient.getFinalizedBlock() } coAnswers { buildBlock(1L) }
            coEvery { thorClient.getBlock(any()) } coAnswers { buildBlock(100L) }
            every { processor.process(any(), any()) } just Runs
            coEvery { eventProcessor.processEvents(any<Block>()) } coAnswers { emptyList() }

            val indexer =
                TestableChannelIndexer(
                    thorClient,
                    processor,
                    eventProcessor,
                    dependsOn = setOf(dependency1, dependency2)
                )

            val job = launch { indexer.start(1L) }

            // Give some time to ensure start() is waiting
            delay(100L)
            expectThat(indexer.status).isEqualTo(Status.PENDING_DEPENDENCY)

            // Change second dependency status to FULLY_SYNCED and verify indexer starts
            every { dependency2.status } returns Status.FULLY_SYNCED
            delay(100L)
            expectThat(indexer.status).isEqualTo(Status.SYNCING)

            job.cancel()
        }
    }
}
