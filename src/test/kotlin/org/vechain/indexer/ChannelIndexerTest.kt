package org.vechain.indexer

import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import kotlin.collections.emptyList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.RestartIndexerException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import strikt.api.expectThrows

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
        dependantIndexers: Set<Indexer> = emptySet()
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
            dependantIndexers = dependantIndexers
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
            every { processor.process(any()) } just runs
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
                every { processor.process(any()) } just runs
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
            val captureResults = mutableListOf<IndexingResult>()

            // Mock block fetching to return synthetic blocks
            coEvery { thorClient.getBlock(any()) } answers
                {
                    buildBlock(it.invocation.args[0] as Long)
                }

            // Mock processor and eventProcessor
            every { eventProcessor.processEvents(any<Block>()) } returns emptyList()
            every { processor.process(capture(captureResults)) } throws
                RuntimeException("Simulated error")

            // Run the sync
            val indexer = TestableChannelIndexer(thorClient, processor, eventProcessor)

            // Expect a RestartIndexerException to be thrown
            expectThrows<RestartIndexerException> { indexer.testSync(maxBlock) }

            // Assert that no blocks were processed due to the error
            assert(captureResults.size == 1) {
                "Expected processEvents to be called once, but was called ${captureResults.size} times"
            }
        }
    }
}
