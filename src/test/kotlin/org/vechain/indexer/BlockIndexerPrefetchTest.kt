package org.vechain.indexer

import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo

@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(MockKExtension::class)
internal class BlockIndexerPrefetchTest {

    @MockK private lateinit var processor: IndexerProcessor

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var eventProcessor: CombinedEventProcessor

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
        every { processor.rollback(any()) } just runs
        every { processor.getLastSyncedBlock() } returns null
        every { eventProcessor.processEvents(any<Block>()) } returns emptyList()
    }

    private fun createIndexer(batchSize: Int = 3): TestableBlockIndexer =
        TestableBlockIndexer(
            name = "prefetch-tester",
            thorClient = thorClient,
            processor = processor,
            eventProcessor = eventProcessor,
            startBlock = 0L,
            syncLoggerInterval = 10_000L,
            batchSizeOverride = batchSize,
        )

    @Nested
    inner class Ordering {

        @Test
        fun `prefetch maintains in order processing`(): Unit = runTest {
            val maxBlock = 20L
            val processedBlocks = mutableListOf<Long>()

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val blockNumber = it.invocation.args[0] as Long
                    buildBlock(blockNumber)
                }
            coEvery { thorClient.getBestBlock() } returns buildBlock(maxBlock)
            every { processor.process(any()) } answers
                {
                    val result = it.invocation.args[0] as IndexingResult.Normal
                    processedBlocks += result.block.number
                }

            val indexer = createIndexer(batchSize = 4)
            indexer.start(maxBlock)

            val expectedBlocks = (0L until maxBlock).toList()
            expectThat(processedBlocks).containsExactly(*expectedBlocks.toTypedArray())
        }
    }

    @Nested
    inner class ErrorRecovery {

        @Test
        fun `prefetch retries transient fetch failures`(): Unit = runTest {
            val maxBlock = 10L
            val attempts = mutableMapOf<Long, Int>()
            val processedBlocks = mutableListOf<Long>()
            var bestBlockNumber = -1L

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val blockNumber = it.invocation.args[0] as Long
                    val count = attempts.getOrDefault(blockNumber, 0)
                    attempts[blockNumber] = count + 1
                    if (count == 0) {
                        bestBlockNumber = (blockNumber - 1).coerceAtLeast(0)
                        throw BlockNotFoundException("Simulated transient failure for $blockNumber")
                    }
                    bestBlockNumber = maxOf(bestBlockNumber, blockNumber)
                    buildBlock(blockNumber)
                }
            coEvery { thorClient.getBestBlock() } answers
                {
                    buildBlock(bestBlockNumber.coerceAtLeast(0))
                }
            every { processor.process(any()) } answers
                {
                    val result = it.invocation.args[0] as IndexingResult.Normal
                    processedBlocks += result.block.number
                }

            val indexer = createIndexer(batchSize = 3)
            // Allow extra iterations to account for retries
            indexer.start((maxBlock * 2))

            val expectedBlocks = (0L until maxBlock).toList()
            val processedPrefix = processedBlocks.take(expectedBlocks.size)
            expectThat(processedPrefix).containsExactly(*expectedBlocks.toTypedArray())

            val processedCounts = processedBlocks.groupingBy { it }.eachCount()
            expectedBlocks.forEach { block -> expectThat(processedCounts[block]).isEqualTo(1) }

            val attemptCounts = attempts.filterKeys { it in expectedBlocks }
            expectThat(attemptCounts.keys.sorted()).containsExactly(*expectedBlocks.toTypedArray())
            attemptCounts.forEach { (_, count) -> expectThat(count).isEqualTo(2) }
        }
    }
}
