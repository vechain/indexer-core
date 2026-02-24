package org.vechain.indexer

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo

@OptIn(ExperimentalCoroutinesApi::class)
internal class BlockFetcherTest {

    @Nested
    inner class CalculateWindowSize {
        private val fetcher = BlockFetcher(mockk(), emptyList())

        @Test
        fun `returns max prefetch size when timestamp is missing`() {
            val result =
                fetcher.calculateWindowSize(lastBlockTimestampSeconds = null, maxPrefetchSize = 5)
            expectThat(result).isEqualTo(5)
        }

        @Test
        fun `reduces window to one block when nearly caught up`() {
            val recentTimestamp = (System.currentTimeMillis() / 1000) - 5
            val result = fetcher.calculateWindowSize(recentTimestamp, 10)
            expectThat(result).isEqualTo(1)
        }

        @Test
        fun `caps window at max prefetch size when far behind`() {
            val oldTimestamp = (System.currentTimeMillis() / 1000) - 500
            val result = fetcher.calculateWindowSize(oldTimestamp, 8)
            expectThat(result).isEqualTo(8)
        }

        @Test
        fun `scales window proportionally when moderately behind`() {
            val secondsBehind = 35L
            val timestamp = (System.currentTimeMillis() / 1000) - secondsBehind
            val result = fetcher.calculateWindowSize(timestamp, 10)
            expectThat(result).isEqualTo(4)
        }
    }

    @Nested
    inner class FetchAndPrepareBlock {

        @Test
        fun `fetches block and inspects clauses`() = runTest {
            val thorClient = mockk<ThorClient>()
            val clause = Clause(to = "0xAddr", value = "0x0", data = "0x1111")
            val block = buildBlock(num = 5L)
            val inspectionResult = mockk<InspectionResult>()

            coEvery { thorClient.waitForBlock(BlockRevision.Number(5L)) } returns block
            coEvery {
                thorClient.inspectClauses(listOf(clause), BlockRevision.Id(block.id))
            } returns listOf(inspectionResult)

            val fetcher = BlockFetcher(thorClient, listOf(clause))
            val result = fetcher.fetchAndPrepareBlock(5L)

            expectThat(result.block).isEqualTo(block)
            expectThat(result.inspectionResults).isEqualTo(listOf(inspectionResult))
        }

        @Test
        fun `returns empty inspection results when no clauses`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block = buildBlock(num = 3L)

            coEvery { thorClient.waitForBlock(BlockRevision.Number(3L)) } returns block

            val fetcher = BlockFetcher(thorClient, emptyList())
            val result = fetcher.fetchAndPrepareBlock(3L)

            expectThat(result.block).isEqualTo(block)
            expectThat(result.inspectionResults).isEqualTo(emptyList())
            coVerify(exactly = 0) { thorClient.inspectClauses(any(), any()) }
        }

        @Test
        fun `retries on transient failure`() = runTest {
            val thorClient = mockk<ThorClient>()
            var attempts = 0

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } coAnswers
                {
                    attempts++
                    if (attempts < 2) throw RuntimeException("Transient error")
                    buildBlock(num = 0L)
                }

            val fetcher = BlockFetcher(thorClient, emptyList())
            val result = fetcher.fetchAndPrepareBlock(0L)

            expectThat(attempts).isEqualTo(2)
            expectThat(result.block.number).isEqualTo(0L)
        }

        @Test
        fun `propagates CancellationException without retry`() = runTest {
            val thorClient = mockk<ThorClient>()

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } throws
                CancellationException("cancelled")

            val fetcher = BlockFetcher(thorClient, emptyList())
            val job = launch {
                assertThrows<CancellationException> { fetcher.fetchAndPrepareBlock(0L) }
            }
            job.join()

            coVerify(exactly = 1) { thorClient.waitForBlock(any<BlockRevision>()) }
        }

        @Test
        fun `propagates ReorgException without retry`() = runTest {
            val thorClient = mockk<ThorClient>()

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } throws ReorgException("reorg")

            val fetcher = BlockFetcher(thorClient, emptyList())

            assertThrows<ReorgException> { fetcher.fetchAndPrepareBlock(0L) }
            coVerify(exactly = 1) { thorClient.waitForBlock(any<BlockRevision>()) }
        }
    }

    @Nested
    inner class PrefetchBlocksInOrder {

        @Test
        fun `sends blocks to all channels in order`() = runTest {
            val thorClient = mockk<ThorClient>()
            val blocks = (0L..2L).map { buildBlock(num = it) }

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns blocks[0]
            coEvery { thorClient.waitForBlock(BlockRevision.Number(1L)) } returns blocks[1]
            coEvery { thorClient.waitForBlock(BlockRevision.Number(2L)) } returns blocks[2]
            coEvery { thorClient.waitForBlock(BlockRevision.Number(3L)) } coAnswers
                {
                    delay(5000)
                    buildBlock(num = 3L)
                }

            val channel1 = Channel<PreparedBlock>(capacity = 5)
            val channel2 = Channel<PreparedBlock>(capacity = 5)

            val fetcher = BlockFetcher(thorClient, emptyList())
            val job = launch {
                fetcher.prefetchBlocksInOrder(
                    startBlock = 0L,
                    maxBatchSize = 1,
                    groupChannels = listOf(channel1, channel2),
                )
            }

            val received1 = (0..2).map { channel1.receive().block.number }
            val received2 = (0..2).map { channel2.receive().block.number }

            job.cancelAndJoin()

            expectThat(received1).isEqualTo(listOf(0L, 1L, 2L))
            expectThat(received2).isEqualTo(listOf(0L, 1L, 2L))
        }

        @Test
        fun `validates startBlock is non-negative`() = runTest {
            val fetcher = BlockFetcher(mockk(), emptyList())

            assertThrows<IllegalArgumentException> {
                fetcher.prefetchBlocksInOrder(
                    startBlock = -1,
                    maxBatchSize = 1,
                    groupChannels = emptyList(),
                )
            }
        }

        @Test
        fun `validates maxBatchSize is at least 1`() = runTest {
            val fetcher = BlockFetcher(mockk(), emptyList())

            assertThrows<IllegalArgumentException> {
                fetcher.prefetchBlocksInOrder(
                    startBlock = 0,
                    maxBatchSize = 0,
                    groupChannels = emptyList(),
                )
            }
        }

        @Test
        fun `fetches multiple blocks in parallel when behind`() = runTest {
            val thorClient = mockk<ThorClient>()
            val fetchedBlocks = mutableListOf<Long>()

            // Return blocks with old timestamps to trigger larger window
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    synchronized(fetchedBlocks) { fetchedBlocks.add(blockNum) }
                    buildBlock(num = blockNum, timestamp = 1L)
                }

            val channel = Channel<PreparedBlock>(capacity = 20)
            val fetcher = BlockFetcher(thorClient, emptyList())

            val job = launch {
                fetcher.prefetchBlocksInOrder(
                    startBlock = 0L,
                    maxBatchSize = 5,
                    groupChannels = listOf(channel),
                )
            }

            // Drain some blocks
            repeat(5) { channel.receive() }
            job.cancelAndJoin()

            // After first batch (null timestamp), should use maxBatchSize=5
            // After that, old timestamps should also trigger large window
            expectThat(fetchedBlocks.size).isGreaterThanOrEqualTo(5)
        }
    }
}
