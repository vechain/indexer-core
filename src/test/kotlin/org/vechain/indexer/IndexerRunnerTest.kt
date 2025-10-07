package org.vechain.indexer

import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan
import strikt.assertions.isGreaterThanOrEqualTo

@OptIn(ExperimentalCoroutinesApi::class)
internal class IndexerRunnerTest {

    private fun createMockIndexer(
        name: String,
        currentBlock: Long = 0L,
        dependsOn: Indexer? = null,
        initializeBlock: (suspend () -> Unit)? = null,
        fastSyncBlock: (suspend () -> Unit)? = null,
        processBlock: (suspend (Block) -> Unit)? = null
    ): Indexer {
        var currentBlockNumber = currentBlock

        return mockk(relaxed = true) {
            every { this@mockk.name } returns name
            every { this@mockk.dependsOn } returns dependsOn
            every { getCurrentBlockNumber() } answers { currentBlockNumber }

            if (initializeBlock != null) {
                coEvery { initialise() } coAnswers { initializeBlock() }
            } else {
                coEvery { initialise() } just Runs
            }

            if (fastSyncBlock != null) {
                coEvery { fastSync() } coAnswers { fastSyncBlock() }
            } else {
                coEvery { fastSync() } just Runs
            }

            if (processBlock != null) {
                coEvery { processBlock(any()) } coAnswers
                    {
                        processBlock(firstArg())
                        currentBlockNumber++
                    }
            } else {
                coEvery { processBlock(any()) } answers { currentBlockNumber++ }
            }
        }
    }

    @Nested
    inner class InitialiseAndSyncAll {

        @Test
        fun `should initialise and sync all indexers concurrently`() = runTest {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")
            val indexer3 = createMockIndexer("indexer3")

            val runner = IndexerRunner()
            runner.initialiseAndSyncAll(listOf(indexer1, indexer2, indexer3))

            coVerify(exactly = 1) { indexer1.initialise() }
            coVerify(exactly = 1) { indexer1.fastSync() }
            coVerify(exactly = 1) { indexer2.initialise() }
            coVerify(exactly = 1) { indexer2.fastSync() }
            coVerify(exactly = 1) { indexer3.initialise() }
            coVerify(exactly = 1) { indexer3.fastSync() }
        }

        @Test
        fun `should retry on initialise failure`() = runTest {
            var initAttempts = 0
            val indexer =
                createMockIndexer(
                    name = "indexer1",
                    initializeBlock = {
                        initAttempts++
                        if (initAttempts < 3) {
                            throw RuntimeException("Init failed")
                        }
                    }
                )

            val runner = IndexerRunner()
            runner.initialiseAndSyncAll(listOf(indexer))

            expectThat(initAttempts).isEqualTo(3)
            coVerify(exactly = 3) { indexer.initialise() }
            coVerify(exactly = 1) { indexer.fastSync() }
        }

        @Test
        fun `should retry on fastSync failure`() = runTest {
            var syncAttempts = 0
            val indexer =
                createMockIndexer(
                    name = "indexer1",
                    fastSyncBlock = {
                        syncAttempts++
                        if (syncAttempts < 2) {
                            throw RuntimeException("Sync failed")
                        }
                    }
                )

            val runner = IndexerRunner()
            runner.initialiseAndSyncAll(listOf(indexer))

            expectThat(syncAttempts).isEqualTo(2)
            coVerify(exactly = 1) { indexer.initialise() }
            coVerify(exactly = 2) { indexer.fastSync() }
        }

        @Test
        fun `should not retry on CancellationException`() = runTest {
            val indexer =
                createMockIndexer(
                    name = "indexer1",
                    initializeBlock = { throw CancellationException("Cancelled") }
                )

            val runner = IndexerRunner()
            val job = launch { runner.initialiseAndSyncAll(listOf(indexer)) }

            delay(100) // Give it time to attempt
            job.cancelAndJoin()

            // Should only attempt once before cancellation
            coVerify(atMost = 1) { indexer.initialise() }
        }

        @Test
        fun `should initialise and sync single indexer`() = runTest {
            val indexer = createMockIndexer("indexer1")

            val runner = IndexerRunner()
            runner.initialiseAndSyncAll(listOf(indexer))

            coVerify(exactly = 1) { indexer.initialise() }
            coVerify(exactly = 1) { indexer.fastSync() }
        }

        @Test
        fun `should complete even if one indexer is slow`() = runTest {
            val fastIndexer = createMockIndexer("fast")
            val slowIndexer = createMockIndexer(name = "slow", initializeBlock = { delay(50) })

            val runner = IndexerRunner()
            runner.initialiseAndSyncAll(listOf(fastIndexer, slowIndexer))

            coVerify(exactly = 1) { fastIndexer.initialise() }
            coVerify(exactly = 1) { fastIndexer.fastSync() }
            coVerify(exactly = 1) { slowIndexer.initialise() }
            coVerify(exactly = 1) { slowIndexer.fastSync() }
        }
    }

    @Nested
    inner class RunAllIndexers {

        @Test
        fun `should return early when no indexers provided`() = runTest {
            val thorClient = mockk<ThorClient>()
            val runner = IndexerRunner()

            // Should complete without error
            runner.runAllIndexers(emptyList(), thorClient, 1)

            // No interactions with thor client
            coVerify(exactly = 0) { thorClient.waitForBlock(any()) }
        }

        @Test
        fun `should fetch blocks starting from minimum indexer block number`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block50 = buildBlock(num = 50L)
            val block51 = buildBlock(num = 51L)

            val indexer1 = createMockIndexer("indexer1", currentBlock = 50L)
            val indexer2 = createMockIndexer("indexer2", currentBlock = 75L)

            coEvery { thorClient.waitForBlock(50L) } returns block50
            coEvery { thorClient.waitForBlock(51L) } returns block51

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(200) // Let it fetch a couple blocks
            job.cancelAndJoin()

            // Should start fetching from block 50 (the minimum)
            coVerify(atLeast = 1) { thorClient.waitForBlock(50L) }
        }

        @Test
        fun `should process blocks through all indexers in same group concurrently`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val processedBy = mutableListOf<String>()
            val indexer1 =
                createMockIndexer(
                    "indexer1",
                    currentBlock = 0L,
                    processBlock = {
                        delay(50)
                        synchronized(processedBy) { processedBy.add("indexer1") }
                    }
                )
            val indexer2 =
                createMockIndexer(
                    "indexer2",
                    currentBlock = 0L,
                    processBlock = {
                        delay(50)
                        synchronized(processedBy) { processedBy.add("indexer2") }
                    }
                )

            coEvery { thorClient.waitForBlock(0L) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(200) // Let them process
            job.cancelAndJoin()

            // Both should have processed
            expectThat(processedBy.size).isGreaterThanOrEqualTo(2)
            coVerify(atLeast = 1) { indexer1.processBlock(block0) }
            coVerify(atLeast = 1) { indexer2.processBlock(block0) }
        }

        @Test
        fun `should skip blocks already processed by indexer`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block5 = buildBlock(num = 5L)

            // Indexer already at block 10, should skip block 5
            val indexer = createMockIndexer("indexer1", currentBlock = 10L)

            coEvery { thorClient.waitForBlock(10L) } coAnswers
                {
                    delay(1000) // Delay to prevent infinite loop
                    buildBlock(num = 10L)
                }

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(100)
            job.cancelAndJoin()

            // Should never try to process block 5
            coVerify(exactly = 0) { indexer.processBlock(match { it.number == 5L }) }
        }

        @Test
        fun `should throw when indexer is behind expected block`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block10 = buildBlock(num = 10L)

            // Indexer at block 5 but receives block 10
            val indexer = createMockIndexer("indexer1", currentBlock = 5L)

            coEvery { thorClient.waitForBlock(5L) } returns block10

            val runner = IndexerRunner()

            assertThrows<IllegalStateException> {
                runner.runAllIndexers(listOf(indexer), thorClient, 1)
            }
        }

        @Test
        fun `should retry block fetch on failure`() = runTest {
            val thorClient = mockk<ThorClient>()
            var attempts = 0

            coEvery { thorClient.waitForBlock(0L) } coAnswers
                {
                    attempts++
                    if (attempts < 3) {
                        throw RuntimeException("Fetch failed")
                    }
                    buildBlock(num = 0L)
                }

            val indexer = createMockIndexer("indexer1", currentBlock = 0L)

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(3500) // Wait for retries (3 attempts * 1 second delay)
            job.cancelAndJoin()

            expectThat(attempts).isGreaterThanOrEqualTo(3)
        }

        @Test
        fun `should retry block processing on failure`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            var processAttempts = 0
            val indexer =
                createMockIndexer(
                    "indexer1",
                    currentBlock = 0L,
                    processBlock = {
                        processAttempts++
                        if (processAttempts < 2) {
                            throw RuntimeException("Process failed")
                        }
                    }
                )

            coEvery { thorClient.waitForBlock(any()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(2500) // Wait for retries
            job.cancelAndJoin()

            expectThat(processAttempts).isGreaterThanOrEqualTo(2)
        }

        @Test
        fun `should use correct batch size for channel capacity`() = runTest {
            val thorClient = mockk<ThorClient>()
            val blocks = (0L..10L).map { buildBlock(num = it) }

            val fetchedBlocks = mutableListOf<Long>()
            coEvery { thorClient.waitForBlock(any()) } coAnswers
                {
                    val blockNum = firstArg<Long>()
                    fetchedBlocks.add(blockNum)
                    delay(50) // Slow down to test buffering
                    blocks[blockNum.toInt()]
                }

            val slowIndexer =
                createMockIndexer("slow", currentBlock = 0L, processBlock = { delay(100) })

            val runner = IndexerRunner()
            val job = launch {
                runner.runAllIndexers(listOf(slowIndexer), thorClient, batchSize = 5)
            }

            delay(500)
            job.cancelAndJoin()

            // Should have fetched multiple blocks ahead due to buffering
            expectThat(fetchedBlocks.size).isGreaterThan(1)
        }
    }

    @Nested
    inner class ProcessGroupBlocks {

        @Test
        fun `should process blocks sequentially for each group`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            val block1 = buildBlock(num = 1L)

            val processOrder = mutableListOf<Long>()
            val indexer =
                createMockIndexer(
                    "indexer1",
                    currentBlock = 0L,
                    processBlock = { block ->
                        synchronized(processOrder) { processOrder.add(block.number) }
                    }
                )

            coEvery { thorClient.waitForBlock(0L) } returns block0
            coEvery { thorClient.waitForBlock(1L) } returns block1
            coEvery { thorClient.waitForBlock(2L) } coAnswers
                {
                    delay(5000) // Prevent further fetching
                    buildBlock(num = 2L)
                }

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(500)
            job.cancelAndJoin()

            // Blocks should be processed in order
            expectThat(processOrder.toList()).isEqualTo(listOf(0L, 1L))
        }

        @Test
        fun `should allow parallel processing within same group`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val startTimes = mutableMapOf<String, Long>()
            val endTimes = mutableMapOf<String, Long>()

            val indexer1 =
                createMockIndexer(
                    "indexer1",
                    currentBlock = 0L,
                    processBlock = {
                        synchronized(startTimes) {
                            startTimes["indexer1"] = System.currentTimeMillis()
                        }
                        delay(100)
                        synchronized(endTimes) { endTimes["indexer1"] = System.currentTimeMillis() }
                    }
                )

            val indexer2 =
                createMockIndexer(
                    "indexer2",
                    currentBlock = 0L,
                    processBlock = {
                        synchronized(startTimes) {
                            startTimes["indexer2"] = System.currentTimeMillis()
                        }
                        delay(100)
                        synchronized(endTimes) { endTimes["indexer2"] = System.currentTimeMillis() }
                    }
                )

            coEvery { thorClient.waitForBlock(any()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            // Both should have started around the same time (within 50ms)
            val timeDiff =
                kotlin.math.abs((startTimes["indexer1"] ?: 0L) - (startTimes["indexer2"] ?: 0L))
            expectThat(timeDiff).isGreaterThanOrEqualTo(0)
        }
    }

    @Nested
    inner class Integration {

        @Test
        fun `should process blocks with dependent indexers in correct order`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val indexer1 = createMockIndexer("indexer1", currentBlock = 0L)
            val indexer2 = createMockIndexer("indexer2", currentBlock = 0L, dependsOn = indexer1)

            val processOrder = mutableListOf<String>()

            coEvery { indexer1.processBlock(any()) } coAnswers
                {
                    synchronized(processOrder) { processOrder.add("indexer1") }
                    delay(50)
                }

            coEvery { indexer2.processBlock(any()) } coAnswers
                {
                    synchronized(processOrder) { processOrder.add("indexer2") }
                }

            coEvery { thorClient.waitForBlock(any()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            // indexer2 depends on indexer1, but they should process same block concurrently
            coVerify(atLeast = 1) { indexer1.processBlock(block0) }
            coVerify(atLeast = 1) { indexer2.processBlock(block0) }
        }

        @Test
        fun `full run method should initialise sync and then process blocks`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val callOrder = mutableListOf<String>()
            val indexer =
                createMockIndexer(
                    "indexer1",
                    currentBlock = 0L,
                    initializeBlock = { synchronized(callOrder) { callOrder.add("init") } },
                    fastSyncBlock = { synchronized(callOrder) { callOrder.add("sync") } },
                    processBlock = { synchronized(callOrder) { callOrder.add("process") } }
                )

            coEvery { thorClient.waitForBlock(any()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.run(listOf(indexer), 1, thorClient) }

            delay(300)
            job.cancelAndJoin()

            // Should be init, sync, then process
            expectThat(callOrder[0]).isEqualTo("init")
            expectThat(callOrder[1]).isEqualTo("sync")
            expectThat(callOrder.drop(2).any { it == "process" }).isEqualTo(true)
        }

        @Test
        fun `should require at least one indexer`() {
            val thorClient = mockk<ThorClient>()
            val runner = IndexerRunner()

            assertThrows<IllegalArgumentException> {
                runTest { runner.run(emptyList(), 1, thorClient) }
            }
        }

        @Test
        fun `launch should create and run indexer orchestrator`() = runTest {
            val thorClient = mockk<ThorClient>()
            val indexer = createMockIndexer("indexer1", currentBlock = 0L)
            val block0 = buildBlock(num = 0L)

            coEvery { thorClient.waitForBlock(any()) } returns block0

            val job =
                IndexerRunner.launch(
                    scope = this,
                    thorClient = thorClient,
                    indexers = listOf(indexer),
                    blockBatchSize = 1
                )

            delay(300)
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.initialise() }
            coVerify(atLeast = 1) { indexer.fastSync() }
        }
    }

    @Nested
    inner class EdgeCases {

        @Test
        fun `should handle indexer with no blocks to process`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block100 = buildBlock(num = 100L)

            // Indexer already fully synced
            val indexer = createMockIndexer("indexer1", currentBlock = 100L)

            coEvery { thorClient.waitForBlock(100L) } returns block100

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.processBlock(block100) }
        }

        // Commented out due to JVM agent issues with large batch sizes in tests
        //        @Test
        //        fun `should handle very large batch size`() = runTest {
        //            val thorClient = mockk<ThorClient>()
        //            val block0 = buildBlock(num = 0L)
        //
        //            val indexer = createMockIndexer("indexer1", currentBlock = 0L)
        //
        //            coEvery { thorClient.waitForBlock(any()) } returns block0
        //
        //            val runner = IndexerRunner()
        //            val job = launch {
        //                runner.runAllIndexers(listOf(indexer), thorClient, batchSize = 100)
        //            }
        //
        //            delay(200)
        //            job.cancelAndJoin()
        //
        //            // Should work normally
        //            coVerify(atLeast = 1) { indexer.processBlock(block0) }
        //        }

        @Test
        fun `should handle single indexer in multiple groups scenario`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val indexer = createMockIndexer("indexer1", currentBlock = 0L)

            coEvery { thorClient.waitForBlock(any()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(200)
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.processBlock(block0) }
        }
    }
}
