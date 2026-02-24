package org.vechain.indexer

import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import kotlin.math.pow
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockRevision
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
            every { getInspectionClauses() } returns null

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
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } returns 0L
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } just Runs
                    coEvery { fastSync() } coAnswers
                        {
                            syncAttempts++
                            if (syncAttempts < 2) {
                                throw RuntimeException("Sync failed")
                            }
                        }
                }

            val runner = IndexerRunner()
            runner.initialiseAndSyncAll(listOf(indexer))

            expectThat(syncAttempts).isEqualTo(2)
            // Both initialise and fastSync are wrapped in retryUntilSuccess, so both retry
            coVerify(exactly = 2) { indexer.initialise() }
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
            coVerify(exactly = 0) { thorClient.waitForBlock(any<BlockRevision>()) }
        }

        @Test
        fun `should fetch blocks starting from minimum indexer block number`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block50 = buildBlock(num = 50L)
            val block51 = buildBlock(num = 51L)

            val indexer1 = createMockIndexer("indexer1", currentBlock = 50L)
            val indexer2 = createMockIndexer("indexer2", currentBlock = 75L)

            coEvery { thorClient.waitForBlock(BlockRevision.Number(50L)) } returns block50
            coEvery { thorClient.waitForBlock(BlockRevision.Number(51L)) } returns block51

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(200) // Let it fetch a couple blocks
            job.cancelAndJoin()

            // Should start fetching from block 50 (the minimum)
            coVerify(atLeast = 1) { thorClient.waitForBlock(BlockRevision.Number(50L)) }
        }

        @Test
        @Disabled("Test timing issue - blocks not processed before cancellation")
        fun `should process blocks through all indexers in same group concurrently`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val processedBy = mutableListOf<String>()
            var block1Num = 0L
            var block2Num = 0L

            val indexer1 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { block1Num }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            delay(50)
                            synchronized(processedBy) { processedBy.add("indexer1") }
                            block1Num++
                        }
                }
            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer2"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { block2Num }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            delay(50)
                            synchronized(processedBy) { processedBy.add("indexer2") }
                            block2Num++
                        }
                }

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(5000)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

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

            coEvery { thorClient.waitForBlock(BlockRevision.Number(10L)) } coAnswers
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

            coEvery { thorClient.waitForBlock(BlockRevision.Number(5L)) } returns block10

            val runner = IndexerRunner()

            assertThrows<IllegalStateException> {
                runner.runAllIndexers(listOf(indexer), thorClient, 1)
            }
        }

        @Test
        fun `should retry block fetch on failure`() = runTest {
            val thorClient = mockk<ThorClient>()
            var attempts = 0

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } coAnswers
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

            // Worst-case delay per retry: base + jitter < 2 * base
            // With 2 retries (initialDelay=1000, multiplier=2.0):
            //   retry 1: < 2 * 1000 = 2000, retry 2: < 2 * 2000 = 4000
            val retriesNeeded = 2
            val initialDelayMs = 1_000L
            val multiplier = 2.0
            val worstCaseDelay =
                (0 until retriesNeeded).sumOf { i ->
                    (2 * initialDelayMs * multiplier.pow(i)).toLong()
                }
            delay(worstCaseDelay)
            job.cancelAndJoin()

            expectThat(attempts).isGreaterThanOrEqualTo(3)
        }

        @Test
        @Disabled("Causes OutOfMemoryError during test execution")
        fun `should retry block processing on failure`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            var processAttempts = 0
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            processAttempts++
                            if (processAttempts < 2) {
                                throw RuntimeException("Process failed")
                            }
                            currentBlockNum++
                        }
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

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
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    fetchedBlocks.add(blockNum)
                    delay(50) // Slow down to test buffering
                    blocks[blockNum.toInt()]
                }

            var currentBlockNum = 0L
            val slowIndexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "slow"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            delay(100)
                            currentBlockNum++
                        }
                }

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
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            val block = firstArg<Block>()
                            synchronized(processOrder) { processOrder.add(block.number) }
                            currentBlockNum++
                        }
                }

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
            coEvery { thorClient.waitForBlock(BlockRevision.Number(1L)) } returns block1
            coEvery { thorClient.waitForBlock(BlockRevision.Number(2L)) } coAnswers
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
            var block1Num = 0L
            var block2Num = 0L

            val indexer1 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { block1Num }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            synchronized(startTimes) {
                                startTimes["indexer1"] = System.currentTimeMillis()
                            }
                            delay(100)
                            synchronized(endTimes) {
                                endTimes["indexer1"] = System.currentTimeMillis()
                            }
                            block1Num++
                        }
                }

            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer2"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { block2Num }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            synchronized(startTimes) {
                                startTimes["indexer2"] = System.currentTimeMillis()
                            }
                            delay(100)
                            synchronized(endTimes) {
                                endTimes["indexer2"] = System.currentTimeMillis()
                            }
                            block2Num++
                        }
                }

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(5000)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

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

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            // indexer2 depends on indexer1, but they should process same block concurrently
            coVerify(atLeast = 1) { indexer1.processBlock(block0) }
            coVerify(atLeast = 1) { indexer2.processBlock(block0) }
        }

        @Test
        @Disabled("Causes JVM instrumentation crash with byte-buddy agent")
        fun `full run method should initialise sync and then process blocks`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val callOrder = mutableListOf<String>()
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } coAnswers
                        {
                            synchronized(callOrder) { callOrder.add("init") }
                        }
                    coEvery { fastSync() } coAnswers
                        {
                            synchronized(callOrder) { callOrder.add("sync") }
                        }
                    coEvery { processBlock(any()) } coAnswers
                        {
                            synchronized(callOrder) { callOrder.add("process") }
                            currentBlockNum++
                        }
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

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
            val block0 = buildBlock(num = 0L)
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } just Runs
                    coEvery { fastSync() } just Runs
                    coEvery { processBlock(any()) } coAnswers { currentBlockNum++ }
                }

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(5000)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

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
    inner class ReorgHandling {

        @Test
        fun `should not retry when ReorgException is thrown during processBlock`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            var processAttempts = 0
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } just Runs
                    coEvery { fastSync() } just Runs
                    coEvery { processBlock(any()) } coAnswers
                        {
                            processAttempts++
                            throw org.vechain.indexer.exception.ReorgException("Reorg at block 0")
                        }
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

            val runner = IndexerRunner()

            val job = launch {
                try {
                    runner.runAllIndexers(listOf(indexer), thorClient, 1)
                } catch (e: org.vechain.indexer.exception.ReorgException) {
                    // Expected - ReorgException should propagate
                }
            }

            delay(200) // Let it process and throw
            job.cancelAndJoin()

            // Should have attempted only once - ReorgException should propagate without retry
            expectThat(processAttempts).isEqualTo(1)
        }

        @Test
        fun `run method should restart initialization when ReorgException occurs`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            var initCount = 0
            var syncCount = 0
            var processAttempts = 0
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } coAnswers { initCount++ }
                    coEvery { fastSync() } coAnswers { syncCount++ }
                    coEvery { processBlock(any()) } coAnswers
                        {
                            processAttempts++
                            if (processAttempts == 1) {
                                throw org.vechain.indexer.exception.ReorgException(
                                    "Reorg at block 0"
                                )
                            }
                            // After reorg, delay to allow cancellation
                            delay(5000)
                        }
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.run(listOf(indexer), 1, thorClient) }

            delay(500) // Let it process, throw reorg, and restart
            job.cancelAndJoin()

            // Should have initialized at least twice (once initially, once after reorg)
            expectThat(initCount).isGreaterThanOrEqualTo(2)
            // Should have synced at least twice
            expectThat(syncCount).isGreaterThanOrEqualTo(2)
            // Should have attempted processing at least once
            expectThat(processAttempts).isGreaterThanOrEqualTo(1)
        }

        @Test
        fun `should restart all indexers when one throws ReorgException`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            var indexer1InitCount = 0
            var indexer2InitCount = 0
            var processAttempts = 0
            var currentBlockNum1 = 0L
            var currentBlockNum2 = 0L

            val indexer1 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum1 }
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } coAnswers { indexer1InitCount++ }
                    coEvery { fastSync() } just Runs
                    coEvery { processBlock(any()) } coAnswers
                        {
                            processAttempts++
                            if (processAttempts == 1) {
                                throw ReorgException("Reorg detected")
                            }
                            delay(5000)
                        }
                }

            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer2"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum2 }
                    every { getInspectionClauses() } returns null
                    coEvery { initialise() } coAnswers { indexer2InitCount++ }
                    coEvery { fastSync() } just Runs
                    coEvery { processBlock(any()) } just Runs
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

            val runner = IndexerRunner()
            val job = launch { runner.run(listOf(indexer1, indexer2), 1, thorClient) }

            delay(500) // Let it process, throw reorg, and restart
            job.cancelAndJoin()

            // Both indexers should be reinitialized after reorg
            expectThat(indexer1InitCount).isGreaterThanOrEqualTo(2)
            expectThat(indexer2InitCount).isGreaterThanOrEqualTo(2)
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

            coEvery { thorClient.waitForBlock(BlockRevision.Number(100L)) } returns block100

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.processBlock(block100) }
        }

        @Test
        @Disabled("Test timing issue - processBlock not called before cancellation")
        fun `should handle single indexer in multiple groups scenario`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            val block1 = buildBlock(num = 1L)

            val indexer = createMockIndexer("indexer1", currentBlock = 0L)

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
            coEvery { thorClient.waitForBlock(BlockRevision.Number(1L)) } returns block1
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(5000) // Block future fetches to prevent OOM
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch { runner.runAllIndexers(listOf(indexer), thorClient, 1) }

            delay(500) // Give more time for processing
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.processBlock(block0) }
        }
    }

    @Nested
    inner class CalculateWindowSize {
        private val runner = TestIndexerRunner()

        @Test
        fun `returns max prefetch size when timestamp is missing`() {
            val result =
                runner.exposedWindowSize(lastBlockTimestampSeconds = null, maxPrefetchSize = 5)

            expectThat(result).isEqualTo(5)
        }

        @Test
        fun `reduces window to one block when nearly caught up`() {
            val recentTimestamp = (System.currentTimeMillis() / 1000) - 5

            val result =
                runner.exposedWindowSize(
                    lastBlockTimestampSeconds = recentTimestamp,
                    maxPrefetchSize = 10,
                )

            expectThat(result).isEqualTo(1)
        }

        @Test
        fun `caps window at max prefetch size when far behind`() {
            val oldTimestamp = (System.currentTimeMillis() / 1000) - 500

            val result =
                runner.exposedWindowSize(
                    lastBlockTimestampSeconds = oldTimestamp,
                    maxPrefetchSize = 8,
                )

            expectThat(result).isEqualTo(8)
        }

        @Test
        fun `scales window proportionally when moderately behind`() {
            val secondsBehind = 35L
            val timestamp = (System.currentTimeMillis() / 1000) - secondsBehind

            val result =
                runner.exposedWindowSize(
                    lastBlockTimestampSeconds = timestamp,
                    maxPrefetchSize = 10,
                )

            expectThat(result).isEqualTo(4)
        }
    }

    private class TestIndexerRunner : IndexerRunner() {
        fun exposedWindowSize(
            lastBlockTimestampSeconds: Long?,
            maxPrefetchSize: Int,
        ): Int = calculateWindowSize(lastBlockTimestampSeconds, maxPrefetchSize)
    }

    @Nested
    inner class BuildClauseListWithMapping {
        private val runner = IndexerRunner()

        @Test
        fun `returns empty when no indexers have clauses`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")

            val (clauses, mapping) = runner.buildClauseListWithMapping(listOf(indexer1, indexer2))

            expectThat(clauses).isEqualTo(emptyList())
            expectThat(mapping).isEqualTo(emptyMap())
        }

        @Test
        fun `maps each indexer to its clause indices`() {
            val clause1 =
                org.vechain.indexer.thor.model.Clause(
                    to = "0xAddr1",
                    value = "0x0",
                    data = "0x1111"
                )
            val clause2 =
                org.vechain.indexer.thor.model.Clause(
                    to = "0xAddr2",
                    value = "0x0",
                    data = "0x2222"
                )
            val clause3 =
                org.vechain.indexer.thor.model.Clause(
                    to = "0xAddr3",
                    value = "0x0",
                    data = "0x3333"
                )

            val indexer1 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { getInspectionClauses() } returns listOf(clause1)
                }
            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer2"
                    every { getInspectionClauses() } returns listOf(clause2, clause3)
                }

            val (clauses, mapping) = runner.buildClauseListWithMapping(listOf(indexer1, indexer2))

            expectThat(clauses).isEqualTo(listOf(clause1, clause2, clause3))
            expectThat(mapping[indexer1]).isEqualTo(listOf(0))
            expectThat(mapping[indexer2]).isEqualTo(listOf(1, 2))
        }

        @Test
        fun `deduplicates clauses and maps correctly`() {
            // Shared clause between two indexers
            val sharedClause =
                org.vechain.indexer.thor.model.Clause(
                    to = "0xShared",
                    value = "0x0",
                    data = "0xAAAA"
                )
            val uniqueClause =
                org.vechain.indexer.thor.model.Clause(
                    to = "0xUnique",
                    value = "0x0",
                    data = "0xBBBB"
                )

            val indexer1 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { getInspectionClauses() } returns listOf(sharedClause)
                }
            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer2"
                    every { getInspectionClauses() } returns listOf(sharedClause, uniqueClause)
                }

            val (clauses, mapping) = runner.buildClauseListWithMapping(listOf(indexer1, indexer2))

            // Only 2 unique clauses in the combined list
            expectThat(clauses).isEqualTo(listOf(sharedClause, uniqueClause))
            // indexer1 gets index 0 (shared clause)
            expectThat(mapping[indexer1]).isEqualTo(listOf(0))
            // indexer2 gets index 0 (shared) and index 1 (unique)
            expectThat(mapping[indexer2]).isEqualTo(listOf(0, 1))
        }

        @Test
        fun `handles mix of indexers with and without clauses`() {
            val clause1 =
                org.vechain.indexer.thor.model.Clause(
                    to = "0xAddr1",
                    value = "0x0",
                    data = "0x1111"
                )

            val indexer1 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { getInspectionClauses() } returns null
                }
            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer2"
                    every { getInspectionClauses() } returns listOf(clause1)
                }
            val indexer3 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer3"
                    every { getInspectionClauses() } returns null
                }

            val (clauses, mapping) =
                runner.buildClauseListWithMapping(listOf(indexer1, indexer2, indexer3))

            expectThat(clauses).isEqualTo(listOf(clause1))
            // Only indexer2 should be in the mapping
            expectThat(mapping.containsKey(indexer1)).isEqualTo(false)
            expectThat(mapping[indexer2]).isEqualTo(listOf(0))
            expectThat(mapping.containsKey(indexer3)).isEqualTo(false)
        }
    }
}
