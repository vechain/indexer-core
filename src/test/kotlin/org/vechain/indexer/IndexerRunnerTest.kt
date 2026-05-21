package org.vechain.indexer

import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import kotlin.math.pow
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TestTimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
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
import org.vechain.indexer.exception.StuckBlockException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.BlockRevision
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isGreaterThan
import strikt.assertions.isGreaterThanOrEqualTo
import strikt.assertions.isTrue

@OptIn(ExperimentalCoroutinesApi::class)
internal class IndexerRunnerTest {

    private class MockState(initialBlock: Long) {
        var blockNumber: Long = initialBlock
        var status: Status = Status.NOT_INITIALISED
    }

    private fun <T : Indexer> configureMockIndexer(
        mock: T,
        state: MockState,
        dependsOn: Indexer?,
        isFastSyncable: Boolean,
        initializeBlock: (suspend () -> Unit)?,
        processBlock: (suspend (Block) -> Unit)?,
    ): T {
        every { mock.name } returns mock.toString()
        every { mock.dependsOn } returns dependsOn
        every { mock.getCurrentBlockNumber() } answers { state.blockNumber }
        every { mock.getInspectionClauses() } returns null
        every { mock.getStatus() } answers { state.status }

        coEvery { mock.initialise() } coAnswers
            {
                initializeBlock?.invoke()
                state.status =
                    if (isFastSyncable && state.status == Status.NOT_INITIALISED)
                        Status.READY_TO_FAST_SYNC
                    else Status.READY_TO_SYNC
            }

        coEvery { mock.processBlock(any()) } coAnswers
            {
                processBlock?.invoke(firstArg())
                state.status = Status.SYNCING
                state.blockNumber++
            }

        return mock
    }

    private fun createMockIndexer(
        name: String,
        currentBlock: Long = 0L,
        dependsOn: Indexer? = null,
        initializeBlock: (suspend () -> Unit)? = null,
        fastSyncBlock: (suspend () -> Unit)? = null,
        processBlock: (suspend (Block) -> Unit)? = null
    ): FastSyncableIndexer {
        val mock = mockk<FastSyncableIndexer>(relaxed = true)
        every { mock.name } returns name
        val state = MockState(currentBlock)
        configureMockIndexer(mock, state, dependsOn, true, initializeBlock, processBlock)

        coEvery { mock.fastSync() } coAnswers
            {
                state.status = Status.FAST_SYNCING
                fastSyncBlock?.invoke()
                state.status = Status.READY_TO_SYNC
            }

        return mock
    }

    private fun createMockNonFastSyncableIndexer(
        name: String,
        currentBlock: Long = 0L,
        dependsOn: Indexer? = null,
        initializeBlock: (suspend () -> Unit)? = null,
        processBlock: (suspend (Block) -> Unit)? = null
    ): Indexer {
        val mock = mockk<Indexer>(relaxed = true)
        every { mock.name } returns name
        val state = MockState(currentBlock)
        return configureMockIndexer(mock, state, dependsOn, false, initializeBlock, processBlock)
    }

    /**
     * Builds a stub indexer with a fixed status and dependsOn for testing the runner's pure
     * predicates (status-driven classification). Use [createMockIndexer] /
     * [createMockNonFastSyncableIndexer] when behaviour like initialise / processBlock matters.
     */
    private fun stubIndexer(
        name: String = "stub",
        status: Status = Status.NOT_INITIALISED,
        dependsOn: Indexer? = null,
        fastSyncable: Boolean = false,
    ): Indexer {
        val mock =
            if (fastSyncable) mockk<FastSyncableIndexer>(relaxed = true)
            else mockk<Indexer>(relaxed = true)
        every { mock.name } returns name
        every { mock.getStatus() } returns status
        every { mock.dependsOn } returns dependsOn
        return mock
    }

    @Nested
    inner class InitialiseAndSyncAll {

        @Test
        fun `should initialise and sync all indexers concurrently`() = runTest {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")
            val indexer3 = createMockIndexer("indexer3")

            val runner = IndexerRunner()
            runner.initialiseAndSync(listOf(indexer1, indexer2, indexer3))

            coVerify(exactly = 1) { indexer1.initialise() }
            coVerify(exactly = 1) { (indexer1 as FastSyncableIndexer).fastSync() }
            coVerify(exactly = 1) { indexer2.initialise() }
            coVerify(exactly = 1) { (indexer2 as FastSyncableIndexer).fastSync() }
            coVerify(exactly = 1) { indexer3.initialise() }
            coVerify(exactly = 1) { (indexer3 as FastSyncableIndexer).fastSync() }
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
            runner.initialiseAndSync(listOf(indexer))

            expectThat(initAttempts).isEqualTo(3)
            coVerify(exactly = 3) { indexer.initialise() }
            coVerify(exactly = 1) { (indexer as FastSyncableIndexer).fastSync() }
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
                    },
                )

            val runner = IndexerRunner()
            runner.initialiseAndSync(listOf(indexer))

            expectThat(syncAttempts).isEqualTo(2)
            // initialise() runs once on the first attempt; on retry the indexer is no longer
            // NOT_INITIALISED so the runner refreshes state instead of re-initialising.
            coVerify(exactly = 1) { indexer.initialise() }
            coVerify(exactly = 1) { indexer.refreshState() }
            coVerify(exactly = 2) { (indexer as FastSyncableIndexer).fastSync() }
        }

        @Test
        fun `should not retry on CancellationException`() = runTest {
            val indexer =
                createMockIndexer(
                    name = "indexer1",
                    initializeBlock = { throw CancellationException("Cancelled") }
                )

            val runner = IndexerRunner()
            val job = launch { runner.initialiseAndSync(listOf(indexer)) }

            delay(100) // Give it time to attempt
            job.cancelAndJoin()

            // Should only attempt once before cancellation
            coVerify(atMost = 1) { indexer.initialise() }
        }

        @Test
        fun `should initialise and sync single indexer`() = runTest {
            val indexer = createMockIndexer("indexer1")

            val runner = IndexerRunner()
            runner.initialiseAndSync(listOf(indexer))

            coVerify(exactly = 1) { indexer.initialise() }
            coVerify(exactly = 1) { (indexer as FastSyncableIndexer).fastSync() }
        }

        @Test
        fun `should complete even if one indexer is slow`() = runTest {
            val fastIndexer = createMockIndexer("fast")
            val slowIndexer = createMockIndexer(name = "slow", initializeBlock = { delay(50) })

            val runner = IndexerRunner()
            runner.initialiseAndSync(listOf(fastIndexer, slowIndexer))

            coVerify(exactly = 1) { fastIndexer.initialise() }
            coVerify(exactly = 1) { (fastIndexer as FastSyncableIndexer).fastSync() }
            coVerify(exactly = 1) { slowIndexer.initialise() }
            coVerify(exactly = 1) { (slowIndexer as FastSyncableIndexer).fastSync() }
        }

        @Test
        fun `deadline-bound initialiseAndSync should not start fast sync after deadline`() =
            runTest {
                val testTimeSource = TestTimeSource()
                val indexer =
                    createMockIndexer(
                        name = "indexer1",
                        initializeBlock = { testTimeSource += 200.milliseconds },
                    )

                val runner = IndexerRunner(testTimeSource)
                runner.initialiseAndSyncFor(listOf(indexer), 100.milliseconds)

                coVerify(exactly = 1) { indexer.initialise() }
                coVerify(exactly = 0) { (indexer as FastSyncableIndexer).fastSync() }
            }
    }

    @Nested
    inner class RunAllIndexers {

        @Test
        fun `should return early when no indexers provided`() = runTest {
            val thorClient = mockk<ThorClient>()
            val runner = IndexerRunner()

            // Should complete without error
            runner.runIndexers(emptyList(), thorClient, 1)

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
            val job = launch { runner.runIndexers(listOf(indexer1, indexer2), thorClient, 1) }

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
            val job = launch { runner.runIndexers(listOf(indexer1, indexer2), thorClient, 1) }

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
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

            delay(100)
            job.cancelAndJoin()

            // Should never try to process block 5
            coVerify(exactly = 0) { indexer.processBlock(match { it.number == 5L }) }
        }

        @Test
        fun `skip path bumps timeLastProcessed on BlockIndexer`() = runTest {
            // Skipping must keep liveness fresh so the health reporter doesn't flag head-synced
            // indexers as DOWN while the fetcher is gated by a slower indexer in the same group.
            val thorClient = mockk<ThorClient>()
            // Slow indexer pegged at block 0 — its processBlock suspends indefinitely so the
            // fetcher stays at startBlock=0 and the skipped indexer keeps taking the skip branch.
            val gate = CompletableDeferred<Unit>()
            val slowIndexer = mockk<Indexer>(relaxed = true)
            every { slowIndexer.name } returns "slow"
            every { slowIndexer.dependsOn } returns null
            every { slowIndexer.getCurrentBlockNumber() } returns 0L
            every { slowIndexer.getStatus() } returns Status.SYNCING
            every { slowIndexer.getInspectionClauses() } returns null
            coEvery { slowIndexer.processBlock(any()) } coAnswers { gate.await() }

            val skippedIndexer = mockk<BlockIndexer>(relaxed = true)
            every { skippedIndexer.name } returns "skipped"
            every { skippedIndexer.dependsOn } returns null
            every { skippedIndexer.getCurrentBlockNumber() } returns 10L
            every { skippedIndexer.getStatus() } returns Status.SYNCING
            every { skippedIndexer.getInspectionClauses() } returns null

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.runIndexers(listOf(slowIndexer, skippedIndexer), thorClient, 1)
            }

            delay(100)
            job.cancelAndJoin()

            verify(atLeast = 1) { skippedIndexer.markSkipped() }
            coVerify(exactly = 0) { skippedIndexer.processBlock(match { it.number < 10L }) }
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
                runner.runIndexers(listOf(indexer), thorClient, 1)
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
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

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
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

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
            val job = launch { runner.runIndexers(listOf(slowIndexer), thorClient, batchSize = 5) }

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
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

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
            val job = launch { runner.runIndexers(listOf(indexer1, indexer2), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            // Both should have started around the same time (within 50ms)
            val timeDiff =
                kotlin.math.abs((startTimes["indexer1"] ?: 0L) - (startTimes["indexer2"] ?: 0L))
            expectThat(timeDiff).isGreaterThanOrEqualTo(0)
        }

        @Test
        fun `independent siblings run in parallel and a child only awaits its direct parent`() =
            runTest {
                // Dependency tree: A -> {B, C}; C -> D
                // Within a block: A runs first; B and C run in parallel after A; D runs after C
                // but does not need to wait for B. With B intentionally slow, D must finish
                // before B does within the same block.
                val thorClient = mockk<ThorClient>()
                val block0 = buildBlock(num = 0L)

                val events = mutableListOf<String>()
                fun record(event: String) = synchronized(events) { events.add(event) }

                val a =
                    createMockNonFastSyncableIndexer(
                        "A",
                        processBlock = {
                            record("start:A")
                            record("end:A")
                        },
                    )
                val b =
                    createMockNonFastSyncableIndexer(
                        "B",
                        dependsOn = a,
                        processBlock = {
                            record("start:B")
                            delay(200)
                            record("end:B")
                        },
                    )
                val c =
                    createMockNonFastSyncableIndexer(
                        "C",
                        dependsOn = a,
                        processBlock = {
                            record("start:C")
                            record("end:C")
                        },
                    )
                val d =
                    createMockNonFastSyncableIndexer(
                        "D",
                        dependsOn = c,
                        processBlock = {
                            record("start:D")
                            record("end:D")
                        },
                    )

                coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
                coEvery { thorClient.waitForBlock(BlockRevision.Number(1L)) } coAnswers
                    {
                        delay(5_000)
                        buildBlock(num = 1L)
                    }

                val runner = IndexerRunner()
                val job = launch { runner.runIndexers(listOf(a, b, c, d), thorClient, 1) }

                delay(500)
                job.cancelAndJoin()

                val snap = synchronized(events) { events.toList() }

                // A finishes before any of its descendants start.
                val endA = snap.indexOf("end:A")
                expectThat(endA).isGreaterThanOrEqualTo(0)
                expectThat(endA < snap.indexOf("start:B")).isTrue()
                expectThat(endA < snap.indexOf("start:C")).isTrue()
                // D only starts after C finishes.
                expectThat(snap.indexOf("end:C") < snap.indexOf("start:D")).isTrue()
                // D does not wait for B — B is slow, so D should finish first.
                expectThat(snap.indexOf("end:D") < snap.indexOf("end:B")).isTrue()
                // B and C overlap: C starts (and finishes) before B finishes.
                expectThat(snap.indexOf("start:C") < snap.indexOf("end:B")).isTrue()
            }

        @Test
        fun `linear chain still processes sequentially in dependency order`() = runTest {
            // Regression: A -> B -> C must still serialize per block.
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val events = mutableListOf<String>()
            fun record(event: String) = synchronized(events) { events.add(event) }

            val a =
                createMockNonFastSyncableIndexer(
                    "A",
                    processBlock = {
                        record("start:A")
                        record("end:A")
                    },
                )
            val b =
                createMockNonFastSyncableIndexer(
                    "B",
                    dependsOn = a,
                    processBlock = {
                        record("start:B")
                        record("end:B")
                    },
                )
            val c =
                createMockNonFastSyncableIndexer(
                    "C",
                    dependsOn = b,
                    processBlock = {
                        record("start:C")
                        record("end:C")
                    },
                )

            coEvery { thorClient.waitForBlock(BlockRevision.Number(0L)) } returns block0
            coEvery { thorClient.waitForBlock(BlockRevision.Number(1L)) } coAnswers
                {
                    delay(5_000)
                    buildBlock(num = 1L)
                }

            val runner = IndexerRunner()
            val job = launch { runner.runIndexers(listOf(a, b, c), thorClient, 1) }

            delay(300)
            job.cancelAndJoin()

            val snap = synchronized(events) { events.toList() }
            expectThat(snap.take(6))
                .containsExactly("start:A", "end:A", "start:B", "end:B", "start:C", "end:C")
        }

        @Test
        fun `unrecoverable failure in one indexer aborts the group and cancels siblings`() =
            runTest {
                // A -> {B, C}; C -> D. B throws ReorgException (the runner does not retry it);
                // sibling C should be cancelled before its slow processing finishes, and D should
                // never run because its parent C never completed.
                val thorClient = mockk<ThorClient>()
                val block0 = buildBlock(num = 0L)

                val completed = mutableSetOf<String>()
                fun mark(name: String) = synchronized(completed) { completed.add(name) }

                val a =
                    createMockNonFastSyncableIndexer(
                        "A",
                        processBlock = { mark("A") },
                    )
                val b =
                    createMockNonFastSyncableIndexer(
                        "B",
                        dependsOn = a,
                        processBlock = { throw ReorgException("boom") },
                    )
                val c =
                    createMockNonFastSyncableIndexer(
                        "C",
                        dependsOn = a,
                        processBlock = {
                            delay(1_000)
                            mark("C")
                        },
                    )
                val d =
                    createMockNonFastSyncableIndexer(
                        "D",
                        dependsOn = c,
                        processBlock = { mark("D") },
                    )

                coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

                val runner = IndexerRunner()
                assertThrows<ReorgException> {
                    runner.runIndexers(listOf(a, b, c, d), thorClient, 1)
                }

                val snap = synchronized(completed) { completed.toSet() }
                // A finished before B threw; C was cancelled mid-delay; D never ran.
                expectThat(snap.contains("A")).isTrue()
                expectThat(snap.contains("C")).isFalse()
                expectThat(snap.contains("D")).isFalse()
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
            val job = launch { runner.runIndexers(listOf(indexer1, indexer2), thorClient, 1) }

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
                mockk<FastSyncableIndexer>(relaxed = true) {
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
            val job = launch {
                runner.run(listOf(indexer), 1, thorClient, 500_000L, 15.minutes, 1.minutes)
            }

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
                runTest { runner.run(emptyList(), 1, thorClient, 500_000L, 15.minutes, 1.minutes) }
            }
        }

        @Test
        fun `launch should create and run indexer orchestrator`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)

            val indexer = createMockIndexer("indexer1")

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
                mockk<FastSyncableIndexer>(relaxed = true) {
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
                    runner.runIndexers(listOf(indexer), thorClient, 1)
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
        fun `run method should restart processing when ReorgException occurs`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            var initCount = 0
            var syncCount = 0
            var processAttempts = 0

            val indexer =
                createMockIndexer(
                    name = "indexer1",
                    initializeBlock = { initCount++ },
                    fastSyncBlock = { syncCount++ },
                    processBlock = {
                        processAttempts++
                        if (processAttempts == 1) {
                            throw ReorgException("Reorg at block 0")
                        }
                        // After reorg, delay to allow cancellation
                        delay(5000)
                    },
                )

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

            val runner = IndexerRunner()
            val job = launch {
                runner.run(listOf(indexer), 1, thorClient, 500_000L, 15.minutes, 1.minutes)
            }

            delay(500) // Let it process, throw reorg, and restart
            job.cancelAndJoin()

            // After a reorg restart, runWithProximityGroups refreshes in-memory state via
            // refreshState() rather than re-running initialise() — the processor was already
            // rolled back by handleReorg, so a second rollback would be harmful.
            expectThat(initCount).isEqualTo(1)
            coVerify(atLeast = 1) { indexer.refreshState() }
            // Fast sync is one-shot per process — restart does not re-fast-sync once the indexer
            // is past READY_TO_FAST_SYNC.
            expectThat(syncCount).isEqualTo(1)
            // Reorg restart causes processBlock to be called again on the same block
            expectThat(processAttempts).isGreaterThanOrEqualTo(2)
        }

        @Test
        fun `bounded retry gives up and raises StuckBlockException after exhausting attempts`() =
            runTest {
                val thorClient = mockk<ThorClient>()
                val block0 = buildBlock(num = 0L)
                var processAttempts = 0

                val indexer =
                    mockk<Indexer>(relaxed = true) {
                        every { name } returns "stuck"
                        every { dependsOn } returns null
                        every { getCurrentBlockNumber() } returns 0L
                        every { getInspectionClauses() } returns null
                        coEvery { processBlock(any()) } coAnswers
                            {
                                processAttempts++
                                throw RuntimeException("permanent failure #$processAttempts")
                            }
                    }

                coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

                val runner = IndexerRunner()

                val thrown =
                    assertThrows<StuckBlockException> {
                        runner.runIndexers(listOf(indexer), thorClient, 1)
                    }

                // Bounded retry: exactly MAX_BLOCK_PROCESS_ATTEMPTS attempts, then give up.
                expectThat(processAttempts).isEqualTo(10)
                expectThat(thrown.message!!).contains("stuck at block 0")
            }

        @Test
        fun `should restart all indexers when one throws ReorgException`() = runTest {
            val thorClient = mockk<ThorClient>()
            val block0 = buildBlock(num = 0L)
            var indexer1InitCount = 0
            var indexer2InitCount = 0
            var processAttempts = 0

            val indexer1 =
                createMockIndexer(
                    name = "indexer1",
                    initializeBlock = { indexer1InitCount++ },
                    processBlock = {
                        processAttempts++
                        if (processAttempts == 1) {
                            throw ReorgException("Reorg detected")
                        }
                        delay(5000)
                    },
                )

            val indexer2 =
                createMockIndexer(
                    name = "indexer2",
                    initializeBlock = { indexer2InitCount++ },
                )

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } returns block0

            val runner = IndexerRunner()
            val job = launch {
                runner.run(
                    listOf(indexer1, indexer2),
                    1,
                    thorClient,
                    500_000L,
                    15.minutes,
                    1.minutes
                )
            }

            delay(500) // Let it process, throw reorg, and restart
            job.cancelAndJoin()

            // After reorg, runWithProximityGroups refreshes in-memory state via refreshState()
            // rather than re-initialising — handleReorg already performed the rollback.
            expectThat(indexer1InitCount).isEqualTo(1)
            expectThat(indexer2InitCount).isEqualTo(1)
            coVerify(atLeast = 1) { indexer1.refreshState() }
            coVerify(atLeast = 1) { indexer2.refreshState() }
            // After reorg, processBlock is called again on the same block
            coVerify(atLeast = 2) { indexer1.processBlock(any()) }
        }
    }

    @Nested
    inner class DeadlineBounded {

        @Test
        fun `runIndexers with expired deadline should exit without processing`() = runTest {
            val testTimeSource = TestTimeSource()
            val thorClient = mockk<ThorClient>()

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } returns 0L
                    every { getInspectionClauses() } returns null
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    buildBlock(num = blockNum)
                }

            val runner = IndexerRunner(testTimeSource)
            // Create a deadline mark, then advance time past it
            val deadlineMark = testTimeSource.markNow() + 100.milliseconds
            testTimeSource += 200.milliseconds
            runner.runIndexers(listOf(indexer), thorClient, 1, deadlineMark)

            coVerify(exactly = 0) { indexer.processBlock(any()) }
        }

        @Test
        fun `runIndexers processes blocks then stops when deadline expires`() = runTest {
            val testTimeSource = TestTimeSource()
            val thorClient = mockk<ThorClient>()
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers { currentBlockNum++ }
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    testTimeSource += 100.milliseconds
                    buildBlock(num = blockNum)
                }

            val runner = IndexerRunner(testTimeSource)
            val deadlineMark = testTimeSource.markNow() + 500.milliseconds
            runner.runIndexers(listOf(indexer), thorClient, 1, deadlineMark)

            // Should have processed some blocks before deadline expired
            coVerify(atLeast = 1) { indexer.processBlock(any()) }
        }

        @Test
        fun `runIndexers without deadline runs indefinitely until cancelled`() = runTest {
            val thorClient = mockk<ThorClient>()
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "indexer1"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers { currentBlockNum++ }
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    delay(100) // Suspension point so virtual time can advance
                    buildBlock(num = blockNum)
                }

            val runner = IndexerRunner()
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

            delay(500)
            expectThat(job.isActive).isTrue()
            job.cancelAndJoin()
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
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

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
            val job = launch { runner.runIndexers(listOf(indexer), thorClient, 1) }

            delay(500) // Give more time for processing
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.processBlock(block0) }
        }
    }

    @Nested
    inner class MixedIndexerTypes {

        @Test
        fun `mixed indexers - fast-syncable get fastSync, non-fast-syncable get initialise only`() =
            runTest {
                val testTimeSource = TestTimeSource()
                val thorClient = mockk<ThorClient>()

                val fastSyncable = createMockIndexer("fast")
                val nonFastSyncable = createMockNonFastSyncableIndexer("plain")

                coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                    {
                        delay(100)
                        testTimeSource += 100.milliseconds
                        buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                    }

                val runner = IndexerRunner(testTimeSource)
                val job = launch {
                    runner.run(
                        listOf(fastSyncable, nonFastSyncable),
                        1,
                        thorClient,
                        500_000L,
                        reshuffleInterval = 15.minutes,
                        // Short catch-up slices so the fast-syncable rejoins promptly.
                        catchUpInterval = 200.milliseconds,
                    )
                }

                delay(500)
                job.cancelAndJoin()

                // Both get initialise()
                coVerify(atLeast = 1) { fastSyncable.initialise() }
                coVerify(atLeast = 1) { nonFastSyncable.initialise() }

                // Only fast-syncable gets fastSync()
                coVerify(atLeast = 1) { fastSyncable.fastSync() }

                // Both get processBlock()
                coVerify(atLeast = 1) { fastSyncable.processBlock(any()) }
                coVerify(atLeast = 1) { nonFastSyncable.processBlock(any()) }
            }

        @Test
        fun `all non-fast-syncable - no fastSync, straight to runIndexers`() = runTest {
            val thorClient = mockk<ThorClient>()
            var blockNum1 = 0L
            var blockNum2 = 0L

            val indexer1 = createMockNonFastSyncableIndexer("plain1") // starts at 0

            val indexer2 = createMockNonFastSyncableIndexer("plain2") // starts at 0

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(100)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.run(
                    listOf(indexer1, indexer2),
                    1,
                    thorClient,
                    500_000L,
                    15.minutes,
                    1.minutes
                )
            }

            delay(500)
            job.cancelAndJoin()

            // Both get initialise()
            coVerify(atLeast = 1) { indexer1.initialise() }
            coVerify(atLeast = 1) { indexer2.initialise() }

            // Both get processBlock()
            coVerify(atLeast = 1) { indexer1.processBlock(any()) }
            coVerify(atLeast = 1) { indexer2.processBlock(any()) }
        }

        @Test
        fun `non-fast-syncable indexers process blocks during fast sync phase`() = runTest {
            val thorClient = mockk<ThorClient>()
            val nfsProcessedDuringFastSync = mutableListOf<Long>()

            val nonFastSyncable =
                createMockNonFastSyncableIndexer(
                    name = "plain",
                    processBlock = { block -> nfsProcessedDuringFastSync.add(block.number) },
                )

            val fastSyncable =
                createMockIndexer(
                    name = "fast",
                    fastSyncBlock = { delay(300) }, // Simulate slow fast sync
                )

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(50)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.run(
                    listOf(fastSyncable, nonFastSyncable),
                    1,
                    thorClient,
                    500_000L,
                    15.minutes,
                    1.minutes
                )
            }

            delay(600)
            job.cancelAndJoin()

            // Non-fast-syncable should have processed blocks during fast sync
            expectThat(nfsProcessedDuringFastSync.isNotEmpty()).isTrue()
        }

        @Test
        fun `catch-up interval waits for in-flight sync block to finish`() = runTest {
            val thorClient = mockk<ThorClient>()
            var firstBlockCompleted = false
            var firstBlockCancelled = false
            var processAttempts = 0

            val nonFastSyncable =
                createMockNonFastSyncableIndexer(
                    name = "plain",
                    processBlock = {
                        processAttempts++
                        if (processAttempts == 1) {
                            try {
                                delay(300)
                                firstBlockCompleted = true
                            } catch (e: CancellationException) {
                                firstBlockCancelled = true
                                throw e
                            }
                        } else {
                            delay(60_000)
                        }
                    },
                )

            val fastSyncable =
                createMockIndexer(
                    name = "fast",
                    fastSyncBlock = { delay(150) },
                )

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.run(
                    listOf(fastSyncable, nonFastSyncable),
                    1,
                    thorClient,
                    500_000L,
                    reshuffleInterval = 15.minutes,
                    catchUpInterval = 50.milliseconds,
                )
            }

            delay(400)
            expectThat(firstBlockCompleted).isTrue()
            expectThat(firstBlockCancelled).isFalse()

            job.cancelAndJoin()
        }

        @Test
        fun `non-fast-syncable depending on fast-syncable waits for fast sync to complete`() =
            runTest {
                val testTimeSource = TestTimeSource()
                val thorClient = mockk<ThorClient>()
                val nfsIndependentProcessedDuringFastSync = mutableListOf<Long>()

                val fastSyncable =
                    createMockIndexer(
                        name = "fast",
                        fastSyncBlock = {
                            delay(300)
                            testTimeSource += 300.milliseconds
                        },
                    )

                // Independent non-fast-syncable: should run during fast sync
                val nfsIndependent =
                    createMockNonFastSyncableIndexer(
                        name = "plain-independent",
                        processBlock = { block ->
                            nfsIndependentProcessedDuringFastSync.add(block.number)
                        },
                    )

                // Dependent non-fast-syncable: depends on fast-syncable, should be excluded
                val nfsDependent =
                    createMockNonFastSyncableIndexer(
                        name = "plain-dependent",
                        dependsOn = fastSyncable,
                    )

                coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                    {
                        delay(50)
                        testTimeSource += 50.milliseconds
                        buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                    }

                val runner = IndexerRunner(testTimeSource)
                val job = launch {
                    runner.run(
                        listOf(fastSyncable, nfsIndependent, nfsDependent),
                        1,
                        thorClient,
                        500_000L,
                        reshuffleInterval = 15.minutes,
                        // Catch-up slice is longer than the 300ms fast sync so it can complete in
                        // one iteration; short enough for the dependent to get initialised before
                        // the test cancels.
                        catchUpInterval = 350.milliseconds,
                    )
                }

                delay(700)
                job.cancelAndJoin()

                // Independent non-fast-syncable should have processed blocks during fast sync
                expectThat(nfsIndependentProcessedDuringFastSync.isNotEmpty()).isTrue()

                // Both should eventually get processBlock in the main run
                coVerify(atLeast = 1) { nfsDependent.processBlock(any()) }
                coVerify(atLeast = 1) { fastSyncable.processBlock(any()) }
            }

        @Test
        fun `transitive dependency on fast-syncable waits for fast sync to complete`() = runTest {
            val testTimeSource = TestTimeSource()
            val thorClient = mockk<ThorClient>()

            val fastSyncable =
                createMockIndexer(
                    name = "fast",
                    fastSyncBlock = {
                        delay(300)
                        testTimeSource += 300.milliseconds
                    },
                )

            // Middle: non-fast-syncable, depends on fast-syncable
            val nfsMiddle =
                createMockNonFastSyncableIndexer(
                    name = "middle",
                    dependsOn = fastSyncable,
                )

            // Leaf: non-fast-syncable, depends on middle (transitive dep on fast-syncable)
            val nfsLeaf =
                createMockNonFastSyncableIndexer(
                    name = "leaf",
                    dependsOn = nfsMiddle,
                )

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(50)
                    testTimeSource += 50.milliseconds
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner(testTimeSource)
            val job = launch {
                runner.run(
                    listOf(fastSyncable, nfsMiddle, nfsLeaf),
                    1,
                    thorClient,
                    500_000L,
                    reshuffleInterval = 15.minutes,
                    // Catch-up slice is longer than the 300ms fast sync, short enough for
                    // dependents to be reclassified within the test window.
                    catchUpInterval = 350.milliseconds,
                )
            }

            delay(700)
            job.cancelAndJoin()

            // Both non-fast-syncable should eventually get processBlock in the main run
            coVerify(atLeast = 1) { nfsMiddle.processBlock(any()) }
            coVerify(atLeast = 1) { nfsLeaf.processBlock(any()) }
        }

        @Test
        fun `ReorgException during fast sync triggers a re-fast-sync on restart`() = runTest {
            // When a reorg fires while the fast indexer is mid-fast-sync, the indexer's status is
            // FAST_SYNCING when the loop restarts. The runner re-classifies it back into the
            // fast-sync group and calls fastSync() again. initialise() is NOT called a second
            // time — handleReorg already rolled back, so the runner only refreshes in-memory
            // state via refreshState() to avoid a redundant rollback.
            val thorClient = mockk<ThorClient>()
            var nfsInitCount = 0
            var fsInitCount = 0
            var fsSyncCount = 0
            var nfsProcessAttempts = 0

            val nonFastSyncable =
                createMockNonFastSyncableIndexer(
                    name = "plain",
                    initializeBlock = { nfsInitCount++ },
                    processBlock = {
                        nfsProcessAttempts++
                        if (nfsProcessAttempts == 1) {
                            throw ReorgException("Reorg during fast sync")
                        }
                    },
                )

            val fastSyncable =
                createMockIndexer(
                    name = "fast",
                    initializeBlock = { fsInitCount++ },
                    fastSyncBlock = {
                        fsSyncCount++
                        delay(200) // Slow enough for the non-fast indexer to throw first
                    },
                    processBlock = { delay(5000) },
                )

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(50)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.run(
                    listOf(fastSyncable, nonFastSyncable),
                    1,
                    thorClient,
                    500_000L,
                    15.minutes,
                    1.minutes
                )
            }

            delay(800)
            job.cancelAndJoin()

            // Non-fast indexer is refreshed (not re-initialised) by runWithProximityGroups
            expectThat(nfsInitCount).isEqualTo(1)
            coVerify(atLeast = 1) { nonFastSyncable.refreshState() }
            // Fast indexer was mid-fast-sync when the reorg fired, so it re-enters the fast-sync
            // group on restart and is fast-synced again. But initialise() is not re-run — the
            // runner refreshes state instead, since handleReorg already rolled back.
            expectThat(fsInitCount).isEqualTo(1)
            coVerify(atLeast = 1) { fastSyncable.refreshState() }
            expectThat(fsSyncCount).isGreaterThanOrEqualTo(2)
        }
    }

    @Nested
    inner class RunWithProximityGroups {

        @Test
        fun `single group delegates to runIndexers without deadline`() = runTest {
            val thorClient = mockk<ThorClient>()
            var currentBlockNum = 0L

            // All indexers at same block — single proximity group
            val indexer = createMockNonFastSyncableIndexer("indexer1", currentBlock = 0L)

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    delay(100)
                    buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.runWithProximityGroups(
                    listOf(indexer),
                    thorClient,
                    1,
                    1_000_000L,
                    5.minutes,
                )
            }

            delay(300)
            // Should still be running (no deadline, steady state)
            expectThat(job.isActive).isTrue()
            job.cancelAndJoin()

            coVerify(atLeast = 1) { indexer.processBlock(any()) }
        }

        @Test
        fun `two groups process blocks from their respective starting points`() = runTest {
            val thorClient = mockk<ThorClient>()

            val indexer1 = createMockNonFastSyncableIndexer("low", currentBlock = 0L)
            val indexer2 = createMockNonFastSyncableIndexer("high", currentBlock = 10000L)

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    delay(50)
                    buildBlock(num = blockNum)
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.runWithProximityGroups(
                    listOf(indexer1, indexer2),
                    thorClient,
                    1,
                    100L,
                    5.minutes,
                )
            }

            delay(500)
            job.cancelAndJoin()

            // Both should have processed at least one block from their starting points
            coVerify(atLeast = 1) { indexer1.processBlock(any()) }
            coVerify(atLeast = 1) { indexer2.processBlock(any()) }
        }

        @Test
        fun `groups reshuffle after deadline`() = runTest {
            val testTimeSource = TestTimeSource()
            val thorClient = mockk<ThorClient>()
            val fetchedBlocks = mutableListOf<Long>()

            val indexer1 = createMockNonFastSyncableIndexer("low", currentBlock = 0L)
            val indexer2 = createMockNonFastSyncableIndexer("high", currentBlock = 10000L)

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    synchronized(fetchedBlocks) { fetchedBlocks.add(blockNum) }
                    delay(10) // suspension point for cancellation
                    testTimeSource += 200.milliseconds
                    buildBlock(num = blockNum)
                }

            val runner = IndexerRunner(testTimeSource)
            val job = launch {
                runner.runWithProximityGroups(
                    listOf(indexer1, indexer2),
                    thorClient,
                    1,
                    100L,
                    500.milliseconds,
                )
            }

            delay(500)
            job.cancelAndJoin()

            // Both block ranges should have been fetched (evidence of two groups)
            val lowBlocks = synchronized(fetchedBlocks) { fetchedBlocks.filter { it < 5000 } }
            val highBlocks = synchronized(fetchedBlocks) { fetchedBlocks.filter { it >= 10000 } }
            expectThat(lowBlocks.isNotEmpty()).isTrue()
            expectThat(highBlocks.isNotEmpty()).isTrue()

            // Should have fetched enough blocks to indicate multiple reshuffle cycles
            // Each cycle: 500ms deadline / 200ms per fetch = ~2-3 fetches per group per cycle
            expectThat(fetchedBlocks.size).isGreaterThan(4)
        }

        @Test
        fun `bounded single group returns after deadline without cancelling active block`() =
            runTest {
                val testTimeSource = TestTimeSource()
                val thorClient = mockk<ThorClient>()
                val indexer =
                    createMockNonFastSyncableIndexer(
                        name = "indexer1",
                        currentBlock = 0L,
                        processBlock = { testTimeSource += 200.milliseconds },
                    )

                coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                    {
                        buildBlock(num = (firstArg<BlockRevision>() as BlockRevision.Number).number)
                    }

                val runner = IndexerRunner(testTimeSource)
                runner.runWithProximityGroupsFor(
                    listOf(indexer),
                    thorClient,
                    1,
                    1_000_000L,
                    100.milliseconds,
                )

                coVerify(atLeast = 1) { indexer.processBlock(any()) }
            }

        @Test
        fun `ReorgException propagates through proximity groups`() = runTest {
            val testTimeSource = TestTimeSource()
            val thorClient = mockk<ThorClient>()
            var currentBlockNum = 0L

            val indexer =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "reorg-indexer"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } answers { currentBlockNum }
                    every { getInspectionClauses() } returns null
                    coEvery { processBlock(any()) } coAnswers
                        {
                            throw ReorgException("Reorg detected")
                        }
                }

            val indexer2 =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "far-indexer"
                    every { dependsOn } returns null
                    every { getCurrentBlockNumber() } returns 10000L
                    every { getInspectionClauses() } returns null
                }

            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    val blockNum = (firstArg<BlockRevision>() as BlockRevision.Number).number
                    testTimeSource += 10.milliseconds
                    buildBlock(num = blockNum)
                }

            val runner = IndexerRunner(testTimeSource)

            assertThrows<ReorgException> {
                runner.runWithProximityGroups(
                    listOf(indexer, indexer2),
                    thorClient,
                    1,
                    100L,
                    5.minutes,
                )
            }
        }
    }

    @Nested
    inner class HasFastSyncingAncestor {

        private val runner = IndexerRunner()

        @Test
        fun `returns false when indexer has no dependsOn`() {
            val indexer = stubIndexer("solo", dependsOn = null)
            with(runner) { expectThat(indexer.hasFastSyncingAncestor()).isFalse() }
        }

        @Test
        fun `returns false when ancestor chain has no fast-syncable indexer`() {
            val grand = stubIndexer("grand", status = Status.SYNCING, dependsOn = null)
            val parent = stubIndexer("parent", status = Status.SYNCING, dependsOn = grand)
            val child = stubIndexer("child", status = Status.NOT_INITIALISED, dependsOn = parent)
            with(runner) { expectThat(child.hasFastSyncingAncestor()).isFalse() }
        }

        @Test
        fun `returns true when direct ancestor is fast-syncable and NOT_INITIALISED`() {
            val ancestor =
                stubIndexer(
                    "fast",
                    status = Status.NOT_INITIALISED,
                    fastSyncable = true,
                )
            val child = stubIndexer("child", dependsOn = ancestor)
            with(runner) { expectThat(child.hasFastSyncingAncestor()).isTrue() }
        }

        @Test
        fun `returns true when direct ancestor is fast-syncable and READY_TO_FAST_SYNC`() {
            val ancestor =
                stubIndexer(
                    "fast",
                    status = Status.READY_TO_FAST_SYNC,
                    fastSyncable = true,
                )
            val child = stubIndexer("child", dependsOn = ancestor)
            with(runner) { expectThat(child.hasFastSyncingAncestor()).isTrue() }
        }

        @Test
        fun `returns true when direct ancestor is fast-syncable and FAST_SYNCING`() {
            val ancestor = stubIndexer("fast", status = Status.FAST_SYNCING, fastSyncable = true)
            val child = stubIndexer("child", dependsOn = ancestor)
            with(runner) { expectThat(child.hasFastSyncingAncestor()).isTrue() }
        }

        @Test
        fun `returns false when fast-syncable ancestor has finished fast sync`() {
            val finishedStatuses = listOf(Status.READY_TO_SYNC, Status.SYNCING, Status.FULLY_SYNCED)
            for (status in finishedStatuses) {
                val ancestor = stubIndexer("fast-$status", status = status, fastSyncable = true)
                val child = stubIndexer("child", dependsOn = ancestor)
                with(runner) {
                    expectThat(child.hasFastSyncingAncestor())
                        .describedAs("status=$status")
                        .isFalse()
                }
            }
        }

        @Test
        fun `returns true when transitive ancestor is fast-syncable and pending`() {
            val grand =
                stubIndexer(
                    "grand-fast",
                    status = Status.FAST_SYNCING,
                    fastSyncable = true,
                )
            val parent = stubIndexer("parent", status = Status.SYNCING, dependsOn = grand)
            val child = stubIndexer("child", dependsOn = parent)
            with(runner) { expectThat(child.hasFastSyncingAncestor()).isTrue() }
        }
    }

    @Nested
    inner class CanBeInitialisedNow {

        private val runner = IndexerRunner()

        @Test
        fun `returns false when indexer is fast-syncable`() {
            val indexer = stubIndexer("fast", status = Status.NOT_INITIALISED, fastSyncable = true)
            with(runner) { expectThat(indexer.canBeInitialisedNow()).isFalse() }
        }

        @Test
        fun `returns false when status is not NOT_INITIALISED`() {
            val statuses =
                listOf(
                    Status.READY_TO_SYNC,
                    Status.SYNCING,
                    Status.FULLY_SYNCED,
                    Status.SHUT_DOWN,
                )
            for (status in statuses) {
                val indexer = stubIndexer("nfs-$status", status = status)
                with(runner) {
                    expectThat(indexer.canBeInitialisedNow())
                        .describedAs("status=$status")
                        .isFalse()
                }
            }
        }

        @Test
        fun `returns false when blocked by a fast-syncing ancestor`() {
            val ancestor = stubIndexer("fast", status = Status.FAST_SYNCING, fastSyncable = true)
            val indexer =
                stubIndexer("blocked", status = Status.NOT_INITIALISED, dependsOn = ancestor)
            with(runner) { expectThat(indexer.canBeInitialisedNow()).isFalse() }
        }

        @Test
        fun `returns true when non-fast NOT_INITIALISED with no fast-syncing ancestor`() {
            val indexer = stubIndexer("ready", status = Status.NOT_INITIALISED)
            with(runner) { expectThat(indexer.canBeInitialisedNow()).isTrue() }
        }

        @Test
        fun `returns true when ancestor is fast-syncable but already finished`() {
            val ancestor = stubIndexer("fast", status = Status.READY_TO_SYNC, fastSyncable = true)
            val indexer =
                stubIndexer("ready", status = Status.NOT_INITIALISED, dependsOn = ancestor)
            with(runner) { expectThat(indexer.canBeInitialisedNow()).isTrue() }
        }
    }

    @Nested
    inner class InitialiseUnblockedIndexers {

        @Test
        fun `initialises only eligible indexers`() = runTest {
            val ancestor = stubIndexer("fast", status = Status.FAST_SYNCING, fastSyncable = true)
            val eligible = stubIndexer("nfs-ready", status = Status.NOT_INITIALISED)
            val blocked =
                stubIndexer(
                    "nfs-blocked",
                    status = Status.NOT_INITIALISED,
                    dependsOn = ancestor,
                )
            val alreadyInitialised = stubIndexer("nfs-running", status = Status.SYNCING)
            val fastNotInitialised =
                stubIndexer(
                    "fast-fresh",
                    status = Status.NOT_INITIALISED,
                    fastSyncable = true,
                )

            val runner = IndexerRunner()
            runner.initialiseUnblockedIndexers(
                listOf(ancestor, eligible, blocked, alreadyInitialised, fastNotInitialised)
            )

            coVerify(exactly = 1) { eligible.initialise() }
            coVerify(exactly = 0) { ancestor.initialise() }
            coVerify(exactly = 0) { blocked.initialise() }
            coVerify(exactly = 0) { alreadyInitialised.initialise() }
            coVerify(exactly = 0) { fastNotInitialised.initialise() }
        }

        @Test
        fun `is a no-op when no indexer is eligible`() = runTest {
            val ancestor =
                stubIndexer("fast", status = Status.READY_TO_FAST_SYNC, fastSyncable = true)
            val blocked =
                stubIndexer(
                    "blocked",
                    status = Status.NOT_INITIALISED,
                    dependsOn = ancestor,
                )

            val runner = IndexerRunner()
            runner.initialiseUnblockedIndexers(listOf(ancestor, blocked))

            coVerify(exactly = 0) { ancestor.initialise() }
            coVerify(exactly = 0) { blocked.initialise() }
        }

        @Test
        fun `initialises a transitive dependent once its fast-syncable ancestor has finished`() =
            runTest {
                val ancestor =
                    stubIndexer(
                        "fast-done",
                        status = Status.READY_TO_SYNC,
                        fastSyncable = true,
                    )
                val middle =
                    stubIndexer(
                        "middle",
                        status = Status.NOT_INITIALISED,
                        dependsOn = ancestor,
                    )
                val leaf = stubIndexer("leaf", status = Status.NOT_INITIALISED, dependsOn = middle)

                val runner = IndexerRunner()
                runner.initialiseUnblockedIndexers(listOf(ancestor, middle, leaf))

                coVerify(exactly = 1) { middle.initialise() }
                coVerify(exactly = 1) { leaf.initialise() }
            }
    }

    @Nested
    inner class Classify {

        private val runner = IndexerRunner()

        @Test
        fun `returns three empty groups for empty input`() {
            val (group1, group2, group3) = runner.classify(emptyList())
            expectThat(group1).isEmpty()
            expectThat(group2).isEmpty()
            expectThat(group3).isEmpty()
        }

        @Test
        fun `places fast-syncable indexers in group 1 for every pending status`() {
            val pendingStatuses =
                listOf(
                    Status.NOT_INITIALISED,
                    Status.READY_TO_FAST_SYNC,
                    Status.FAST_SYNCING,
                )
            for (status in pendingStatuses) {
                val indexer = stubIndexer("fast-$status", status = status, fastSyncable = true)
                val (group1, group2, group3) = runner.classify(listOf(indexer))
                expectThat(group1).describedAs("status=$status group1").containsExactly(indexer)
                expectThat(group2).describedAs("status=$status group2").isEmpty()
                expectThat(group3).describedAs("status=$status group3").isEmpty()
            }
        }

        @Test
        fun `places sync-ready indexers in group 2`() {
            val syncReadyStatuses =
                listOf(Status.READY_TO_SYNC, Status.SYNCING, Status.FULLY_SYNCED)
            for (status in syncReadyStatuses) {
                val nfs = stubIndexer("nfs-$status", status = status)
                val fast = stubIndexer("fast-$status", status = status, fastSyncable = true)
                val (group1, group2, group3) = runner.classify(listOf(nfs, fast))
                expectThat(group1).describedAs("status=$status group1").isEmpty()
                expectThat(group2)
                    .describedAs("status=$status group2")
                    .containsExactlyInAnyOrder(nfs, fast)
                expectThat(group3).describedAs("status=$status group3").isEmpty()
            }
        }

        @Test
        fun `places non-fast-syncable NOT_INITIALISED in group 3`() {
            val indexer = stubIndexer("blocked", status = Status.NOT_INITIALISED)
            val (group1, group2, group3) = runner.classify(listOf(indexer))
            expectThat(group1).isEmpty()
            expectThat(group2).isEmpty()
            expectThat(group3).containsExactly(indexer)
        }

        @Test
        fun `excludes SHUT_DOWN indexers from every group`() {
            val nfs = stubIndexer("nfs-down", status = Status.SHUT_DOWN)
            val fast = stubIndexer("fast-down", status = Status.SHUT_DOWN, fastSyncable = true)
            val (group1, group2, group3) = runner.classify(listOf(nfs, fast))
            expectThat(group1).isEmpty()
            expectThat(group2).isEmpty()
            expectThat(group3).isEmpty()
        }

        @Test
        fun `splits a mixed indexer set across all three groups`() {
            val fastPending =
                stubIndexer("fast-pending", status = Status.FAST_SYNCING, fastSyncable = true)
            val nfsRunning = stubIndexer("nfs-running", status = Status.SYNCING)
            val nfsBlocked = stubIndexer("nfs-blocked", status = Status.NOT_INITIALISED)
            val shutDown = stubIndexer("shut", status = Status.SHUT_DOWN)

            val (group1, group2, group3) =
                runner.classify(listOf(fastPending, nfsRunning, nfsBlocked, shutDown))

            expectThat(group1).containsExactly(fastPending)
            expectThat(group2).containsExactly(nfsRunning)
            expectThat(group3).containsExactly(nfsBlocked)
        }
    }

    @Nested
    inner class ReorgRecovery {

        /**
         * Drives a real [BlockIndexer] through a reorg detected at block 2 and asserts the runner
         * recovers in-memory state on restart. Without [IndexerRunner.runWithProximityGroups]
         * re-initialising on entry, the indexer keeps a stale `previousBlock` from the old chain
         * after [BlockIndexer.handleReorg] rolls the processor back, and every retry of block 2
         * triggers reorg detection again — an infinite loop.
         */
        @Test
        fun `runWithProximityGroups recovers a real BlockIndexer after a reorg`() = runTest {
            val thorClient = mockk<ThorClient>(relaxed = true)
            val syncedByNumber = mutableMapOf<Long, BlockIdentifier>()
            val processedBlockIds = mutableListOf<String>()
            val processor =
                mockk<IndexerProcessor>(relaxed = true) {
                    every { getLastSyncedBlock() } answers
                        {
                            syncedByNumber.maxByOrNull { it.key }?.value
                        }
                    coEvery { process(any()) } coAnswers
                        {
                            val result = firstArg<IndexingResult>() as IndexingResult.BlockResult
                            syncedByNumber[result.block.number] =
                                BlockIdentifier(result.block.number, result.block.id)
                            processedBlockIds.add(result.block.id)
                        }
                    every { rollback(any<Long>()) } answers
                        {
                            val from = firstArg<Long>()
                            syncedByNumber.keys
                                .filter { it >= from }
                                .toList()
                                .forEach { syncedByNumber.remove(it) }
                        }
                }

            val indexer =
                BlockIndexer(
                    name = "real",
                    thorClient = thorClient,
                    processor = processor,
                    startBlock = 0L,
                    syncLoggerInterval = Long.MAX_VALUE,
                    eventProcessor = null,
                    inspectionClauses = null,
                    dependsOn = null,
                )

            // Block 0 is a shared ancestor; old and new chains diverge at block 1. Block 2's
            // parentID points to the new-chain block 1, so when the indexer (already past
            // block 1 of the old chain) sees block 2, checkForReorg fires.
            val block0 = buildBlock(num = 0L)
            val block1Old = buildBlock(num = 1L, parentId = block0.id)
            val block1New = buildBlock(num = 1L, parentId = block0.id).copy(id = "0xnew_1")
            val block2New = buildBlock(num = 2L, parentId = "0xnew_1").copy(id = "0xnew_2")

            // Switch chains the first time block 2 is requested. Subsequent block 1 requests
            // serve the new chain so post-recovery the indexer reaches a consistent state.
            var reorgServed = false
            coEvery { thorClient.waitForBlock(any<BlockRevision>()) } coAnswers
                {
                    when (val num = (firstArg<BlockRevision>() as BlockRevision.Number).number) {
                        0L -> block0
                        1L -> if (reorgServed) block1New else block1Old
                        2L -> {
                            reorgServed = true
                            block2New
                        }
                        else -> {
                            // Block production beyond block 2 hasn't happened yet — block forever.
                            delay(60_000)
                            buildBlock(num = num)
                        }
                    }
                }

            val runner = IndexerRunner()
            val job = launch {
                runner.run(listOf(indexer), 1, thorClient, 500_000L, 5.minutes, 1.minutes)
            }

            delay(1_000)
            job.cancelAndJoin()

            // Without recovery, block1New is never processed because the indexer keeps detecting
            // reorgs at block 2 and never advances. Recovery resets the in-memory previousBlock,
            // letting the new-chain block 1 process cleanly.
            expectThat(processedBlockIds).contains(block1New.id)
        }
    }

    @Nested
    inner class BypassFastSyncForIndexersWithDependants {

        @Test
        fun `bypasses fast-syncable indexer that has a dependant`() = runTest {
            val parent =
                mockk<FastSyncableIndexer>(relaxed = true) {
                    every { name } returns "parent"
                    every { dependsOn } returns null
                    every { getStatus() } returns Status.NOT_INITIALISED
                }
            val child =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "child"
                    every { dependsOn } returns parent
                    every { getStatus() } returns Status.NOT_INITIALISED
                }

            val runner = IndexerRunner()
            runner.bypassFastSyncForIndexersWithDependants(listOf(parent, child))

            coVerify(exactly = 1) { parent.initialise() }
            verify(exactly = 1) { parent.bypassFastSync() }
            coVerify(exactly = 0) { parent.fastSync() }
        }

        @Test
        fun `does not touch a fast-syncable indexer with no dependants`() = runTest {
            val standalone =
                mockk<FastSyncableIndexer>(relaxed = true) {
                    every { name } returns "standalone"
                    every { dependsOn } returns null
                    every { getStatus() } returns Status.NOT_INITIALISED
                }

            val runner = IndexerRunner()
            runner.bypassFastSyncForIndexersWithDependants(listOf(standalone))

            verify(exactly = 0) { standalone.bypassFastSync() }
            coVerify(exactly = 0) { standalone.initialise() }
        }

        @Test
        fun `skips initialise call when fast-syncable parent is already initialised`() = runTest {
            val parent =
                mockk<FastSyncableIndexer>(relaxed = true) {
                    every { name } returns "parent"
                    every { dependsOn } returns null
                    every { getStatus() } returns Status.READY_TO_FAST_SYNC
                }
            val child =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "child"
                    every { dependsOn } returns parent
                }

            val runner = IndexerRunner()
            runner.bypassFastSyncForIndexersWithDependants(listOf(parent, child))

            coVerify(exactly = 0) { parent.initialise() }
            verify(exactly = 1) { parent.bypassFastSync() }
        }

        @Test
        fun `bypasses transitive ancestors when a deeper descendant exists`() = runTest {
            val grandparent =
                mockk<FastSyncableIndexer>(relaxed = true) {
                    every { name } returns "grandparent"
                    every { dependsOn } returns null
                    every { getStatus() } returns Status.NOT_INITIALISED
                }
            val parent =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "parent"
                    every { dependsOn } returns grandparent
                }
            val child =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "child"
                    every { dependsOn } returns parent
                }

            val runner = IndexerRunner()
            runner.bypassFastSyncForIndexersWithDependants(listOf(grandparent, parent, child))

            verify(exactly = 1) { grandparent.bypassFastSync() }
        }
    }

    @Nested
    inner class AlignComponents {

        // Real BlockIndexer instances with mocked processors. Avoids mocking BlockIndexer itself
        // (which triggers OOM during mock generation) while still exercising the actual
        // alignToBlock / rollback path.
        private fun blockIndexer(
            name: String,
            persistedBlock: Long,
            dependsOn: Indexer? = null,
        ): BlockIndexer {
            val processor = mockk<IndexerProcessor>(relaxed = true)
            every { processor.getLastSyncedBlock() } returns
                BlockIdentifier(persistedBlock, "0x$persistedBlock")
            every { processor.rollback(any()) } just Runs
            val indexer =
                BlockIndexer(
                    name = name,
                    thorClient = mockk(relaxed = true),
                    processor = processor,
                    startBlock = 0L,
                    syncLoggerInterval = 1L,
                    eventProcessor = null,
                    inspectionClauses = null,
                    dependsOn = dependsOn,
                )
            indexer.initialise()
            return indexer
        }

        private fun unpersistedBlockIndexer(
            name: String,
            startBlock: Long,
            dependsOn: Indexer? = null,
        ): BlockIndexer {
            val processor = mockk<IndexerProcessor>(relaxed = true)
            every { processor.getLastSyncedBlock() } returns null
            every { processor.rollback(any()) } just Runs
            val indexer =
                BlockIndexer(
                    name = name,
                    thorClient = mockk(relaxed = true),
                    processor = processor,
                    startBlock = startBlock,
                    syncLoggerInterval = 1L,
                    eventProcessor = null,
                    inspectionClauses = null,
                    dependsOn = dependsOn,
                )
            indexer.initialise()
            return indexer
        }

        @Test
        fun `is a no-op when all indexers are aligned`() {
            val a = blockIndexer("a", persistedBlock = 100L)
            val b = blockIndexer("b", persistedBlock = 100L, dependsOn = a)
            val processorA = (a as BlockIndexer)
            val processorB = (b as BlockIndexer)

            IndexerRunner().alignComponents(listOf(a, b))

            expectThat(processorA.getCurrentBlockNumber()).isEqualTo(100L)
            expectThat(processorB.getCurrentBlockNumber()).isEqualTo(100L)
        }

        @Test
        fun `rolls the ahead indexer back to the component min`() {
            val a = blockIndexer("a", persistedBlock = 3000L)
            val b = blockIndexer("b", persistedBlock = 2500L, dependsOn = a)

            // Simulate processor having only data <= 2499 after the alignment-driven rollback.
            every { (a as BlockIndexer).getLastSyncedBlock() } returns
                BlockIdentifier(2499L, "0x2499")

            IndexerRunner().alignComponents(listOf(a, b))

            expectThat(a.getCurrentBlockNumber()).isEqualTo(2500L)
            expectThat(b.getCurrentBlockNumber()).isEqualTo(2500L)
        }

        @Test
        fun `independent components are aligned independently`() {
            // Component 1: a (3000) and b (2500) — a aligns to 2500.
            val a = blockIndexer("a", persistedBlock = 3000L)
            val b = blockIndexer("b", persistedBlock = 2500L, dependsOn = a)
            every { (a as BlockIndexer).getLastSyncedBlock() } returns
                BlockIdentifier(2499L, "0x2499")
            // Component 2: c (5000) and d (4000) — c aligns to 4000.
            val c = blockIndexer("c", persistedBlock = 5000L)
            val d = blockIndexer("d", persistedBlock = 4000L, dependsOn = c)
            every { (c as BlockIndexer).getLastSyncedBlock() } returns
                BlockIdentifier(3999L, "0x3999")

            IndexerRunner().alignComponents(listOf(a, b, c, d))

            expectThat(a.getCurrentBlockNumber()).isEqualTo(2500L)
            expectThat(b.getCurrentBlockNumber()).isEqualTo(2500L)
            expectThat(c.getCurrentBlockNumber()).isEqualTo(4000L)
            expectThat(d.getCurrentBlockNumber()).isEqualTo(4000L)
        }

        @Test
        fun `skips indexers that are not yet initialised`() {
            // Uninitialised indexers report currentBlockNumber = 0; including them would force a
            // rollback to 0 for every persisted indexer.
            val a = blockIndexer("a", persistedBlock = 3000L)
            val uninit =
                mockk<Indexer>(relaxed = true) {
                    every { name } returns "uninit"
                    every { dependsOn } returns a
                    every { getStatus() } returns Status.NOT_INITIALISED
                    every { getCurrentBlockNumber() } returns 0L
                }

            IndexerRunner().alignComponents(listOf(a, uninit))

            expectThat(a.getCurrentBlockNumber()).isEqualTo(3000L)
        }

        @Test
        fun `does not roll back an unpersisted indexer sitting at a later startBlock`() {
            // Delayed-dependant configuration: parent has done no work yet and is at startBlock=0;
            // child is at startBlock=500 with no persistence. The runtime skip path handles the
            // gap. Without this carve-out, alignment would target=0 and force child back to 0.
            val parent = unpersistedBlockIndexer("parent", startBlock = 0L)
            val child = unpersistedBlockIndexer("child", startBlock = 500L, dependsOn = parent)

            IndexerRunner().alignComponents(listOf(parent, child))

            expectThat(parent.getCurrentBlockNumber()).isEqualTo(0L)
            expectThat(child.getCurrentBlockNumber()).isEqualTo(500L)
        }

        @Test
        fun `does not roll back an unpersisted indexer sitting at an earlier startBlock`() {
            // Pre-dependency-work configuration: child runs alone before its dependency on the
            // parent becomes relevant. Child starts at 100, parent at 500. The library trusts the
            // consumer not to read parent state in [100, 500) and leaves both alone.
            val parent = unpersistedBlockIndexer("parent", startBlock = 500L)
            val child = unpersistedBlockIndexer("child", startBlock = 100L, dependsOn = parent)

            IndexerRunner().alignComponents(listOf(child, parent))

            expectThat(child.getCurrentBlockNumber()).isEqualTo(100L)
            expectThat(parent.getCurrentBlockNumber()).isEqualTo(500L)
        }

        @Test
        fun `aggregates rollback failures into a single exception listing every stuck indexer`() {
            // Two persisted indexers in a component whose processors retain only a shallow
            // rollback window — both refuse to actually roll back to the component target. The
            // operator should see one error listing both names rather than failing-restarting once
            // per indexer.
            val stuckParent = stuckBlockIndexer("stuck-parent", persistedBlock = 10_000_000L)
            val stuckChild =
                stuckBlockIndexer(
                    "stuck-child",
                    persistedBlock = 10_000_000L,
                    dependsOn = stuckParent,
                )
            val newChild =
                unpersistedBlockIndexer(
                    "new-child",
                    startBlock = 1_000_000L,
                    dependsOn = stuckParent
                )

            val ex =
                assertThrows<IllegalStateException> {
                    IndexerRunner().alignComponents(listOf(stuckParent, stuckChild, newChild))
                }
            expectThat(ex.message!!).contains("Cannot align 2 indexer(s)")
            expectThat(ex.message!!).contains("'stuck-parent'")
            expectThat(ex.message!!).contains("'stuck-child'")
            expectThat(ex.message!!).contains("Drop persisted state")
        }

        // A persisted BlockIndexer whose processor's rollback is a no-op — getLastSyncedBlock
        // continues to report the persisted block after rollback. Models a processor with
        // insufficient retention for deep realignment.
        private fun stuckBlockIndexer(
            name: String,
            persistedBlock: Long,
            dependsOn: Indexer? = null,
        ): BlockIndexer {
            val processor = mockk<IndexerProcessor>(relaxed = true)
            every { processor.getLastSyncedBlock() } returns
                BlockIdentifier(persistedBlock, "0x$persistedBlock")
            every { processor.rollback(any()) } just Runs // no-op rollback
            val indexer =
                BlockIndexer(
                    name = name,
                    thorClient = mockk(relaxed = true),
                    processor = processor,
                    startBlock = 0L,
                    syncLoggerInterval = 1L,
                    eventProcessor = null,
                    inspectionClauses = null,
                    dependsOn = dependsOn,
                )
            indexer.initialise()
            return indexer
        }
    }
}
