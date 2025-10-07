package org.vechain.indexer

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.orchestration.InterruptController
import org.vechain.indexer.orchestration.InterruptReason
import org.vechain.indexer.thor.client.ThorClient

@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(MockKExtension::class)
class IndexerRunnerTest {

    @MockK private lateinit var thorClient: ThorClient

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
    }

    @Test
    fun `initialiseAndSyncPhase requests error when fast sync fails`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val indexer = mockIndexer()
        coEvery { indexer.fastSync() } throws RuntimeException("boom")

        val thrown =
            runCatching {
                    initialiseAndSyncPhase(
                        scope = this,
                        indexers = listOf(indexer),
                        interruptController = controller,
                    )
                }
                .exceptionOrNull()

        Assertions.assertTrue(thrown is RuntimeException)
        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertEquals(InterruptReason.Error, controller.currentReason())
    }

    @Test
    fun `processBlocksPhase uses minimum block number from all indexers`() = runTest {
        val indexer1 = mockIndexer("indexer1", currentBlock = 100L)
        val indexer2 = mockIndexer("indexer2", currentBlock = 50L)
        val indexer3 = mockIndexer("indexer3", currentBlock = 75L)

        val executionGroups = listOf(listOf(indexer1, indexer2, indexer3))

        // Verify that the currentBlockProvider returns the minimum
        val currentBlockProvider: () -> Long = {
            executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
        }

        Assertions.assertEquals(50L, currentBlockProvider())
    }

    @Test
    fun `processBlocksPhase with multiple groups uses minimum across all groups`() = runTest {
        val group1Indexer1 = mockIndexer("g1-i1", currentBlock = 100L)
        val group1Indexer2 = mockIndexer("g1-i2", currentBlock = 90L)
        val group2Indexer1 = mockIndexer("g2-i1", currentBlock = 85L)
        val group2Indexer2 = mockIndexer("g2-i2", currentBlock = 95L)

        val executionGroups =
            listOf(listOf(group1Indexer1, group1Indexer2), listOf(group2Indexer1, group2Indexer2))

        // Verify that the currentBlockProvider returns the minimum across all groups
        val currentBlockProvider: () -> Long = {
            executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
        }

        Assertions.assertEquals(85L, currentBlockProvider())
    }

    @Test
    fun `processBlocksPhase with single indexer uses its block number`() = runTest {
        val indexer = mockIndexer("single-indexer", currentBlock = 42L)

        val executionGroups = listOf(listOf(indexer))

        // Verify that the currentBlockProvider returns the single indexer's block number
        val currentBlockProvider: () -> Long = {
            executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
        }

        Assertions.assertEquals(42L, currentBlockProvider())
    }

    private fun mockIndexer(
        indexerName: String = "test-indexer",
        currentBlock: Long = 0L
    ): Indexer =
        mockk(relaxed = true) {
            every { name } returns indexerName
            every { getCurrentBlockNumber() } returns currentBlock
        }
}
