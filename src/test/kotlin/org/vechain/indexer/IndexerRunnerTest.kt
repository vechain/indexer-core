package org.vechain.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vechain.indexer.orchestration.InterruptController
import org.vechain.indexer.orchestration.InterruptReason

@OptIn(ExperimentalCoroutinesApi::class)
class IndexerRunnerTest {

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

    private fun mockIndexer(): Indexer =
        mockk(relaxed = true) { every { name } returns "test-indexer" }
}
