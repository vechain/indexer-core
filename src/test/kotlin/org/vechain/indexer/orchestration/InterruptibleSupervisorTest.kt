package org.vechain.indexer.orchestration

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.vechain.indexer.Indexer
import org.vechain.indexer.initialiseAndSyncPhase

class InterruptibleSupervisorTest {
    @Test
    fun `runPhases restarts phase on error and completes on success`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val attempts = AtomicInteger(0)
        val supervisor = InterruptibleSupervisor(this, controller)
        val phases =
            listOf<suspend () -> Unit>({
                if (attempts.incrementAndGet() == 1) throw RuntimeException("fail once")
            })
        supervisor.runPhases(phases)
        assertEquals(2, attempts.get())
        assertEquals(listOf(InterruptReason.Error), interrupts)
    }

    @Test
    fun `runPhases stops on shutdown`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val supervisor = InterruptibleSupervisor(this, controller)
        val phases =
            listOf<suspend () -> Unit>(
                { controller.request(InterruptReason.Shutdown) },
                { throw IllegalStateException("Should not reach second phase") }
            )
        supervisor.runPhases(phases)
        assertEquals(listOf(InterruptReason.Shutdown), interrupts)
    }

    @Test
    fun `interruptible supervisor restarts phase after fast sync failure`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val attempts = AtomicInteger(0)
        val indexer = mockIndexer()
        coEvery { indexer.fastSync() } answers
            {
                if (attempts.incrementAndGet() == 1) {
                    throw RuntimeException("boom")
                }
            }

        val supervisor = InterruptibleSupervisor(scope = this, interruptController = controller)

        val thrown =
            runCatching {
                    supervisor.runPhases(
                        listOf {
                            initialiseAndSyncPhase(
                                scope = this,
                                indexers = listOf(indexer),
                                interruptController = controller,
                            )
                        }
                    )
                }
                .exceptionOrNull()

        Assertions.assertNull(thrown)
        Assertions.assertEquals(2, attempts.get())
        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertFalse(controller.isRequested())
    }

    private fun mockIndexer(): Indexer =
        mockk(relaxed = true) { every { name } returns "test-indexer" }
}
