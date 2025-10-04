package org.vechain.indexer.orchestration

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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
}
