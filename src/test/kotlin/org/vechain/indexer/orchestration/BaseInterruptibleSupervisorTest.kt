package org.vechain.indexer.orchestration

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class BaseInterruptibleSupervisorTest {
    @Test
    fun `supervise restarts on error and completes on success`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val attempts = AtomicInteger(0)
        val supervisor =
            object : BaseInterruptibleSupervisor(this, controller) {
                suspend fun <T> testSupervise(
                    work: suspend () -> T,
                    onInterrupt: suspend (InterruptReason) -> SupervisorAction
                ) = supervise(work, onInterrupt)
            }

        val result =
            supervisor.testSupervise(
                work = {
                    if (attempts.incrementAndGet() == 1) throw RuntimeException("fail once")
                    "done"
                },
                onInterrupt = { reason ->
                    when (reason) {
                        InterruptReason.Error ->
                            BaseInterruptibleSupervisor.SupervisorAction.Restart
                        InterruptReason.Shutdown ->
                            BaseInterruptibleSupervisor.SupervisorAction.Stop
                    }
                }
            )
        assertEquals("done", result)
        assertEquals(2, attempts.get())
        assertEquals(listOf(InterruptReason.Error), interrupts)
    }

    @Test
    fun `supervise stops on shutdown`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val supervisor =
            object : BaseInterruptibleSupervisor(this, controller) {
                suspend fun <T> testSupervise(
                    work: suspend () -> T,
                    onInterrupt: suspend (InterruptReason) -> SupervisorAction
                ) = supervise(work, onInterrupt)
            }
        // Simulate shutdown interrupt
        launch { controller.request(InterruptReason.Shutdown) }
        val result =
            supervisor.testSupervise(
                work = { "should not run" },
                onInterrupt = { reason ->
                    when (reason) {
                        InterruptReason.Error ->
                            BaseInterruptibleSupervisor.SupervisorAction.Restart
                        InterruptReason.Shutdown ->
                            BaseInterruptibleSupervisor.SupervisorAction.Stop
                    }
                }
            )
        assertNull(result)
        assertEquals(listOf(InterruptReason.Shutdown), interrupts)
    }
}
