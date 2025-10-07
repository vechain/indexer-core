package org.vechain.indexer.orchestration

import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class InterruptControllerTest {
    @Test
    fun `reset controller only triggers once`() {
        val invocations = AtomicInteger(0)
        val controller = InterruptController { invocations.incrementAndGet() }

        controller.request(InterruptReason.Error)
        controller.request(InterruptReason.Error)

        Assertions.assertTrue(controller.isRequested())
        Assertions.assertEquals(1, invocations.get())
    }
}
