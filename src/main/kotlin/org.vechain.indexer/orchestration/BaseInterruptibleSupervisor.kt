package org.vechain.indexer.orchestration

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select

abstract class BaseInterruptibleSupervisor(
    protected val scope: CoroutineScope,
    protected val interruptController: InterruptController
) {
    protected suspend fun <T> supervise(
        work: suspend () -> T,
        onInterrupt: suspend (InterruptReason) -> SupervisorAction
    ): T? {
        val interrupts = interruptController.registerListener()
        try {
            while (scope.isActive) {
                var exceptionThrown = false
                val job =
                    scope.async {
                        try {
                            work()
                        } catch (_: Throwable) {
                            exceptionThrown = true
                            interruptController.request(InterruptReason.Error)
                            null // Return null to indicate restart
                        }
                    }
                val action = select {
                    job.onAwait {
                        if (exceptionThrown) SupervisorAction.Restart
                        else SupervisorAction.Completed
                    }
                    interrupts.onReceive { reason -> onInterrupt(reason) }
                }
                when (action) {
                    SupervisorAction.Completed -> return job.await()
                    SupervisorAction.Restart -> {
                        job.cancel()
                        runCatching { job.join() }
                        interruptController.clear(InterruptReason.Error)
                        continue
                    }
                    SupervisorAction.Stop -> {
                        job.cancel()
                        runCatching { job.join() }
                        return null
                    }
                }
            }
        } finally {
            interrupts.cancel()
        }
        return null
    }

    enum class SupervisorAction {
        Completed,
        Restart,
        Stop
    }
}
