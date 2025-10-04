package org.vechain.indexer.orchestration

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.isActive

class InterruptibleSupervisor(
    scope: CoroutineScope,
    interruptController: InterruptController,
) : BaseInterruptibleSupervisor(scope, interruptController) {
    suspend fun runPhases(phases: List<suspend () -> Unit>) {
        var currentPhase = 0
        var shouldExit = false
        while (scope.isActive && !shouldExit && currentPhase < phases.size) {
            val parentContext = scope.coroutineContext
            val parentJob = parentContext[Job]
            val phaseSupervisor = parentJob?.let { SupervisorJob(it) } ?: SupervisorJob()
            val phase = phases[currentPhase]
            val result =
                supervise(
                    work = { phase() },
                    onInterrupt = { reason ->
                        when (reason) {
                            InterruptReason.Error ->
                                BaseInterruptibleSupervisor.SupervisorAction.Restart
                            InterruptReason.Shutdown ->
                                BaseInterruptibleSupervisor.SupervisorAction.Stop
                        }
                    }
                )
            when (result) {
                null -> shouldExit = true
                else -> currentPhase++
            }
            phaseSupervisor.cancel()
        }
    }
}
