package org.vechain.indexer.orchestration

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select

class InterruptibleSupervisor(
    private val scope: CoroutineScope,
    private val interruptController: InterruptController,
) {
    suspend fun runPhases(phases: List<suspend () -> Unit>) {
        var currentPhase = 0
        var shouldExit = false
        val interrupts = interruptController.registerListener()
        try {
            while (scope.isActive && !shouldExit && currentPhase < phases.size) {
                val parentContext = scope.coroutineContext
                val parentJob = parentContext[Job]
                val phaseSupervisor = parentJob?.let { SupervisorJob(it) } ?: SupervisorJob()
                val phaseJob =
                    scope.async(context = parentContext + phaseSupervisor) {
                        phases[currentPhase]()
                    }
                try {
                    val action =
                        try {
                            select {
                                phaseJob.onAwait { SupervisorAction.Completed }
                                interrupts.onReceive { reason ->
                                    when (reason) {
                                        InterruptReason.Error -> SupervisorAction.RestartPhase
                                        InterruptReason.Shutdown -> SupervisorAction.Stop
                                    }
                                }
                            }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (_: ClosedReceiveChannelException) {
                            SupervisorAction.CompleteAndExit
                        } catch (e: Throwable) {
                            when (interruptController.currentReason()) {
                                InterruptReason.Error -> SupervisorAction.RestartPhase
                                InterruptReason.Shutdown -> SupervisorAction.Stop
                                null -> throw e
                            }
                        }

                    when (action) {
                        SupervisorAction.Completed -> {
                            currentPhase++
                        }
                        SupervisorAction.RestartPhase -> {
                            phaseJob.cancel()
                            runCatching { phaseJob.join() }
                            interruptController.clear(InterruptReason.Error)
                            continue
                        }
                        SupervisorAction.Stop,
                        SupervisorAction.CompleteAndExit -> {
                            phaseJob.cancel()
                            runCatching { phaseJob.join() }
                            shouldExit = true
                        }
                    }
                } finally {
                    phaseSupervisor.cancel()
                }
            }
        } finally {
            interrupts.cancel()
        }
    }

    private enum class SupervisorAction {
        Completed,
        RestartPhase,
        Stop,
        CompleteAndExit,
    }
}
