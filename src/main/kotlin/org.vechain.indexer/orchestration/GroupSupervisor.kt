package org.vechain.indexer.orchestration

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import org.vechain.indexer.Indexer
import org.vechain.indexer.orchestration.OrchestrationUtils.runWithInterruptHandling
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.model.Block

class GroupSupervisor(
    scope: CoroutineScope,
    private val stream: BlockStream,
    private val executionGroups: List<List<Indexer>>,
    interruptController: InterruptController,
) : BaseInterruptibleSupervisor(scope, interruptController) {
    suspend fun run() {
        supervise(
            work = {
                var activeGroups = launchGroups()
                var shouldExit = false
                while (scope.isActive && !shouldExit) {
                    val action = waitForGroupAction(activeGroups)
                    when (action) {
                        GroupAction.Completed,
                        GroupAction.Shutdown, -> shouldExit = true
                        GroupAction.Restart -> {
                            activeGroups = restartGroups(activeGroups)
                        }
                    }
                }
                activeGroups.cancelAll()
                stream.close()
            },
            onInterrupt = { reason ->
                when (reason) {
                    InterruptReason.Error -> SupervisorAction.Restart
                    InterruptReason.Shutdown -> SupervisorAction.Stop
                }
            },
        )
    }

    private suspend fun waitForGroupAction(current: GroupSet): GroupAction = select {
        current.completion.onAwait { GroupAction.Completed }
        interruptController.registerListener().onReceive { reason ->
            when (reason) {
                InterruptReason.Error -> GroupAction.Restart
                InterruptReason.Shutdown -> GroupAction.Shutdown
            }
        }
    }

    private suspend fun restartGroups(current: GroupSet): GroupSet {
        current.cancelAll()
        interruptController.clear(InterruptReason.Error)
        stream.reset()
        return launchGroups()
    }

    private enum class GroupAction {
        Completed,
        Restart,
        Shutdown,
    }

    private suspend fun launchGroups(): GroupSet {
        val jobs =
            executionGroups.map { group ->
                val subscription = stream.subscribe()
                scope.launch {
                    processGroup(
                        group = group,
                        subscription = subscription,
                        interruptController = interruptController,
                    )
                }
            }
        val completion = scope.async { jobs.joinAll() }
        return GroupSet(jobs, completion)
    }

    private suspend fun GroupSet.cancelAll() {
        completion.cancel()
        jobs.forEach { it.cancel() }
        jobs.joinAll()
    }

    private data class GroupSet(
        val jobs: List<Job>,
        val completion: Deferred<Unit>,
    )
}

internal suspend fun CoroutineScope.processGroup(
    group: List<Indexer>,
    subscription: BlockStreamSubscription,
    interruptController: InterruptController,
) {
    try {
        while (isActive && !interruptController.isRequested()) {
            val block =
                try {
                    subscription.next()
                } catch (e: CancellationException) {
                    throw e
                } catch (_: Exception) {
                    interruptController.request(InterruptReason.Error)
                    break
                }
            try {
                runGroupForBlock(group, block, interruptController)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                // Request reset and suppress exception, matching runGroupForBlock behavior
                interruptController.request(InterruptReason.Error)
                // suppress
            }
        }
    } finally {
        subscription.close()
    }
}

internal suspend fun runGroupForBlock(
    group: List<Indexer>,
    block: Block,
    interruptController: InterruptController,
) {
    for (indexer in group) {
        if (interruptController.isRequested()) {
            return
        }

        val currentBlock = indexer.getCurrentBlockNumber()
        when {
            currentBlock == block.number ->
                runWithInterruptHandling(interruptController, suppressException = true) {
                    indexer.processBlock(block)
                }
            currentBlock > block.number -> Unit
            else -> {
                interruptController.request(InterruptReason.Error)
                throw IllegalStateException(
                    "Indexer ${indexer.name} is behind: current block $currentBlock, processing ${block.number}",
                )
            }
        }
    }
}
