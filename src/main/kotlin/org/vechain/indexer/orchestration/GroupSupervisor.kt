package org.vechain.indexer.orchestration

import kotlin.collections.forEach
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.coroutineScope
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
    private val scope: CoroutineScope,
    private val stream: BlockStream,
    private val executionGroups: List<List<Indexer>>,
    private val interruptController: InterruptController,
) {

    suspend fun run() {
        val interrupts = interruptController.registerListener()
        var activeGroups = launchGroups()
        try {
            var shouldExit = false
            while (scope.isActive && !shouldExit) {
                when (
                    select<GroupAction> {
                        activeGroups.completion.onAwait { GroupAction.Completed }
                        interrupts.onReceive { reason ->
                            when (reason) {
                                InterruptReason.Error -> GroupAction.Restart
                                InterruptReason.Shutdown -> GroupAction.Shutdown
                            }
                        }
                    }
                ) {
                    GroupAction.Completed -> shouldExit = true
                    GroupAction.Shutdown -> shouldExit = true
                    GroupAction.Restart -> {
                        activeGroups = restartGroups(activeGroups)
                    }
                }
            }
        } catch (e: ClosedReceiveChannelException) {
            // Treat listener closure as shutdown request
        } finally {
            interrupts.cancel()
            activeGroups.cancelAll()
            stream.close()
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
) = coroutineScope {
    group
        .filter { it.getCurrentBlockNumber() == block.number }
        .forEach { indexer ->
            launch {
                runWithInterruptHandling(interruptController, suppressException = true) {
                    indexer.processBlock(block)
                }
            }
        }
}
