package org.vechain.indexer

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.PrefetchingBlockStream
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

open class IndexerCoordinator(
    private val thorClient: ThorClient,
    private val batchSize: Int = 1,
) {
    companion object {
        @Suppress("unused")
        fun launch(
            scope: CoroutineScope,
            thorClient: ThorClient,
            indexers: List<Indexer>,
            blockBatchSize: Int = 1,
        ): Job {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val indexerCoordinator =
                IndexerCoordinator(thorClient = thorClient, batchSize = blockBatchSize)

            return scope.launch {
                indexerCoordinator.run(
                    indexers = indexers,
                    batchSize = blockBatchSize,
                    thorClient = thorClient,
                )
            }
        }
    }

    suspend fun run(indexers: List<Indexer>, batchSize: Int, thorClient: ThorClient) =
        coroutineScope {
            require(indexers.isNotEmpty()) { "At least one indexer is required" }

            val interruptController = InterruptController()
            val executionGroups = CoordinatorSupport.topologicalOrder(indexers)

            val supervisor =
                InterruptibleSupervisor(scope = this, interruptController = interruptController)

            supervisor.runPhases(
                listOf(
                    {
                        initialiseAndSyncPhase(
                            scope = this,
                            indexers = indexers,
                            interruptController = interruptController
                        )
                    },
                    {
                        processBlocksPhase(
                            scope = this,
                            batchSize = batchSize,
                            thorClient = thorClient,
                            executionGroups = executionGroups,
                            interruptController = interruptController
                        )
                    }
                )
            )
        }
}

suspend fun initialiseAndSyncPhase(
    scope: CoroutineScope,
    indexers: List<Indexer>,
    interruptController: InterruptController,
) {
    val jobs =
        indexers.map { indexer ->
            scope.launch { runIndexerInitialisation(indexer, interruptController) }
        }
    jobs.joinAll()
}

private suspend fun runIndexerInitialisation(
    indexer: Indexer,
    interruptController: InterruptController,
) {
    try {
        indexer.initialise()
        indexer.fastSync()
    } catch (e: CancellationException) {
        interruptController.request(InterruptReason.Shutdown)
        throw e
    } catch (e: Throwable) {
        interruptController.request(InterruptReason.Error)
        throw e
    }
}

suspend fun processBlocksPhase(
    scope: CoroutineScope,
    batchSize: Int,
    thorClient: ThorClient,
    executionGroups: List<List<Indexer>>,
    interruptController: InterruptController,
) {
    val stream =
        PrefetchingBlockStream(
            scope = scope,
            batchSize = batchSize,
            currentBlockProvider = {
                executionGroups.flatten().minOf { it.getCurrentBlockNumber() }
            },
            thorClient = thorClient,
        )
    GroupSupervisor(scope, stream, executionGroups, interruptController).run()
}

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
                val phaseJob = scope.async { phases[currentPhase]() }
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

internal class GroupSupervisor(
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
                try {
                    indexer.processBlock(block)
                } catch (e: CancellationException) {
                    interruptController.request(InterruptReason.Shutdown)
                    throw e
                } catch (_: Exception) {
                    interruptController.request(InterruptReason.Error)
                }
            }
        }
}

enum class InterruptReason {
    Error,
    Shutdown,
}

class InterruptController(onRequest: ((InterruptReason) -> Unit)? = null) {
    private val requested = AtomicReference<InterruptReason?>(null)
    private val listeners = CopyOnWriteArrayList<Channel<InterruptReason>>()
    private val callback = onRequest

    fun registerListener(): ReceiveChannel<InterruptReason> {
        val channel = Channel<InterruptReason>(capacity = Channel.CONFLATED)
        listeners.add(channel)
        requested.get()?.let { channel.trySend(it) }
        channel.invokeOnClose { listeners.remove(channel) }
        return channel
    }

    fun request(reason: InterruptReason) {
        var reasonToNotify: InterruptReason? = null
        while (true) {
            val current = requested.get()
            if (current == InterruptReason.Shutdown) {
                return
            }
            if (current == reason) {
                return
            }

            val updated =
                when (reason) {
                    InterruptReason.Error -> current ?: InterruptReason.Error
                    InterruptReason.Shutdown -> InterruptReason.Shutdown
                }

            if (requested.compareAndSet(current, updated)) {
                if (updated != current) {
                    reasonToNotify = updated
                }
                break
            }
        }

        reasonToNotify?.let {
            callback?.invoke(it)
            notifyListeners(it)
        }
    }

    fun clear(reason: InterruptReason) {
        requested.compareAndSet(reason, null)
    }

    fun isRequested(): Boolean = requested.get() != null

    fun currentReason(): InterruptReason? = requested.get()

    private fun notifyListeners(reason: InterruptReason) {
        listeners.forEach { it.trySend(reason) }
    }
}
