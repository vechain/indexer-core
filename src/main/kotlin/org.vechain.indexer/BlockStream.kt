package org.vechain.indexer

import java.util.ArrayDeque
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

// VeChain block time averages ~10 seconds; start with a short poll and cap near the expected block
// interval to avoid hammering the node while we wait for the next block to land.
private const val TIP_POLL_BASE_DELAY_MS = 1_000L
private const val TIP_POLL_MAX_DELAY_MS = 10_000L

interface BlockStream {
    suspend fun next(): Block

    fun reset()

    fun close()

    val isCaughtUp: Boolean
}

class PrefetchingBlockStream(
    private val scope: CoroutineScope,
    private val batchSize: Int,
    private val currentBlockProvider: () -> Long,
    private val thorClient: ThorClient,
) : BlockStream {
    private val buffer = ArrayDeque<Deferred<Pair<Long, Block>>>()
    private var nextToSchedule = currentBlockProvider()
    private var supervisor: CompletableJob = newSupervisor()
    private var prefetchScope = CoroutineScope(scope.coroutineContext + supervisor)
    private var latestBestBlock = Long.MIN_VALUE
    private var caughtUp = false

    override val isCaughtUp: Boolean
        get() = caughtUp

    override suspend fun next(): Block {
        ensurePrefetched()
        val deferred = buffer.removeFirst()
        val (_, result) = deferred.await()
        return result
    }

    override fun reset() {
        cancelPrefetchScope()
        createPrefetchScope()
        nextToSchedule = currentBlockProvider()
        caughtUp = false
    }

    override fun close() {
        cancelPrefetchScope()
    }

    private fun ensurePrefetched() {
        while (buffer.size < batchSize) {
            val blockNumber = nextToSchedule
            val deferred = prefetchScope.async { blockNumber to fetchBlock(blockNumber) }
            buffer.add(deferred)
            nextToSchedule++
        }
    }

    private fun cancelPrefetchScope() {
        buffer.forEach { it.cancel() }
        buffer.clear()
        supervisor.cancel()
    }

    private fun createPrefetchScope() {
        supervisor = newSupervisor()
        prefetchScope = CoroutineScope(scope.coroutineContext + supervisor)
    }

    private fun newSupervisor(): CompletableJob {
        val parentJob = scope.coroutineContext[Job]
        return SupervisorJob(parentJob)
    }

    private suspend fun fetchBlock(blockNumber: Long): Block {
        var delayMs = TIP_POLL_BASE_DELAY_MS
        while (true) {
            try {
                val block = thorClient.getBlock(blockNumber)
                caughtUp = block.number >= latestBestBlock
                return block
            } catch (e: BlockNotFoundException) {
                val bestBlock = thorClient.getBestBlock()
                latestBestBlock = maxOf(latestBestBlock, bestBlock.number)
                if (bestBlock.number <= blockNumber) {
                    caughtUp = true
                    delay(delayMs)
                    delayMs = (delayMs * 2).coerceAtMost(TIP_POLL_MAX_DELAY_MS)
                    continue
                }
                caughtUp = false
                throw e
            }
        }
    }
}
