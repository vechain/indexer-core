package org.vechain.indexer

import kotlin.time.TimeMark
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.InspectionResult
import org.vechain.indexer.utils.retryOnFailure

/**
 * Data class holding a block with its pre-fetched inspection results. Used to pipeline block
 * fetching and contract calls ahead of processing.
 */
data class PreparedBlock(
    val block: org.vechain.indexer.thor.model.Block,
    val inspectionResults: List<InspectionResult>,
)

class BlockFetcher(
    private val thorClient: ThorClient,
    private val allClauses: List<Clause>,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        internal const val BLOCK_INTERVAL_SECONDS = 10L
    }

    /**
     * Prefetches blocks and their inspection results in parallel while maintaining order. Uses a
     * sliding window of concurrent fetches to hide network latency.
     */
    suspend fun prefetchBlocksInOrder(
        startBlock: Long,
        maxBatchSize: Int,
        groupChannels: List<Channel<PreparedBlock>>,
        deadlineMark: TimeMark? = null,
    ) = coroutineScope {
        require(startBlock >= 0) { "startBlock must be >= 0" }
        require(maxBatchSize >= 1) { "maxBatchSize must be >= 1" }

        var nextBlockNumber = startBlock
        var lastBlockTimestamp: Long? = null

        while (isActive && (deadlineMark == null || deadlineMark.hasNotPassedNow())) {
            val currentBlock = nextBlockNumber
            val windowSize = calculateWindowSize(lastBlockTimestamp, maxBatchSize)
            logger.debug("Block fetch window size: $windowSize")
            // Launch prefetch for next batch of blocks in parallel
            val deferredBlocks =
                (0 ..< windowSize).map { offset ->
                    val blockNum = currentBlock + offset
                    async { fetchAndPrepareBlock(blockNum) }
                }

            // Await and send in order
            deferredBlocks.forEach { deferred ->
                val preparedBlock = deferred.await()
                groupChannels.forEach { channel -> channel.send(preparedBlock) }
                nextBlockNumber++
                lastBlockTimestamp = preparedBlock.block.timestamp
            }
        }
    }

    /**
     * Fetches a block and performs inspection calls, returning a PreparedBlock. This combines the
     * network calls so they can be pipelined.
     */
    internal suspend fun fetchAndPrepareBlock(blockNumber: Long): PreparedBlock {
        return retryOnFailure {
            val block = thorClient.waitForBlock(BlockRevision.Number(blockNumber))
            val inspectionResults =
                if (allClauses.isNotEmpty()) {
                    thorClient.inspectClauses(allClauses, BlockRevision.Id(block.id))
                } else {
                    emptyList()
                }
            PreparedBlock(block, inspectionResults)
        }
    }

    internal fun calculateWindowSize(
        lastBlockTimestampSeconds: Long?,
        maxPrefetchSize: Int,
    ): Int {
        if (lastBlockTimestampSeconds == null) {
            return maxPrefetchSize
        }
        val nowSeconds = System.currentTimeMillis() / 1000
        val secondsBehind = (nowSeconds - lastBlockTimestampSeconds).coerceAtLeast(0)
        val estimatedBlocksBehind =
            (secondsBehind / BLOCK_INTERVAL_SECONDS).toInt().coerceAtLeast(0) + 1
        return minOf(maxPrefetchSize, estimatedBlocksBehind)
    }
}
