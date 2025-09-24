package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.RestartIndexerException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block

open class ChannelIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    private val syncLoggerInterval: Long,
    override val eventProcessor: CombinedEventProcessor?,
    pruner: Pruner?,
    prunerInterval: Long,
    private val batchSize: Int,
    dependsOn: Set<Indexer>
) :
    PreSyncIndexer(
        name,
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        eventProcessor,
        pruner,
        prunerInterval,
        dependsOn
    ) {

    override suspend fun sync(toBlock: Long) {
        coroutineScope {
            // Create a channel to load blocks up to the target block
            val blockReceiver = loadBlocks(toBlock)

            // Process blocks
            for (block in blockReceiver) {
                waitForDependenciesIfRequired()
                try {

                    if (
                        logger.isTraceEnabled ||
                            status != Status.SYNCING ||
                            currentBlockNumber % syncLoggerInterval == 0L
                    ) {
                        logger.info("($status) Processing Block  ${block.number}")
                    }

                    val events = eventProcessor?.processEvents(block) ?: emptyList()
                    process(events, block)

                    currentBlockNumber = block.number + 1
                    timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
                } catch (e: Exception) {
                    logger.error(
                        "Restarting sync due to error syncing at block $currentBlockNumber: ${e.message}"
                    )
                    throw RestartIndexerException()
                }
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    protected fun CoroutineScope.loadBlocks(toBlock: Long) = produce {
        var current = currentBlockNumber
        while (current <= toBlock) {
            val upper = minOf(current + batchSize - 1, toBlock)

            // Parallel fetch the batch
            val blocks =
                (current..upper)
                    .map { i -> async { i to getBlock(i) } }
                    .awaitAll()
                    .sortedBy { it.first } // Ensure order just in case
                    .map { it.second }

            for (block in blocks) {
                send(block)
            }

            current = upper + 1
        }
    }

    // Fetch a block with retry logic and logging
    private suspend fun getBlock(i: Long): Block {
        var currentDelay = 1000L
        var attempt = 0
        while (true) {
            try {
                return thorClient.getBlock(i)
            } catch (e: Exception) {
                attempt++
                logger.warn("Failed to fetch block $i (attempt $attempt): ${e.message}")
                kotlinx.coroutines.delay(currentDelay)
                currentDelay = (currentDelay * 2).coerceAtMost(30000L)
            }
        }
    }
}
