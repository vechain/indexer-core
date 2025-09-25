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
import org.vechain.indexer.thor.model.Clause

open class ChannelIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    private val syncLoggerInterval: Long,
    eventProcessor: CombinedEventProcessor?,
    inspectionClauses: List<Clause>? = null,
    pruner: Pruner?,
    prunerInterval: Long,
    private val batchSize: Int,
) :
    PreSyncIndexer(
        name,
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        eventProcessor,
        inspectionClauses,
        pruner,
        prunerInterval,
    ) {


    override suspend fun sync(toBlock: Long) {
        coroutineScope {
            // Create a channel to load blocks up to the target block
            val blockReceiver = loadBlocks(toBlock)

            // Process blocks
            for (event in blockReceiver) {
                try {
                    process(event)
                    currentBlockNumber = when (event) {
                        is BlockEvent.EventsOnly -> throw IllegalStateException("Unexpected EventsOnly event")
                        is BlockEvent.Normal -> event.block.number
                        is BlockEvent.WithCallData -> event.block.number
                    }
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
    private suspend fun getBlock(i: Long): BlockEvent {
        var currentDelay = 1000L
        var attempt = 0
        while (true) {
            try {
                val block = thorClient.getBlock(i)

                if (
                    logger.isTraceEnabled ||
                    status != Status.SYNCING ||
                    block.number % syncLoggerInterval == 0L
                ) {
                    logger.info("($status) Processing Block  ${block.number}")
                }

                val event = if (eventProcessor != null) {
                    BlockEvent.Normal(block, eventProcessor.processEvents(block))
                } else if (inspectionClauses != null) {
                    val inspections = thorClient.inspectClauses(inspectionClauses, block.id)
                    BlockEvent.WithCallData(block, inspections)
                } else {
                    BlockEvent.Normal(block, emptyList())
                }

                return event
            } catch (e: Exception) {
                attempt++
                logger.warn("Failed to fetch block $i (attempt $attempt): ${e.message}")
                kotlinx.coroutines.delay(currentDelay)
                currentDelay = (currentDelay * 2).coerceAtMost(30000L)
            }
        }
    }
}
