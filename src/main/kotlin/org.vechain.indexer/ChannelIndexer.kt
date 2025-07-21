package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.produce
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.thor.client.ThorClient

open class ChannelIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    startBlock: Long,
    private val syncLoggerInterval: Long,
    override val eventProcessor: CombinedEventProcessor?,
    pruner: Pruner?,
    prunerInterval: Long,
) :
    BlockIndexer(
        name,
        thorClient,
        processor,
        startBlock,
        syncLoggerInterval,
        eventProcessor,
        pruner,
        prunerInterval,
    ) {

    override suspend fun start(iterations: Long?) {
        initialise()
        val finalizedBlock = thorClient.getFinalizedBlock().number

        remainingIterations = iterations

        if (currentBlockNumber < finalizedBlock) {
            sync(finalizedBlock)
        }

        logger.info("Fast sync complete, switching to block indexer")
        run()
    }

    private suspend fun sync(toBlock: Long) {
        // Create a channel to load blocks up to the target block
        val blockReceiver = loadBlocks(toBlock)

        // Process blocks with eventProcessor.process
        for (block in blockReceiver) {
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
                    "Error fetching logs at block $currentBlockNumber: ${e.message} \n${e.stackTraceToString()}"
                )
                handleError()
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun loadBlocks(toBlock: Long) =
        GlobalScope.produce {
            for (i in currentBlockNumber..toBlock) {
                if (hasNoRemainingIterations()) return@produce
                send(thorClient.getBlock(i))
            }
        }
}
