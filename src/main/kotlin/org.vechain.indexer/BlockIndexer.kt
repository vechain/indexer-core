package org.vechain.indexer

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.Clause

open class BlockIndexer(
    override val name: String,
    protected open val thorClient: ThorClient,
    private val processor: IndexerProcessor,
    protected val startBlock: Long,
    private val syncLoggerInterval: Long,
    protected val eventProcessor: CombinedEventProcessor?,
    protected val inspectionClauses: List<Clause>?,
    override val pruner: Pruner?,
    private val prunerInterval: Long,
    override val dependsOn: Indexer?,
) : Indexer {
    /** The last block that was successfully synchronised */
    var previousBlock: BlockIdentifier? = null
        protected set

    // A random number between 0 and `prunerInterval`. This makes it less like that all pruners will
    // run at the same time.
    private val prunerIntervalOffset = (0 until prunerInterval).random()

    protected val logger: Logger = LoggerFactory.getLogger(name)

    override var status = Status.NOT_INITIALISED

    var currentBlockNumber: Long = 0
        protected set

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        internal set

    /** Initialises the indexer processing */
    internal fun initialise(blockNumber: Long? = null) {
        // If no block number is provided, get the last synced block. If no block is found, start
        // from the beginning.
        val lastSyncedBlockNumber = blockNumber ?: getLastSyncedBlock()?.number ?: startBlock

        // To ensure data integrity roll back changes made in the last block
        rollback(lastSyncedBlockNumber)

        // Initialise fields
        currentBlockNumber = lastSyncedBlockNumber

        // Set the previous block to the previously synced block if it exists, or null otherwise.
        val lastBlock = getLastSyncedBlock()
        previousBlock =
            if (lastBlock?.number == lastSyncedBlockNumber - 1L) {
                lastBlock
            } else {
                null
            }

        status = Status.INITIALISED
    }

    /** Restarts the processing based on the current indexer status */
    protected open fun restart() {
        logger.info("⚠️ Restarting @ Block: $currentBlockNumber with status $status")
        // Initialise the indexer
        when (status) {
            Status.ERROR -> initialise(currentBlockNumber)
            Status.REORG -> initialise(currentBlockNumber - 1)
            else -> initialise()
        }

        logger.info("✅ Successfully Restarted @ Block: $currentBlockNumber with status $status")
    }

    protected suspend fun buildIndexingResult(block: Block): IndexingResult {
        val callResults =
            inspectionClauses?.let { thorClient.inspectClauses(it, block.id) } ?: emptyList()
        val events = eventProcessor?.processEvents(block) ?: emptyList()

        return IndexingResult.Normal(block, events, callResults)
    }

    internal fun restartIfNeeded() {
        if (status == Status.ERROR || status == Status.REORG) restart()
    }

    internal fun logStartingState() {
        logger.info("Starting @ Block: $currentBlockNumber")
    }

    internal suspend fun processBlock(block: Block, onReset: () -> Unit) {
        if (status == Status.NOT_INITIALISED) {
            throw IllegalStateException("Indexer $name is not initialised")
        }
        updateSyncStatus(block)
        if (block.number != currentBlockNumber) {
            throw IllegalStateException(
                "Block number mismatch: expected $currentBlockNumber, got ${block.number}"
            )
        }
        try {
            logProcessingBlock()
            checkForReorg(block)
            process(buildIndexingResult(block))
            // Set the current block number to the next block.
            currentBlockNumber = block.number + 1
            // Set the previous block id.
            previousBlock = BlockIdentifier(number = block.number, id = block.id)
            timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
            runPruner()
        } catch (_: ReorgException) {
            handleReorg()
            onReset()
        } catch (e: Exception) {
            logger.error("Error while processing block ${block.number}", e)
            handleError()
            onReset()
        }
    }

    private fun updateSyncStatus(block: Block) {
        // if the timestamp of the block is within a minute of the current time, we are fully synced
        val blockTime = LocalDateTime.ofEpochSecond(block.timestamp, 0, ZoneOffset.UTC)
        val now = LocalDateTime.now(ZoneOffset.UTC)
        status =
            if (Duration.between(blockTime, now).toMinutes() < 1) {
                Status.SYNCING
            } else {
                Status.FULLY_SYNCED
            }
    }

    internal fun handleError() {
        status = Status.ERROR
    }

    internal fun logBlockFetchError(blockNumber: Long, throwable: Exception) {
        logger.error("Error while fetching block $blockNumber", throwable)
    }

    private fun handleReorg() {
        status = Status.REORG
    }

    /**
     * Runs the pruner service if the indexer is in a fully synced state and the current block. The
     * pruner will run every `prunerInterval` blocks, offset by a random number between 0 and
     * `prunerInterval` to ensure that not all indexers run the pruner at the same time.
     */
    protected fun runPruner() {
        val prunerInstance = pruner ?: return
        if (status != Status.FULLY_SYNCED) return
        if (currentBlockNumber % prunerInterval != prunerIntervalOffset) return

        status = Status.PRUNING
        try {
            prunerInstance.run(currentBlockNumber)
        } finally {
            status = Status.FULLY_SYNCED
        }
    }

    override fun getLastSyncedBlock(): BlockIdentifier? = processor.getLastSyncedBlock()

    override fun rollback(blockNumber: Long) = processor.rollback(blockNumber)

    override fun process(entry: IndexingResult) = processor.process(entry)

    private fun logProcessingBlock() {
        if (logger.isDebugEnabled) {
            logger.debug("($status) Processing Block  $currentBlockNumber")
        } else if (currentBlockNumber % syncLoggerInterval == 0L) {
            logger.info("($status) Processing Block  $currentBlockNumber")
        }
    }

    internal fun checkForReorg(block: Block) {
        // Check for chain re-organization.
        if (
            currentBlockNumber > startBlock &&
                previousBlock?.id?.let { it != block.parentID } == true
        ) {
            val message =
                "REORG @ Block $currentBlockNumber previousBlock=(id=${previousBlock?.id ?: "null"} number=${previousBlock?.number ?: "null"})  parentID=${block.parentID}"
            logger.error(message)
            throw ReorgException(message = message)
        }
    }
}
