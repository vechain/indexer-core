package org.vechain.indexer

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.coroutines.cancellation.CancellationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.utils.IndexerUtils.ensureStatus

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
    private var previousBlock: BlockIdentifier? = null

    override fun getPreviousBlock(): BlockIdentifier? = previousBlock

    protected fun setPreviousBlock(value: BlockIdentifier?) {
        previousBlock = value
    }

    // A random number between 0 and `prunerInterval`. This makes it less like that all pruners will
    // run at the same time.
    private val prunerIntervalOffset = (0 until prunerInterval).random()

    protected val logger: Logger = LoggerFactory.getLogger(name)

    private var status = Status.NOT_INITIALISED

    protected fun setStatus(newStatus: Status) {
        status = newStatus
    }

    override fun getStatus(): Status = status

    private var currentBlockNumber: Long = 0

    override fun getCurrentBlockNumber(): Long = currentBlockNumber

    protected fun setCurrentBlockNumber(value: Long) {
        currentBlockNumber = value
    }

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        internal set

    /** Initialises the indexer processing */
    override fun initialise() {
        val lastSyncedBlockNumber = getLastSyncedBlock()?.number ?: startBlock

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

        logger.info("Initialised @ Block: $currentBlockNumber")
        status = Status.INITIALISED
    }

    protected suspend fun buildIndexingResult(block: Block): IndexingResult {
        val callResults =
            inspectionClauses?.let { thorClient.inspectClauses(it, block.id) } ?: emptyList()
        val events = eventProcessor?.processEvents(block) ?: emptyList()

        return IndexingResult.Normal(block, events, callResults)
    }

    protected fun checkIfShuttingDown() {
        // If shut down throw an error
        if (status == Status.SHUT_DOWN) {
            throw CancellationException("Indexer is shut down")
        }
    }

    override suspend fun processBlock(block: Block) {
        checkIfShuttingDown()
        ensureStatus(status, setOf(Status.INITIALISED, Status.SYNCING, Status.FULLY_SYNCED))
        updateSyncStatus(block)
        checkForReorg(block)
        if (block.number != currentBlockNumber) {
            throw IllegalStateException(
                "Block number mismatch: expected $currentBlockNumber, got ${block.number}"
            )
        }

        logProcessingBlock()
        process(buildIndexingResult(block))
        // Set the current block number to the next block.
        currentBlockNumber = block.number + 1
        // Set the previous block id.
        previousBlock = BlockIdentifier(number = block.number, id = block.id)
        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
        runPruner()
    }

    protected fun updateSyncStatus(block: Block) {
        // if the timestamp of the block is within 15 seconds of the current time, we are fully
        // synced
        val blockTime = LocalDateTime.ofEpochSecond(block.timestamp, 0, ZoneOffset.UTC)
        val now = LocalDateTime.now(ZoneOffset.UTC)
        status =
            if (Duration.between(blockTime, now).toSeconds() < 15) {
                Status.FULLY_SYNCED
            } else {
                Status.SYNCING
            }
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
        } else if (status == Status.FULLY_SYNCED || currentBlockNumber % syncLoggerInterval == 0L) {
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
                "REORG @ Block $currentBlockNumber previousBlock=(id=${previousBlock?.id ?: "null"} number=${previousBlock?.number ?: "null"})  block=(parentID=${block.parentID} blockNumber=${block.number} id=${block.id})"
            logger.error(message)
            // Rollback and set the status to error
            rollback(currentBlockNumber - 1)
            throw ReorgException(message)
        }
    }

    override fun shutDown() {
        setStatus(Status.SHUT_DOWN)
        logger.info("Indexer Shut down")
    }
}
