package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier

/** Initial processing backoff duration */
const val INITIAL_BACKOFF_PERIOD = 10_000L

private const val DEPENDENCY_CHECK_INTERVAL_MS = 50L

open class BlockIndexer(
    override val name: String,
    protected open val thorClient: ThorClient,
    private val processor: IndexerProcessor,
    protected val startBlock: Long,
    private val syncLoggerInterval: Long,
    protected open val eventProcessor: CombinedEventProcessor?,
    override val pruner: Pruner? = null,
    private val prunerInterval: Long,
    override val dependsOn: Set<Indexer>,
) : Indexer {
    /** The last block that was successfully synchronised */
    protected var previousBlock: BlockIdentifier? = null

    // A random number between 0 and `prunerInterval`. This makes it less like that all pruners will
    // run at the same time.
    private val prunerIntervalOffset = (0 until prunerInterval).random()

    protected val logger: Logger = LoggerFactory.getLogger(name)

    override var status = Status.SYNCING

    private var dependencyResumeStatus: Status? = null

    var currentBlockNumber: Long = 0
        protected set

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        internal set

    private var backoffPeriod = 0L

    /** Initialises the indexer processing */
    protected fun initialise(blockNumber: Long? = null) {
        // If no block number is provided, get the last synced block. If no block is found, start
        // from the beginning.
        val lastSyncedBlockNumber = blockNumber ?: getLastSyncedBlock()?.number ?: startBlock

        // To ensure data integrity roll back changes made in the last block
        rollback(lastSyncedBlockNumber)

        // Initialise fields
        currentBlockNumber = lastSyncedBlockNumber
        status = Status.SYNCING

        // Set the previous block to the previously synced block if it exists, or null otherwise.
        val lastBlock = getLastSyncedBlock()
        previousBlock =
            if (lastBlock?.number == lastSyncedBlockNumber - 1L) {
                lastBlock
            } else {
                null
            }
    }

    /**
     * Triggers the non-blocking suspendable start() function inside its required coroutine scope.
     */
    override fun startInCoroutine(scope: CoroutineScope) {
        scope.launch {
            try {
                start()
            } catch (e: Exception) {
                logger.error("Error starting indexer ${this.javaClass.simpleName}: ", e)
                throw Exception(e.message, e)
            }
        }
    }

    /** Starts the indexer */
    override suspend fun start() {
        initialise()
        waitForDependenciesIfRequired()

        logger.info("Starting @ Block: $currentBlockNumber")
        run()
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

    /** The core indexer logic */
    protected open suspend fun run() {
        while (true) {
            runOnce()
        }
    }

    protected suspend fun runOnce() {
        try {
            waitForDependenciesIfRequired()
            backoffDelay()

            if (status == Status.ERROR || status == Status.REORG) restart()

            val block = thorClient.getBlock(currentBlockNumber)

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

            if (
                logger.isTraceEnabled ||
                    status != Status.SYNCING ||
                    currentBlockNumber % syncLoggerInterval == 0L
            ) {
                logger.info("($status) Processing Block  $currentBlockNumber")
            }

            val events = eventProcessor?.processEvents(block) ?: emptyList()
            process(events, block)
            postProcessBlock(block)
            runPruner()
        } catch (_: BlockNotFoundException) {
            logger.info("Block $currentBlockNumber not found. Indexer may be fully synchronised.")
            handleFullySynced()
            ensureFullySynced()
        } catch (_: ReorgException) {
            handleReorg()
        } catch (e: Exception) {
            logger.error("Error while processing block $currentBlockNumber", e)
            handleError()
        }
    }

    private fun handleFullySynced() {
        backoffPeriod = 4000
        status = Status.FULLY_SYNCED
    }

    internal fun handleError() {
        backoffPeriod = INITIAL_BACKOFF_PERIOD
        status = Status.ERROR
    }

    private fun handleReorg() {
        backoffPeriod = INITIAL_BACKOFF_PERIOD
        status = Status.REORG
    }

    /**
     * Ensures that indexer is fully synced, recalculates the backoff period & increments the
     * current block number.
     *
     * @param block the block to undergo post-processing
     */
    protected suspend fun postProcessBlock(block: Block) {

        // If this block is not the block after currentBlockNumber, then we are in an invalid state.
        if (block.number != currentBlockNumber) {
            throw IllegalStateException(
                "Block number mismatch: expected $currentBlockNumber, got ${block.number}",
            )
        }

        // Every 20 blocks, check if we are fully synced.
        if (status == Status.FULLY_SYNCED && currentBlockNumber % 20 == 0L) {
            ensureFullySynced()
        }

        // If we are fully synced, recalculate the backoff period.
        if (status == Status.FULLY_SYNCED) {
            val currentEpoch =
                LocalDateTime.now(ZoneOffset.UTC).toInstant(ZoneOffset.UTC).toEpochMilli()
            val timeSinceLastBlock = maxOf(currentEpoch - block.timestamp.times(1000), 0)
            backoffPeriod = maxOf(0, INITIAL_BACKOFF_PERIOD - (timeSinceLastBlock)) + 100

            logger.debug("Success @ Block $currentBlockNumber ($timeSinceLastBlock ms since mine)")
        }

        // Set the current block number to the next block.
        currentBlockNumber = block.number + 1

        // Set the previous block id.
        previousBlock = BlockIdentifier(number = block.number, id = block.id)

        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
    }

    /**
     * Runs the pruner service if the indexer is in a fully synced state and the current block. The
     * pruner will run every `prunerInterval` blocks, offset by a random number between 0 and
     * `prunerInterval` to ensure that not all indexers run the pruner at the same time.
     */
    protected fun runPruner() {
        if (status != Status.FULLY_SYNCED) return
        pruner?.let {
            if (currentBlockNumber % prunerInterval == prunerIntervalOffset) {
                status = Status.PRUNING
                it.run(currentBlockNumber)
                status = Status.FULLY_SYNCED
            }
        }
    }

    /** Ensures that indexer is not behind on-chain best block when in fully synced state */
    protected suspend fun ensureFullySynced() {
        if (status == Status.FULLY_SYNCED) {
            val latestBlock = thorClient.getBestBlock()
            if (latestBlock.number > currentBlockNumber) {
                logger.info(
                    "$name - Changing status to SYNCING (indexerBlock=$currentBlockNumber, latestBlock=${latestBlock.number})",
                )
                status = Status.SYNCING
            }
        }
    }

    internal suspend fun backoffDelay() {
        if (status != Status.SYNCING) {
            delay(backoffPeriod)
        }
    }

    override fun getLastSyncedBlock(): BlockIdentifier? = processor.getLastSyncedBlock()

    override fun rollback(blockNumber: Long) = processor.rollback(blockNumber)

    override fun process(matchedEvents: List<IndexedEvent>, block: Block?) =
        processor.process(matchedEvents, block)

    protected open suspend fun waitForDependenciesIfRequired() {
        if (dependsOn.isEmpty()) {
            dependencyResumeStatus = null
            return
        }

        if (dependenciesReady()) {
            if (status == Status.PENDING_DEPENDENCY) {
                status = dependencyResumeStatus ?: Status.SYNCING
            }
            dependencyResumeStatus = null
            return
        }

        if (status != Status.PENDING_DEPENDENCY) {
            dependencyResumeStatus = status
        }

        status = Status.PENDING_DEPENDENCY

        while (!dependenciesReady()) {
            delay(DEPENDENCY_CHECK_INTERVAL_MS)
        }

        status = dependencyResumeStatus ?: Status.SYNCING
        dependencyResumeStatus = null
    }

    private fun dependenciesReady(): Boolean = dependsOn.all { it.status == Status.FULLY_SYNCED }
}
