package org.vechain.indexer

import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.BusinessEventProcessor
import org.vechain.indexer.event.GenericEventIndexer
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier

/** Initial processing backoff duration */
const val INITIAL_BACKOFF_PERIOD = 10_000L

abstract class BlockIndexer(
    protected open val thorClient: ThorClient,
    protected val startBlock: Long = 0L,
    private val syncLoggerInterval: Long = 1_000L,
    protected open val abiManager: AbiManager? = null, // Optional AbiManager
    protected open val businessEventManager: BusinessEventManager? =
        null, // Optional BusinessEventManager
) : Indexer {
    /** The last block that was successfully synchronised */
    private var previousBlock: BlockIdentifier? = null

    /**
     * The Number of indexer iterations remaining in case a given number of iterations has been
     * specified
     */
    internal var remainingIterations: Long? = null

    val name: String
        get() = this.javaClass.simpleName

    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    override var status = Status.SYNCING

    var currentBlockNumber: Long = 0
        protected set

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        internal set

    private var backoffPeriod = 0L

    /** Initialises the indexer processing */
    private fun initialise(blockNumber: Long? = null) {
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
    override fun startInCoroutine(iterations: Long?) {
        CoroutineScope(Dispatchers.Default).launch {
            try {
                start(iterations)
            } catch (e: Exception) {
                logger.error("Error starting indexer ${this.javaClass.simpleName}: ", e)
                throw Exception(e.message, e)
            }
        }
    }

    /** Starts the indexer processing */
    override suspend fun start(iterations: Long?) {
        remainingIterations = iterations

        if (this !is LogsIndexer) initialise()

        logger.info("Starting @ Block: $currentBlockNumber")
        run()
    }

    /** Restarts the processing based on the current indexer status */
    private fun restart() {
        // Initialise the indexer
        when (status) {
            Status.ERROR -> initialise(currentBlockNumber)
            Status.REORG -> initialise(currentBlockNumber - 1)
            else -> initialise()
        }

        logger.info("Restarting indexer @ Block: $currentBlockNumber")
    }

    /** The core indexer logic */
    private tailrec suspend fun run() {
        try {
            if (hasNoRemainingIterations()) return

            backoffDelay()

            if (status == Status.ERROR || status == Status.REORG) restart()

            val block = getBlockFromChain(currentBlockNumber)

            // Check for chain re-organization.
            if (
                currentBlockNumber > startBlock &&
                    previousBlock?.id?.let { it != block.parentID } == true
            ) {
                throw ReorgException(
                    "Chain re-organization detected @ Block $currentBlockNumber with parent block ID ${block.parentID}",
                )
            }

            if (logger.isTraceEnabled) {
                logger.trace("Processing @ Block $currentBlockNumber ($status)")
            } else if (status != Status.SYNCING || currentBlockNumber % syncLoggerInterval == 0L) {
                logger.info("Processing @ Block $currentBlockNumber ($status)")
            }

            processBlock(block)
            postProcessBlock(block)
        } catch (_: BlockNotFoundException) {
            logger.info("Block $currentBlockNumber not found. Indexer may be fully synchronised.")
            handleFullySynced()
            ensureFullySynced()
        } catch (_: ReorgException) {
            logger.error("REORG @ Block $currentBlockNumber")
            handleReorg()
        } catch (e: Exception) {
            logger.error("Error while processing block $currentBlockNumber", e)
            handleError()
        }

        run()
    }

    /**
     * Checks whether there are remaining indexer iterations in case a given number of iterations
     * has been specified
     *
     * @return whether the indexer has remaining iterations
     */
    internal fun hasNoRemainingIterations(): Boolean {
        if (remainingIterations != null) {
            if (remainingIterations!! <= 0) {
                logger.info("Indexer finished at block $currentBlockNumber")
                return true
            }
            remainingIterations = remainingIterations?.dec()
        }
        return false
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
     * current block number
     *
     * @param block the block to undergo post-processing
     */
    private suspend fun postProcessBlock(block: Block) {
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

            logger.info("Success @ Block $currentBlockNumber ($timeSinceLastBlock ms since mine)")
        }

        // Increment the current block.
        currentBlockNumber++

        // Set the previous block id.
        previousBlock = BlockIdentifier(number = block.number, id = block.id)

        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
    }

    /** Ensures that indexer is not behind on-chain best block when in fully synced state */
    private suspend fun ensureFullySynced() {
        if (status == Status.FULLY_SYNCED) {
            val latestBlock = getBestBlockFromChain()
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

    /**
     * Returns the block identified by its number from the chain, or throw a BlockNotFoundException
     * if it doesn't exist.
     *
     * @param blockNumber the block number
     * @return the block corresponding to the number
     * @throws BlockNotFoundException if no block is found with that number
     */
    private suspend fun getBlockFromChain(blockNumber: Long): Block =
        thorClient.getBlock(blockNumber)

    /**
     * Returns the latest block from the chain
     *
     * @return the chain best block
     * @throws BlockNotFoundException if not found
     */
    private suspend fun getBestBlockFromChain(): Block = thorClient.getBestBlock()

    /**
     * @param block The block containing events to process.
     * @param criteria Filtering criteria to determine which events to process.
     * @return A list of decoded events and their associated parameters.
     * @notice Processes all events (generic and business) in a block based on the provided
     *   criteria.
     * @dev Updates the filter criteria with business event names if applicable. Decodes and
     *   processes both generic and business events.
     */
    protected open fun processAllEvents(
        block: Block,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<IndexedEvent> {
        val updatedCriteria =
            businessEventManager?.updateCriteriaWithBusinessEvents(criteria) ?: criteria
        val decodedEvents = processBlockGenericEvents(block, updatedCriteria)
        return processBusinessEvents(
            decodedEvents,
            updatedCriteria.businessEventNames,
            updatedCriteria.removeDuplicates
        )
    }

    /**
     * @param block The block containing events to process.
     * @param criteria Filtering criteria to determine which generic events to process.
     * @return A list of decoded generic events and their associated parameters.
     * @notice Processes generic events in a block based on the provided criteria.
     * @dev Requires the `AbiManager` to decode events. If not configured, skips processing and
     *   returns an empty list.
     */
    protected open fun processBlockGenericEvents(
        block: Block,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<IndexedEvent> {
        if (abiManager == null) {
            logger.warn("ABI Manager is not configured. Skipping generic event processing.")
            return emptyList()
        }

        val eventIndexer = GenericEventIndexer(abiManager!!)
        return eventIndexer.getBlockEventsByFilters(
            block = block,
            filterCriteria = criteria,
        )
    }

    /**
     * @param decodedEvents The list of previously decoded generic events and their parameters.
     * @param criteria Filtering criteria to determine which business events to process.
     * @return A list of processed business events and their associated parameters.
     * @notice Processes only business events in a block based on the provided decoded generic
     *   events.
     * @dev Requires the `BusinessEventManager` to decode and process business events.
     */
    protected open fun processBlockBusinessEvents(
        decodedEvents: List<IndexedEvent>,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<IndexedEvent> {
        if (businessEventManager == null) {
            logger.warn(
                "Business Event Manager is not configured. Skipping business event processing."
            )
            return emptyList()
        }

        // Process business events
        val processor = BusinessEventProcessor(businessEventManager!!)
        return processor.getOnlyBusinessEvents(
            decodedEvents,
            criteria.businessEventNames,
            this is LogsIndexer
        )
    }

    /**
     * @param decodedEvents A list of decoded generic events and their associated parameters.
     * @param businessEventNames A list of business event names to process.
     * @return A list of processed business events and their associated parameters.
     * @notice Processes business events from a list of decoded events.
     * @dev Utilizes the `BusinessEventProcessor` to process events if the `BusinessEventManager` is
     *   configured. Logs a debug message if the `BusinessEventManager` is not present.
     */
    internal fun processBusinessEvents(
        decodedEvents: List<IndexedEvent>,
        businessEventNames: List<String>,
        removeDuplicates: Boolean? = true,
    ): List<IndexedEvent> {
        if (businessEventManager != null) {
            val processor = BusinessEventProcessor(businessEventManager!!)
            val logsIndexer = this is LogsIndexer
            return processor.processEvents(
                decodedEvents,
                businessEventNames,
                removeDuplicates ?: true,
                logsIndexer
            )
        }
        logger.debug("Skipping business event processing as manager is missing.")
        return decodedEvents
    }
}
