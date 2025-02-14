package org.vechain.indexer

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.BusinessEventProcessor
import org.vechain.indexer.event.GenericEventIndexer
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.exception.ReorgException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import java.time.LocalDateTime
import java.time.ZoneOffset

/** The possible states the indexer can be */
enum class Status {
    /** Indexer is processing blocks */
    SYNCING,

    /** Indexing is up-to-date with the latest on-chain block */
    FULLY_SYNCED,

    /** A chain re-organization has been detected during processing */
    REORG,

    /** Indexer encountered an unknown exception during processing */
    ERROR,
}

/** Initial processing backoff duration */
const val INITIAL_BACKOFF_PERIOD = 10_000L

abstract class BlockIndexer(
    protected open val thorClient: ThorClient,
    protected val startBlock: Long = 0L,
    private val syncLoggerInterval: Long = 1_000L,
    protected open val abiManager: AbiManager? = null, // Optional AbiManager
    protected open val businessEventManager: BusinessEventManager? = null, // Optional BusinessEventManager
) {
    /** The last block that was successfully synchronised */
    private var previousBlock: BlockIdentifier? = null

    /**
     * Number of indexer iterations remaining in case a given number of iterations has been
     * specified
     */
    private var remainingIterations: Long? = null

    val name: String
        get() = this.javaClass.simpleName

    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    var status = Status.SYNCING
        private set

    var currentBlockNumber: Long = 0
        private set

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        private set

    private var backoffPeriod = 0L

    /** Initialises the indexer processing */
    private fun initialise(blockNumber: Long? = null) {
        // If no block number is provided, get the last synced block. If no block is found, start from the beginning.
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
    fun startInCoroutine(iterations: Long? = null) {
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
    open suspend fun start(iterations: Long? = null) {
        remainingIterations = iterations

        // Initialise the indexer
        initialise()

        logger.info("Starting @ Block: $currentBlockNumber")
        run()
    }

    /** Restarts the processing based on the current indexer status */
    private suspend fun restart() {
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
            if (currentBlockNumber > startBlock && previousBlock?.id?.let { it != block.parentID } == true) {
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
        } catch (ex: BlockNotFoundException) {
            logger.info("Block $currentBlockNumber not found. Indexer may be fully synchronised.")
            handleFullySynced()
            ensureFullySynced()
        } catch (e: ReorgException) {
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
     * @return whether indexer has remaining iterations
     */
    private fun hasNoRemainingIterations(): Boolean {
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

    private fun handleError() {
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

    private suspend fun backoffDelay() {
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
    private suspend fun getBlockFromChain(blockNumber: Long): Block = thorClient.getBlock(blockNumber)

    /**
     * Returns the latest block from the chain
     *
     * @return the chain best block
     * @throws BlockNotFoundException if not found
     */
    private suspend fun getBestBlockFromChain(): Block = thorClient.getBestBlock()

    /**
     * Returns the last block that was successfully processed.
     * If no block was processed, returns null.
     *
     * @return last synced block
     */
    abstract fun getLastSyncedBlock(): BlockIdentifier?

    /**
     * Rolls back changes made in the given block number. The block number will always be the last
     * synchronized block. It is provided as a parameter here for convenience.
     *
     * @param blockNumber the block number to be rolled back
     */
    abstract fun rollback(blockNumber: Long)

    /**
     * Holds the business logic for this indexer.
     *
     * @param block the block to be processed
     */
    abstract fun processBlock(block: Block)

    /**
     * @notice Processes all events (generic and business) in a block based on the provided criteria.
     * @dev Updates the filter criteria with business event names if applicable.
     *      Decodes and processes both generic and business events.
     * @param block The block containing events to process.
     * @param criteria Filtering criteria to determine which events to process.
     * @return A list of decoded events and their associated parameters.
     */
    protected open fun processAllEvents(
        block: Block,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        val updatedCriteria = updateCriteriaWithBusinessEvents(criteria)
        val decodedEvents = processBlockGenericEvents(block, updatedCriteria)
        return processBusinessEvents(decodedEvents, updatedCriteria.businessEventNames, updatedCriteria.removeDuplicates)
    }

    /**
     * @notice Processes generic events in a block based on the provided criteria.
     * @dev Requires the `AbiManager` to decode events. If not configured, skips processing and returns an empty list.
     * @param block The block containing events to process.
     * @param criteria Filtering criteria to determine which generic events to process.
     * @return A list of decoded generic events and their associated parameters.
     */
    protected open fun processBlockGenericEvents(
        block: Block,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
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
     * @notice Processes only business events in a block based on the provided decoded generic events.
     * @dev Requires the `BusinessEventManager` to decode and process business events.
     * @param decodedEvents The list of previously decoded generic events and their parameters.
     * @param criteria Filtering criteria to determine which business events to process.
     * @return A list of processed business events and their associated parameters.
     */
    protected open fun processBlockBusinessEvents(
        decodedEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
        criteria: FilterCriteria = FilterCriteria(),
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        if (businessEventManager == null) {
            logger.warn("Business Event Manager is not configured. Skipping business event processing.")
            return emptyList()
        }

        // Filter events based on criteria if needed
        val filteredCriteria = updateCriteriaWithBusinessEvents(criteria)

        // Process business events
        val processor = BusinessEventProcessor(businessEventManager!!)
        return processor.getOnlyBusinessEvents(decodedEvents, filteredCriteria.businessEventNames)
    }

    /**
     * @notice Updates the filter criteria with business event names if applicable.
     * @dev Retrieves the generic event names for the specified business event names
     *      using the `BusinessEventManager` and adds them to the criteria.
     * @param criteria The original filtering criteria.
     * @return Updated filtering criteria with additional event names for business events.
     */
    private fun updateCriteriaWithBusinessEvents(criteria: FilterCriteria): FilterCriteria {
        if (criteria.businessEventNames.isNotEmpty() && businessEventManager != null) {
            val names = businessEventManager!!.getBusinessGenericEventNames(criteria.businessEventNames)
            return criteria.addBusinessEventNames(names)
        }
        return criteria
    }

    /**
     * @notice Processes business events from a list of decoded events.
     * @dev Utilizes the `BusinessEventProcessor` to process events if the `BusinessEventManager` is configured.
     *      Logs a debug message if the `BusinessEventManager` is not present.
     * @param decodedEvents A list of decoded generic events and their associated parameters.
     * @param businessEventNames A list of business event names to process.
     * @return A list of processed business events and their associated parameters.
     */
    private fun processBusinessEvents(
        decodedEvents: List<Pair<IndexedEvent, GenericEventParameters>>,
        businessEventNames: List<String>,
        removeDuplicates: Boolean? = true,
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        if (businessEventManager != null) {
            val processor = BusinessEventProcessor(businessEventManager!!)
            return processor.processEvents(decodedEvents, businessEventNames, removeDuplicates ?: true)
        }
        logger.debug("Skipping business event processing as manager is missing.")
        return decodedEvents
    }
}

class LogIndexer {
}