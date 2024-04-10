import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.INITIAL_BACKOFF_PERIOD
import org.vechain.indexer.Status
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.*
import java.time.LocalDateTime
import java.time.ZoneOffset

abstract class LogsIndexer(
    private val criteriaSet: List<CriteriaSet>,
    protected open val thorClient: ThorClient,
    private val startBlock: Long = 0L,
    private val syncLoggerInterval: Long = 1_000L,
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

    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    var status = Status.SYNCING
        private set

    var currentBlockNumber: Long = 0
        private set

    var timeLastProcessed: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
        private set

    private var backoffPeriod = 0L

    /** Initialises the indexer processing */
    private fun initialise(
        blockNumber: Long? = null,
    ) {
        // If no block number is provided, get the last synced block. If no block is found, start from the beginning.
        val lastSyncedBlockNumber = blockNumber ?: getLastSyncedBlock()?.number ?: startBlock

        // To ensure data integrity roll back changes made in the last block
        rollback(lastSyncedBlockNumber)

        // Initialise fields
        currentBlockNumber = lastSyncedBlockNumber
        status = Status.SYNCING

        // Set the previous block to the previously synced block if it exists, or null otherwise.
        val lastBlock = getLastSyncedBlock()
        previousBlock = if (lastBlock?.number == lastSyncedBlockNumber - 1L) {
            lastBlock
        } else null
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
    suspend fun start(iterations: Long? = null) {
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

        val results = mutableMapOf<Number, List<EventLog>>()
        var offset = 0
        val amountOfBlocks = 10_000
        val limit = 10_000

        logger.info("Processing logs from block $currentBlockNumber")

        do {
            val request = EventFilter(
                criteriaSet = criteriaSet,
                range = Range(from = currentBlockNumber, to = currentBlockNumber + amountOfBlocks, unit = "block"),
                order = "asc",
                options = Options(offset = offset, limit = limit)
            )

            offset += limit

            val response = getLogsFromChain(request)

            //put each log in the map, indexed by block number
            response.forEach { log ->
                val blockNumber = log.meta.blockNumber
                results[blockNumber] = results.getOrDefault(blockNumber, emptyList()) + log
            }

        } while (response.isNotEmpty())

        if (results.isNotEmpty()) {
            processLogs(results)
        }

        postProcessLogs(results, amountOfBlocks)

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

    private suspend fun postProcessLogs(logs: Map<Number, List<EventLog>>, amountOfBlocks: Int){

        if (status == Status.FULLY_SYNCED && currentBlockNumber % 20 == 0L) {
            ensureFullySynced()
        }

        if (status == Status.FULLY_SYNCED) {
            val lastLog = logs.values.flatten().maxByOrNull { it.meta.blockNumber }!!
            val currentEpoch =
                LocalDateTime.now(ZoneOffset.UTC).toInstant(ZoneOffset.UTC).toEpochMilli()
            val timeSinceLastBlock = maxOf(currentEpoch - lastLog.meta.blockTimestamp.times(1000), 0)
            backoffPeriod = maxOf(0, INITIAL_BACKOFF_PERIOD - (timeSinceLastBlock)) + 100

            logger.info("Success @ Block $currentBlockNumber ($timeSinceLastBlock ms since mine)")
        }

        currentBlockNumber += amountOfBlocks
        val prev = thorClient.getBlock(currentBlockNumber - 1)
        previousBlock = BlockIdentifier(number = prev.number, id = prev.id)
        timeLastProcessed = LocalDateTime.now(ZoneOffset.UTC)
    }

    /** Ensures that indexer is not behind on-chain best block when in fully synced state */
    private suspend fun ensureFullySynced() {
        if (status == Status.FULLY_SYNCED) {
            val latestBlock = thorClient.getBestBlock()
            if (latestBlock.number > currentBlockNumber) {
                logger.info(
                    "$name - Changing status to SYNCING (indexerBlock=${currentBlockNumber}, latestBlock=${latestBlock.number})"
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
     * @param criteriaSet the criteria set to be used to query the chain
     * @return the
     * @throws BlockNotFoundException if no block is found with that number
     */
    private suspend fun getLogsFromChain(criteriaSet: EventFilter): List<EventLog> {
        return thorClient.queryEventLogs(criteriaSet)
    }


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
     * @param logs the event logs to be processed
     */
    abstract fun processLogs(logs: Map<Number, List<EventLog>>)
}
