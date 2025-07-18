package org.vechain.indexer

import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.thor.client.DefaultThorClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.TransferCriteria

class IndexerFactory {

    private var name: String? = null
    private var thorClient: ThorClient? = null
    private var processor: IndexerProcessor? = null
    private var startBlock: Long = 0L
    private var abiBasePath: String? = null
    private var abiEventNames: List<String> = emptyList()
    private var abiContracts: List<String> = emptyList()
    private var includeVetTransfers: Boolean = true
    private var businessEventBasePath: String? = null
    private var businessEventAbiBasePath: String? = null
    private var businessEventNames: List<String> = emptyList()
    private var businessEventContracts: List<String> = emptyList()
    private var substitutionParams: Map<String, String> = emptyMap()
    private var syncLoggerInterval: Long = 1_000L
    private var blockBatchSize: Long = 100L //  Block batch size
    private var logFetchLimit: Long = 1000L //  Limits logs per API call (pagination)
    private var pruner: Pruner? = null
    private var eventCriteriaSet: List<EventCriteria>? = null
    private var transferCriteriaSet: List<TransferCriteria>? = null
    private var includeFullBlock: Boolean = false

    fun build(): Indexer {
        requireNotNull(name)
        requireNotNull(thorClient) { "Thor client must be set using thorClient() method." }
        requireNotNull(processor) { "Processor must be set using processor() method." }

        val eventProcessor =
            CombinedEventProcessor.create(
                abiBasePath = abiBasePath,
                abiEventNames = abiEventNames,
                abiContracts = abiContracts,
                includeVetTransfers = includeVetTransfers,
                businessEventPath = businessEventBasePath,
                businessEventAbiBasePath = businessEventAbiBasePath,
                businessEventNames = businessEventNames,
                businessEventContracts = businessEventContracts,
                substitutionParams = substitutionParams,
            )

        // If `includeFullBlock` is true, return a `BlockIndexer`
        return if (includeFullBlock) {
            BlockIndexer(
                name = name!!,
                thorClient = thorClient!!,
                processor = processor!!,
                startBlock = startBlock,
                syncLoggerInterval = syncLoggerInterval,
                pruner = pruner,
                eventProcessor = eventProcessor,
            )
        } else {
            LogsIndexer(
                name = name!!,
                thorClient = thorClient!!,
                processor = processor!!,
                startBlock = startBlock,
                syncLoggerInterval = syncLoggerInterval,
                excludeVetTransfers = !includeVetTransfers,
                blockBatchSize = blockBatchSize,
                logFetchLimit = logFetchLimit,
                pruner = pruner,
                eventCriteriaSet = eventCriteriaSet ?: emptyList(),
                transferCriteriaSet = transferCriteriaSet ?: emptyList(),
                eventProcessor = eventProcessor,
            )
        }
    }

    // Setters for configuration options
    /**
     * Sets the name of the indexer.
     *
     * This is used for logging and identification purposes.
     *
     * @param name The name of the indexer.
     */
    fun name(name: String) = apply { this.name = name }

    /**
     * Sets the Thor client to be used by the indexer.
     *
     * We recommend providing a header to identify your application, such as: ``"X-Project_Id" to
     * "your-project-id"``
     *
     * @param baseUrl The base URL of the Thor client.
     * @param headers Optional headers to be included in requests.
     */
    fun thorClient(
        baseUrl: String,
        vararg headers: Pair<String, Any>,
    ) = apply { this.thorClient = DefaultThorClient(baseUrl, *headers) }

    /**
     * Sets the Thor client to be used by the indexer.
     *
     * @param thorClient The Thor client instance to use.
     */
    fun thorClient(thorClient: ThorClient) = apply { this.thorClient = thorClient }

    /**
     * Sets the processor function to handle indexed events.
     *
     * This function will be called with a list of indexed events after they are processed.
     *
     * @param processor The function to process indexed events.
     */
    fun processor(processor: IndexerProcessor) = apply { this.processor = processor }

    /**
     * Sets the starting block number for the indexer.
     *
     * The indexer will skip blocks before this number.
     *
     * @param startBlock The block number to start indexing from.
     */
    fun startBlock(startBlock: Long) = apply { this.startBlock = startBlock }

    /**
     * Sets the base path for ABI files.
     *
     * Will load all `json` files in this directory and one level deeper.
     *
     * @param basePath base path for ABI files.
     */
    fun abiBasePath(basePath: String) = apply { this.abiBasePath = basePath }

    /**
     * Sets the event names to be used for filtering ABI events.
     *
     * This is useful when you want to process only specific events from the ABI files.
     *
     * @param eventNames List of event names to filter.
     */
    fun abiEventNames(eventNames: List<String>) = apply { this.abiEventNames = eventNames }

    /**
     * Sets the contract addresses to be used for filtering events.
     *
     * This is useful when you want to process events only from specific contracts.
     *
     * @param abiContracts List of contract addresses to filter.
     */
    fun abiContracts(abiContracts: List<String>) = apply { this.abiContracts = abiContracts }

    /**
     * Sets the base path for business event files.
     *
     * Will load all `json` files in this directory and one level deeper.
     *
     * @param basePath base path for business event files.
     */
    fun businessEventBasePath(basePath: String) = apply { this.businessEventBasePath = basePath }

    /**
     * Sets the base path for business event ABI files.
     *
     * Will load all `json` files in this directory and one level deeper.
     *
     * This is distinct from the `abiBasePath` which is used for loading abi events. This parameter
     * allows you to define only the abis that are used by your business events. All abis referenced
     * in your business events must be contained in these abi files or an error will be thrown.
     *
     * @param basePath base path for business event ABI files.
     */
    fun businessEventAbiBasePath(basePath: String) = apply {
        this.businessEventAbiBasePath = basePath
    }

    /**
     * Sets the business event names to be used for filtering business events.
     *
     * This is useful when you want to process only specific business events from the files.
     *
     * @param eventNames List of business event names to filter.
     */
    fun businessEventNames(eventNames: List<String>) = apply {
        this.businessEventNames = eventNames
    }

    /**
     * Sets the business event contracts to be used for filtering business events.
     *
     * @param contracts List of contract addresses to filter business events.
     */
    fun businessEventContracts(contracts: List<String>) = apply {
        this.businessEventContracts = contracts
    }

    /**
     * Business event substitution parameters.
     *
     * This is used to substitute environment variables in the business event files.
     *
     * @param substitutionParams map of environment variables to substitute in the business event
     *   files.
     */
    fun businessEventSubstitutionParams(substitutionParams: Map<String, String>) = apply {
        this.substitutionParams = substitutionParams
    }

    /**
     * Vet transfer events will be included by default. If you don't need them, you can exclude them
     * and reduce the number of calls to the Thor API.
     */
    fun excludeVetTransfers() = apply { this.includeVetTransfers = false }

    /**
     * Sets the pruner to be used by the indexer.
     *
     * The pruner will be called periodically to remove old blocks from the indexer.
     *
     * @param pruner The pruner to use for removing old data
     */
    fun pruner(pruner: Pruner) = apply { this.pruner = pruner }

    /**
     * Optional criteria for filtering event logs. This can be used to optimise the call to the Thor
     * API to fetch only the relevant logs.
     */
    fun eventCriteriaSet(criteria: List<EventCriteria>) = apply { this.eventCriteriaSet = criteria }

    /**
     * Optional criteria for filtering transfer logs. This can be used to optimise the call to the
     * Thor API to fetch only the relevant VET transfers.
     */
    fun transferCriteriaSet(criteria: List<TransferCriteria>) = apply {
        this.transferCriteriaSet = criteria
    }

    /**
     * Used to tune how often the indexer will log its progress when syncing.
     *
     * The default value is `1000` blocks
     *
     * @param interval The interval in `blocks` for logging progress.
     */
    fun syncLoggerInterval(interval: Long) = apply { this.syncLoggerInterval = interval }

    /**
     * Sets the block bach size for retrieving events logs and transfers from the Thor API.
     *
     * The default value is `100` blocks.
     *
     * @param size The size of the block range.
     */
    fun blockBatchSize(size: Long) = apply { this.blockBatchSize = size }

    /**
     * Sets the limit for the number of event logs or transfers fetched per Thor API call.
     *
     * The default value is `1000` logs.
     *
     * @param limit The maximum number of logs to fetch per API call.
     */
    fun logFetchLimit(limit: Long) = apply { this.logFetchLimit = limit }

    /**
     * By default, the full block object is not returned to the `process` function. This allows us
     * to sync faster by using log and vet transfer events only.
     *
     * However, if you need access to gas information or reverted transactions you will need to
     * return the full block.
     *
     * When enabled reverted transactions will be included in the `IndexedEvent` list.
     */
    fun includeFullBlock() = apply { this.includeFullBlock = true }
}
