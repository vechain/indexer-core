package org.vechain.indexer

import org.vechain.indexer.event.EventProcessor
import org.vechain.indexer.thor.client.DefaultThorClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.TransferCriteria

class IndexerFactory {

    private var name: String? = null
    private var thorClient: ThorClient? = null
    private var processor: IndexerProcessor? = null
    private var startBlock: Long = 0L
    private var abiFiles: List<String> = emptyList()
    private var eventNames: List<String> = emptyList()
    private var contractAddresses: List<String> = emptyList()
    private var includeVetTransfers: Boolean = true
    private var includeEvents: Boolean = true
    private var businessEventFiles: List<String> = emptyList()
    private var businessEventNames: List<String> = emptyList()
    private var removeDuplicates: Boolean = true
    private var onlyBusinessEvents: Boolean = true
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
            EventProcessor.create(
                abiFiles = abiFiles,
                eventNames = eventNames,
                contractAddresses = contractAddresses,
                includeVetTransfers = includeVetTransfers,
                includeEvents = includeEvents,
                businessEventFiles = businessEventFiles,
                businessEventNames = businessEventNames,
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
                excludeLogEvents = !includeEvents,
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
     * Sets the ABI files to be used for decoding event logs.
     *
     * @param abiFiles List of ABI file paths.
     */
    fun abis(abiFiles: List<String>) = apply { this.abiFiles = abiFiles }

    /**
     * Sets the event names to be used for filtering ABI events.
     *
     * This is useful when you want to process only specific events from the ABI files.
     *
     * @param eventNames List of event names to filter.
     */
    fun eventNames(eventNames: List<String>) = apply { this.eventNames = eventNames }

    /**
     * Sets the contract addresses to be used for filtering events.
     *
     * This is useful when you want to process events only from specific contracts.
     *
     * @param contractAddresses List of contract addresses to filter.
     */
    fun contractAddresses(contractAddresses: List<String>) = apply {
        this.contractAddresses = contractAddresses
    }

    /**
     * Sets the business event files to be used for processing business events.
     *
     * @param eventFiles List of business event file paths.
     */
    fun businessEvents(eventFiles: List<String>) = apply { this.businessEventFiles = eventFiles }

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
     * If set to `true`, the indexer will remove the abi events that match a business event.
     *
     * Generally this will always be set to `true` and that is the default value.
     *
     * @param removeDuplicates If `true`, abi events that match a business event will be removed.
     */
    fun removeDuplicates(removeDuplicates: Boolean) = apply {
        this.removeDuplicates = removeDuplicates
    }

    /**
     * If set to `true`, the indexer will only process business events. This is useful when you want
     * to focus on business events only and ignore other events.
     *
     * @param onlyBusinessEvents If `true`, only business events will be processed.
     */
    fun onlyBusinessEvents(onlyBusinessEvents: Boolean) = apply {
        this.onlyBusinessEvents = onlyBusinessEvents
    }

    /**
     * Vet transfer events will be included by default. If you don't need them, you can exclude them
     * and reduce the number of calls to the Thor API.
     */
    fun excludeVetTransfers() = apply { this.includeVetTransfers = false }

    /**
     * Event logs will be included by default. If you don't need them, you can exclude them and
     * reduce the number of calls to the Thor API.
     *
     * If you are using abis or business events, you might want to keep this enabled
     */
    fun excludeEvents() = apply { this.includeEvents = false }

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
