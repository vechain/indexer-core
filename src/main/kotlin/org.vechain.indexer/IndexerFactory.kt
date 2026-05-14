package org.vechain.indexer

import org.slf4j.LoggerFactory
import org.vechain.indexer.event.CombinedEventProcessor
import org.vechain.indexer.thor.client.DefaultThorClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.TransferCriteria

class IndexerFactory {

    private val logger = LoggerFactory.getLogger(IndexerFactory::class.java)

    private var name: String? = null
    private var thorClient: ThorClient? = null
    private var processor: IndexerProcessor? = null
    private var startBlock: Long? = null
    private var syncLoggerInterval: Long = 1_000L
    private var abiBasePath: String? = null
    private var abiEventNames: List<String> = emptyList()
    private var abiContracts: List<String> = emptyList()
    private var includeVetTransfers: Boolean = false
    private var businessEventBasePath: String? = null
    private var businessEventAbiBasePath: String? = null
    private var businessEventNames: List<String> = emptyList()
    private var businessEventContracts: List<String> = emptyList()
    private var substitutionParams: Map<String, String> = emptyMap()
    private var eventCriteriaSet: List<EventCriteria>? = null
    private var transferCriteriaSet: List<TransferCriteria>? = null
    private var includeFullBlock: Boolean = false
    private var dependsOn: Indexer? = null
    private var callDataClauses: List<Clause>? = null

    fun build(): BlockIndexer {
        requireNotNull(name)
        requireNotNull(thorClient) { "Thor client must be set using thorClient() method." }
        requireNotNull(processor) { "Processor must be set using processor() method." }
        if (!includeFullBlock) {
            require(callDataClauses.isNullOrEmpty()) {
                "Call data inspection clauses can only be used when includeFullBlock() is set."
            }
        }

        val resolvedStartBlock = resolveStartBlock()

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

        val needsVetTransfers = includeVetTransfers || eventProcessor.needsVetTransfers()

        // If `includeFullBlock` is true, return a `BlockIndexer`
        return if (includeFullBlock || dependsOn != null) {
            BlockIndexer(
                name = name!!,
                thorClient = thorClient!!,
                processor = processor!!,
                startBlock = resolvedStartBlock,
                syncLoggerInterval = syncLoggerInterval,
                eventProcessor = eventProcessor,
                inspectionClauses = callDataClauses,
                dependsOn = dependsOn
            )
        } else {

            LogsIndexer(
                name = name!!,
                thorClient = thorClient!!,
                processor = processor!!,
                startBlock = resolvedStartBlock,
                syncLoggerInterval = syncLoggerInterval,
                excludeVetTransfers = !needsVetTransfers,
                logFetchLimit = LOG_FETCH_PAGE_SIZE,
                eventCriteriaSet = eventCriteriaSet ?: eventProcessor.deriveEventCriteria(),
                transferCriteriaSet = transferCriteriaSet ?: emptyList(),
                eventProcessor = eventProcessor,
            )
        }
    }

    // Reconciles the configured startBlock against any dependsOn parent's startBlock so that the
    // dependency component shares a single start block. A child reading the parent's table during
    // processBlock(N) requires the parent to be at exactly N — there is no way to satisfy that if
    // the child starts before the parent. The mismatched-but-correctable case (child > parent) is
    // pulled back with a warning rather than rejected so consumers can be deliberate about
    // misalignment without it being silently accepted.
    private fun resolveStartBlock(): Long {
        val parentStart = dependsOn?.startBlock
        val childStart = startBlock
        return when {
            parentStart == null -> childStart ?: 0L
            childStart == null -> parentStart
            childStart < parentStart ->
                throw IllegalArgumentException(
                    "Indexer '${name}' has startBlock $childStart but its parent " +
                        "'${dependsOn!!.name}' starts at $parentStart. A dependent indexer cannot " +
                        "start before its parent."
                )
            childStart > parentStart -> {
                logger.warn(
                    "Indexer '{}' configured startBlock {} is being overridden to {} to match " +
                        "parent '{}'. Dependents must share their parent's start block.",
                    name,
                    childStart,
                    parentStart,
                    dependsOn!!.name,
                )
                parentStart
            }
            else -> childStart
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
     * We recommend providing a header to identify your application, such as: ``"X-Project-Id" to
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
     * Used to tune how often the indexer will log its progress when syncing.
     *
     * Acts as a throttle: while the indexer is catching up, info-level progress logs are emitted at
     * most once per `interval` seconds. Live-tip processing (status `FULLY_SYNCED`) is not
     * throttled. Debug-level logging, when enabled, is unaffected.
     *
     * The default value is `1000` seconds.
     *
     * @param interval The minimum interval in `seconds` between info-level progress logs.
     */
    fun syncLoggerInterval(interval: Long) = apply {
        require(interval > 0) { "syncLoggerInterval must be > 0" }
        this.syncLoggerInterval = interval
    }

    /**
     * This function allows you to configure the ABI files for the indexer.
     *
     * Matching ABI events will appear in the events list in your `process` function.
     *
     * You must provide the base path for ABI files.
     *
     * All `json` files in the provided path will be loaded as ABI files.
     *
     * @param basePath base path for ABI files.
     */
    fun abis(basePath: String) = apply { this.abiBasePath = basePath }

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
     * This function allows you to configure business events for the indexer.
     *
     * Matching business events will appear in the events list in your `process` function.
     *
     * You must provide the base path for business event files and ABI files.
     *
     * All `json` files in the provided path will be loaded as business event definitions.
     *
     * If a business event references an event that doesn't appear in the ABIs provided an exception
     * will be thrown.
     *
     * @param basePath base path for business event files.
     * @param abiBasePath base path for business event ABI files.
     */
    fun businessEvents(basePath: String, abiBasePath: String) = apply {
        this.businessEventBasePath = basePath
        this.businessEventAbiBasePath = abiBasePath
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
     * If you leave this empty, all business events will be processed.
     *
     * If you provide a list of contract addresses, only events from those contracts will be
     * processed. Please ensure that if you include a list of contracts, that it is comprehensive
     * enough to cover all business events you want to process.
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
     * If a business event file contains a variable like `${REPLACE_ME}`, you must provide a
     * matching substitution parameter or an exception will be thrown.
     *
     * @param substitutionParams map of environment variables to substitute in the business event
     *   files.
     */
    fun businessEventSubstitutionParams(substitutionParams: Map<String, String>) = apply {
        this.substitutionParams = substitutionParams
    }

    /**
     * Opts into including VET transfer events. This increases the number of calls to the Thor API.
     */
    fun includeVetTransfers() = apply { this.includeVetTransfers = true }

    /**
     * VET transfer events are excluded by default. If you previously enabled them, you can disable
     * them again and reduce the number of calls to the Thor API.
     */
    fun excludeVetTransfers() = apply { this.includeVetTransfers = false }

    /**
     * Optional criteria for filtering event logs. This can be used to optimise the call to the Thor
     * API to fetch only the relevant logs.
     *
     * If left unset, criteria are auto-derived from the configured ABIs and contract addresses
     * (cartesian product of `abiContracts` × event topic0s, including business event ABIs). Pass an
     * empty list to disable filtering entirely; pass a custom list to override the default.
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
     * By default, the full block object is not returned to the `process` function. This allows us
     * to sync faster by using log and vet transfer events only.
     *
     * However, if you need access to gas information or reverted transactions you will need to
     * return the full block.
     *
     * When enabled reverted transactions will be included in the `IndexedEvent` list.
     */
    fun includeFullBlock() = apply { this.includeFullBlock = true }

    /** Sets a parent indexer that this indexer depends on. */
    fun dependsOn(indexer: Indexer) = apply { this.dependsOn = indexer }

    /**
     * Sets the clauses to be used for call data inspection. This requires a block by block indexer
     * and cannot be used when fast syncing via log events.
     */
    fun callDataClauses(clauses: List<Clause>) = apply { this.callDataClauses = clauses }

    private companion object {
        const val LOG_FETCH_PAGE_SIZE = 1000L
    }
}
