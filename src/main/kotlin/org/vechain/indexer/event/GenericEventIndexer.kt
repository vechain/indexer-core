package org.vechain.indexer.event

import HexUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.*
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.Transaction
import org.vechain.indexer.thor.model.TxEvent

class GenericEventIndexer<T : EventParameters>(
    private val abiManager: AbiManager,
) {
    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /**
     * Extracts and decodes all events in a block based on loaded ABIs.
     *
     * @param block The block to process.
     * @return A list of decoded events, each paired with its metadata.
     */
    fun getEvents(block: Block): List<Pair<IndexedEvent<T>, T>> {
        return getEventsByFilters(block) // Calls getEventsByFilters without filters
    }

    /**
     * Extracts and decodes events in a block based on specified ABIs, event names, and contract address.
     *
     * @param block The block to process.
     * @param abiNames The list of ABI file names to filter events (e.g., ["b3tr", "erc721"]). Defaults to all.
     * @param eventNames The list of event names to filter (e.g., ["Transfer", "Approval"]). Defaults to all.
     * @param contractAddress The contract address to filter events. Optional.
     * @return A list of decoded events, each paired with its metadata.
     */
    fun getEventsByFilters(
        block: Block,
        abiNames: List<String> = emptyList(),
        eventNames: List<String> = emptyList(),
        contractAddress: String? = null,
    ): List<Pair<IndexedEvent<T>, T>> {
        // Fetch ABIs based on provided ABI names and event names
        val configuredEvents =
            if (abiNames.isNotEmpty()) {
                abiManager.getEventsByNames(abiNames, eventNames)
            } else {
                abiManager
                    .getAbis()
                    .values
                    .flatten()
                    .filter { it.type == "event" }
            }

        // Process events and filter by contract address if provided
        return processEvents(block, configuredEvents, contractAddress)
    }

    /**
     * Processes and decodes events from a block based on provided ABIs.
     *
     * @param block The block containing transactions and events.
     * @param configuredEvents The list of ABIs to use for decoding.
     * @param contractAddress Optional contract address to filter events.
     * @return A list of decoded events, each paired with its metadata.
     */
    private fun processEvents(
        block: Block,
        configuredEvents: List<AbiElement>,
        contractAddress: String? = null,
    ): List<Pair<IndexedEvent<T>, T>> =
        block.transactions.flatMap { tx ->
            tx.outputs.flatMapIndexed { outputIndex, output ->
                output.events
                    .filter { event ->
                        isEventValid(event, configuredEvents, contractAddress)
                    }.mapNotNull { event ->
                        decodeEvent(event, configuredEvents, tx, block, outputIndex)
                    }
            }
        }

    /**
     * Validates if an event matches the provided configured ABIs and contract address filter.
     *
     * @param event The blockchain event to validate.
     * @param configuredEvents The list of ABIs to use for validation.
     * @param contractAddress Optional contract address to filter events.
     * @return True if the event matches the criteria; otherwise, false.
     */
    private fun isEventValid(
        event: TxEvent,
        configuredEvents: List<AbiElement>,
        contractAddress: String?,
    ): Boolean {
        val hasValidTopics = event.topics.isNotEmpty()
        val matchesConfiguredAbis = configuredEvents.any { it.signature == HexUtils.removePrefix(event.topics[0]) }
        val matchesContractAddress = contractAddress == null || event.address.equals(contractAddress, ignoreCase = true)

        return hasValidTopics && matchesConfiguredAbis && matchesContractAddress
    }

    /**
     * Decodes an event and returns a pair of IndexedEvent and its parameters if successful.
     *
     * @param event The blockchain event to decode.
     * @param configuredEvents The list of ABIs to use for decoding.
     * @param tx The transaction containing the event.
     * @param block The block containing the transaction.
     * @param outputIndex The index of the output in the transaction.
     * @return A pair of IndexedEvent and its decoded parameters, or null if decoding fails.
     */
    private fun decodeEvent(
        event: TxEvent,
        configuredEvents: List<AbiElement>,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
    ): Pair<IndexedEvent<T>, T>? {
        val matchingAbi =
            configuredEvents.firstOrNull { abi ->
                HexUtils.compare(abi.signature!!, event.topics[0]) &&
                    abi.inputs.count { it.indexed } + 1 == event.topics.size
            }

        return matchingAbi?.let { abi ->
            try {
                val parameters = EventUtils.decodeEvent(event, abi) as? T ?: GenericEventParameters() as T
                val indexedEvent =
                    IndexedEvent(
                        id = generateEventId(tx.id, outputIndex, event),
                        blockId = block.id,
                        blockNumber = block.number,
                        blockTimestamp = block.timestamp,
                        txId = tx.id,
                        origin = tx.origin,
                        raw = RawEvent(event.data, event.topics),
                        params = parameters,
                        address = event.address,
                        eventType = parameters.getEventType(),
                        clauseIndex = outputIndex.toLong(),
                        signature = event.topics[0],
                    )
                indexedEvent to parameters
            } catch (ex: IllegalArgumentException) {
                logger.warn("Failed to decode event with ABI: ${abi.name}. Skipping event.", ex)
                null
            }
        } ?: run {
            logger.error("No suitable ABI found or decoding failed for event: ${event.topics[0]}")
            null
        }
    }

    /**
     * Generates a unique ID for an event based on its transaction, output, and topic.
     *
     * @param txId The ID of the transaction containing the event.
     * @param outputIndex The index of the transaction output containing the event.
     * @param event The event for which the ID is generated.
     * @return A unique event ID as a string.
     */
    private fun generateEventId(
        txId: String,
        outputIndex: Int,
        event: TxEvent,
    ): String = "$txId-TOPIC-$outputIndex-${event.topics[0]}"
}
