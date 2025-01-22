package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.model.generic.RawEvent
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.Transaction
import org.vechain.indexer.thor.model.TxEvent
import org.web3j.utils.Numeric

class GenericEventIndexer(
    private val abiManager: AbiManager,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /**
     * Extracts and decodes all events in a block based on loaded ABIs.
     */
    fun getEvents(block: Block): List<Pair<IndexedEvent, GenericEventParameters>> = getEventsByFilters(block)

    /**
     * Extracts and decodes events in a block based on specified filters.
     */
    fun getEventsByFilters(
        block: Block,
        abiNames: List<String> = emptyList(),
        eventNames: List<String> = emptyList(),
        contractAddresses: List<String> = emptyList(),
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        val configuredEvents = getConfiguredEvents(abiNames, eventNames)
        return processEvents(block, configuredEvents, contractAddresses)
    }

    /**
     * Retrieves the configured ABIs based on filter criteria.
     */
    private fun getConfiguredEvents(
        abiNames: List<String>,
        eventNames: List<String>,
    ): List<AbiElement> =
        if (abiNames.isNotEmpty()) {
            abiManager.getEventsByNames(abiNames, eventNames)
        } else {
            abiManager
                .getAbis()
                .values
                .flatten()
                .filter { it.type == "event" && (eventNames.isEmpty() || it.name in eventNames) }
        }

    /**
     * Processes and decodes events from a block based on provided ABIs.
     */
    private fun processEvents(
        block: Block,
        configuredEvents: List<AbiElement>,
        contractAddresses: List<String>,
    ): List<Pair<IndexedEvent, GenericEventParameters>> =
        block.transactions.flatMap { tx ->
            tx.outputs.flatMapIndexed { outputIndex, output ->
                output.events.mapIndexedNotNull { eventIndex, event ->
                    if (isEventValid(event, configuredEvents, contractAddresses)) {
                        decodeEvent(event, configuredEvents, tx, block, outputIndex, eventIndex)
                    } else {
                        null
                    }
                }
            }
        }

    /**
     * Validates if an event matches the provided ABIs and contract address filter.
     */
    private fun isEventValid(
        event: TxEvent,
        configuredEvents: List<AbiElement>,
        contractAddresses: List<String>,
    ): Boolean {
        val matchesAbi =
            configuredEvents.any {
                it.signature == Numeric.cleanHexPrefix(event.topics[0])
            }
        val matchesContract = contractAddresses.isEmpty() || contractAddresses.any { it.equals(event.address, ignoreCase = true) }

        return event.topics.isNotEmpty() && matchesAbi && matchesContract
    }

    /**
     * Decodes an event and returns a pair of IndexedEvent and its parameters if successful.
     */
    private fun decodeEvent(
        event: TxEvent,
        configuredEvents: List<AbiElement>,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
        eventIndex: Int,
    ): Pair<IndexedEvent, GenericEventParameters>? {
        val matchingAbi =
            configuredEvents.firstOrNull { abi ->
                abi.signature == Numeric.cleanHexPrefix(event.topics[0]) &&
                    abi.inputs.count { it.indexed } + 1 == event.topics.size
            }

        return matchingAbi?.let { abi ->
            try {
                val parameters = EventUtils.decodeEvent(event, abi)
                IndexedEvent(
                    id = generateEventId(tx.id, outputIndex, eventIndex, event),
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
                ) to parameters
            } catch (ex: IllegalArgumentException) {
                logger.warn("Failed to decode event with ABI: ${abi.name}. Skipping event.", ex)
                null
            }
        }
    }

    /**
     * Generates a unique ID for an event based on its transaction, output, and topic.
     */
    private fun generateEventId(
        txId: String,
        outputIndex: Int,
        eventIndex: Int,
        event: TxEvent,
    ): String {
        val eventHash = Numeric.cleanHexPrefix(event.data).hashCode() // Use hash of data as a unique identifier
        return "$txId-$outputIndex-$eventIndex-${event.topics[0]}-$eventHash"
    }
}
