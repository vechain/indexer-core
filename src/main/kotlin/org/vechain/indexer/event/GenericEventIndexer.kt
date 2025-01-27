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
import org.vechain.indexer.thor.model.TxOutputs
import org.vechain.indexer.utils.DataUtils

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
        vetTransfers: Boolean = false,
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        val configuredEvents = getConfiguredEvents(abiNames, eventNames)
        return processEvents(block, configuredEvents, contractAddresses, vetTransfers)
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
        vetTransfers: Boolean,
    ): List<Pair<IndexedEvent, GenericEventParameters>> {
        val events = mutableListOf<Pair<IndexedEvent, GenericEventParameters>>()

        block.transactions.forEach { tx ->
            tx.outputs.forEachIndexed { outputIndex, output ->
                output.events.forEachIndexed { eventIndex, event ->
                    if (isEventValid(event, configuredEvents, contractAddresses)) {
                        decodeEvent(event, configuredEvents, tx, block, outputIndex, eventIndex)?.let {
                            events.add(it)
                        }
                    }
                }
                if (vetTransfers) {
                    events.addAll(extractVetTransfers(output, tx, block, outputIndex))
                }
            }
        }
        return events
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
                it.signature == DataUtils.removePrefix(event.topics[0])
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
                abi.signature == DataUtils.removePrefix(event.topics[0]) &&
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
     * Extracts VET transfers from a transaction output and returns them as events.
     */
    private fun extractVetTransfers(
        output: TxOutputs,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
    ): List<Pair<IndexedEvent, GenericEventParameters>> =
        output.transfers.mapIndexedNotNull { transferIndex, transfer ->
            try {
                val parameters =
                    GenericEventParameters(
                        mapOf(
                            "from" to transfer.sender,
                            "to" to transfer.recipient,
                            "amount" to DataUtils.decodeQuantity(transfer.amount),
                        ),
                        "VET_TRANSFER",
                    )
                val indexedEvent =
                    IndexedEvent(
                        id = generateEventId(tx.id, outputIndex, transferIndex, transfer),
                        blockId = block.id,
                        blockNumber = block.number,
                        blockTimestamp = block.timestamp,
                        txId = tx.id,
                        origin = tx.origin,
                        params = parameters,
                        address = transfer.recipient,
                        eventType = "VET_TRANSFER",
                        clauseIndex = outputIndex.toLong(),
                    )
                indexedEvent to parameters
            } catch (ex: Exception) {
                logger.warn("Failed to process VET transfer: ${transfer.sender} -> ${transfer.recipient}", ex)
                null
            }
        }

    /**
     * Generates a unique ID for an event based on its transaction, output, and topic.
     */
    private fun generateEventId(
        txId: String,
        outputIndex: Int,
        eventIndex: Int,
        event: Any,
    ): String {
        val eventHash = event.hashCode() // Use hash of the event as a unique identifier
        return "$txId-$outputIndex-$eventIndex-$eventHash"
    }
}
