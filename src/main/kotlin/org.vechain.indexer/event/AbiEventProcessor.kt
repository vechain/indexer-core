package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.model.generic.RawEvent
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.model.*
import org.vechain.indexer.utils.DataUtils

class AbiEventProcessor(
    private val abiManager: AbiManager,
    private val contractAddresses: List<String>,
    private val includeVetTransfers: Boolean
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /** Extracts and decodes all events in a block based on loaded ABIs. */
    fun getEvents(block: Block): List<IndexedEvent> {
        val events = mutableListOf<IndexedEvent>()

        block.transactions.forEach { tx ->
            tx.outputs.forEachIndexed { outputIndex, output ->
                output.events.forEachIndexed { eventIndex, event ->
                    if (EventUtils.isEventValid(event, abiManager.eventAbis, contractAddresses)) {
                        decodeEvent(event, tx, block, outputIndex, eventIndex)?.let {
                            events.add(it)
                        }
                    }
                }
                if (includeVetTransfers) {
                    events.addAll(extractVetTransfers(output, tx, block, outputIndex))
                }
            }
        }
        return events
    }

    /** Decodes an event and returns a pair of IndexedEvent and its parameters if successful. */
    private fun decodeEvent(
        event: TxEvent,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
        eventIndex: Int,
    ): IndexedEvent? {
        val matchingAbi = EventUtils.findMatchingAbi(event.topics, abiManager.eventAbis)
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
                    gasPayer = tx.gasPayer,
                    gasUsed = tx.gasUsed,
                    paid = tx.paid,
                    raw = RawEvent(event.data, event.topics),
                    params = parameters,
                    address = event.address,
                    eventType = parameters.getEventType(),
                    clauseIndex = outputIndex.toLong(),
                    signature = event.topics[0],
                )
            } catch (_: IllegalArgumentException) {
                logger.warn(
                    "Failed to decode event with ABI: ${abi.name}, txId: ${tx.id}. Skipping event."
                )
                null
            }
        }
    }

    /**
     * Decodes a batch of log events into IndexedEvents.
     *
     * @param logs List of raw EventLogs.
     * @return A list of decoded Indexed Events with their parameters.
     */
    fun decodeLogEvents(logs: List<EventLog>): List<IndexedEvent> {
        if (logs.isEmpty()) return emptyList()

        return logs.mapIndexedNotNull { i, log ->
            val txEvent = TxEvent(log.address, log.topics, log.data)

            // Check if the event is valid and has a matching ABI
            if (!EventUtils.isEventValid(txEvent, abiManager.eventAbis, contractAddresses))
                return@mapIndexedNotNull null
            val matchingAbi =
                EventUtils.findMatchingAbi(log.topics, abiManager.eventAbis)
                    ?: return@mapIndexedNotNull null

            try {
                val parameters = EventUtils.decodeEvent(txEvent, matchingAbi)

                IndexedEvent(
                    id = generateEventId(log.meta.txID, log.meta.clauseIndex, i, txEvent),
                    blockId = log.meta.blockID,
                    blockNumber = log.meta.blockNumber,
                    blockTimestamp = log.meta.blockTimestamp,
                    txId = log.meta.txID,
                    origin = log.meta.txOrigin,
                    raw = RawEvent(log.data, log.topics),
                    params = parameters,
                    address = log.address,
                    eventType = parameters.getEventType(),
                    clauseIndex = log.meta.clauseIndex.toLong(),
                    signature = log.topics[0],
                )
            } catch (_: Exception) {
                logger.warn(
                    "Failed to decode log event with ABI: ${matchingAbi.name}, txId: ${log.meta.txID}"
                )
                null
            }
        }
    }

    /**
     * Decodes a batch of log transfers into IndexedEvents.
     *
     * @param logs List of raw TransferLogs.
     * @return A list of decoded Indexed Transfers with their senders and receivers.
     */
    fun decodeLogTransfers(logs: List<TransferLog>): List<IndexedEvent> {
        if (logs.isEmpty()) return emptyList()

        return logs.mapIndexedNotNull { i, log ->
            try {
                val parameters =
                    AbiEventParameters(
                        mapOf(
                            "from" to log.sender,
                            "to" to log.recipient,
                            "amount" to DataUtils.decodeQuantity(log.amount),
                        ),
                        "VET_TRANSFER",
                    )

                IndexedEvent(
                    id = generateEventId(log.meta.txID, log.meta.clauseIndex, i, parameters),
                    blockId = log.meta.blockID,
                    blockNumber = log.meta.blockNumber,
                    blockTimestamp = log.meta.blockTimestamp,
                    txId = log.meta.txID,
                    origin = log.meta.txOrigin,
                    params = parameters,
                    eventType = "VET_TRANSFER",
                    clauseIndex = log.meta.clauseIndex.toLong(),
                )
            } catch (ex: Exception) {
                logger.warn("Failed to process VET transfer: ${log.sender} -> ${log.recipient}", ex)
                null
            }
        }
    }

    /** Extracts VET transfers from a transaction output and returns them as events. */
    private fun extractVetTransfers(
        output: TxOutputs,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
    ): List<IndexedEvent> =
        output.transfers.mapIndexedNotNull { transferIndex, transfer ->
            try {
                val parameters =
                    AbiEventParameters(
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
                        paid = tx.paid,
                        gasUsed = tx.gasUsed,
                        gasPayer = tx.gasPayer,
                        params = parameters,
                        eventType = "VET_TRANSFER",
                        clauseIndex = outputIndex.toLong(),
                    )
                indexedEvent
            } catch (ex: Exception) {
                logger.warn(
                    "Failed to process VET transfer: ${transfer.sender} -> ${transfer.recipient}",
                    ex
                )
                null
            }
        }

    /** Generates a unique ID for an event based on its transaction, output, and topic. */
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
