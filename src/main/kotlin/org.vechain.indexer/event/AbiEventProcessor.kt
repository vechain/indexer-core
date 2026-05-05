package org.vechain.indexer.event

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.model.generic.RawEvent
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.event.utils.EventUtils.generateEventId
import org.vechain.indexer.event.utils.IndexedEventOrder
import org.vechain.indexer.thor.model.*
import org.vechain.indexer.utils.DataUtils

open class AbiEventProcessor(
    basePath: String?,
    eventNames: List<String>,
    private val contractAddresses: List<String>,
    private val includeVetTransfers: Boolean
) : EventProcessor {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    val eventAbis: List<AbiElement> =
        if (basePath != null) {
            AbiLoader.loadEvents(basePath = basePath, eventNames = eventNames)
        } else {
            if (eventNames.isNotEmpty()) {
                throw IllegalArgumentException(
                    "ABI base path must be provided when abiEventNames is not empty."
                )
            }
            emptyList()
        }

    override fun processEvents(block: Block): List<IndexedEvent> {
        val events = mutableListOf<IndexedEvent>()
        var eventLogIndex = 0L
        var transferLogIndex = 0L

        block.transactions.forEachIndexed { txIndex, tx ->
            tx.outputs.forEachIndexed { outputIndex, output ->
                output.events.forEachIndexed { eventIndex, event ->
                    val logIndex = eventLogIndex++
                    if (EventUtils.isEventValid(event, eventAbis, contractAddresses)) {
                        decodeEvent(
                                event = event,
                                tx = tx,
                                block = block,
                                outputIndex = outputIndex,
                                eventIndex = eventIndex,
                                txIndex = txIndex,
                                logIndex = logIndex,
                            )
                            ?.let { events.add(it) }
                    }
                }
                if (includeVetTransfers) {
                    events.addAll(
                        extractVetTransfers(
                            output = output,
                            tx = tx,
                            block = block,
                            outputIndex = outputIndex,
                            txIndex = txIndex,
                            firstLogIndex = transferLogIndex,
                        )
                    )
                    transferLogIndex += output.transfers.size
                }
            }
        }
        return events
    }

    override fun processEvents(
        eventLogs: List<EventLog>,
        transferLogs: List<TransferLog>
    ): List<IndexedEvent> {
        val abiEvents = decodeLogEvents(eventLogs)
        val vetTransfers =
            if (includeVetTransfers) decodeLogTransfers(transferLogs) else emptyList()
        return IndexedEventOrder.sortChronologically(abiEvents + vetTransfers)
    }

    /** Decodes a single transaction event into an IndexedEvent. */
    protected fun decodeEvent(
        event: TxEvent,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
        eventIndex: Int,
        txIndex: Int? = null,
        logIndex: Long? = null,
    ): IndexedEvent? {
        val matchingAbi = EventUtils.findMatchingAbi(event.topics, eventAbis)
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
                    txIndex = txIndex?.toLong(),
                    logIndex = logIndex,
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
    protected fun decodeLogEvents(logs: List<EventLog>): List<IndexedEvent> {
        if (logs.isEmpty()) return emptyList()

        return logs.mapIndexedNotNull { i, log ->
            val txEvent = TxEvent(log.address, log.topics, log.data)

            // Check if the event is valid and has a matching ABI
            if (!EventUtils.isEventValid(txEvent, eventAbis, contractAddresses))
                return@mapIndexedNotNull null
            val matchingAbi =
                EventUtils.findMatchingAbi(log.topics, eventAbis) ?: return@mapIndexedNotNull null

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
                    txIndex = log.meta.txIndex,
                    logIndex = log.meta.logIndex,
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
    protected fun decodeLogTransfers(logs: List<TransferLog>): List<IndexedEvent> {
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
                    txIndex = log.meta.txIndex,
                    logIndex = log.meta.logIndex,
                )
            } catch (ex: Exception) {
                logger.warn("Failed to process VET transfer: ${log.sender} -> ${log.recipient}", ex)
                null
            }
        }
    }

    /** Extracts VET transfers from a transaction output and returns them as events. */
    protected fun extractVetTransfers(
        output: TxOutputs,
        tx: Transaction,
        block: Block,
        outputIndex: Int,
        txIndex: Int? = null,
        firstLogIndex: Long = 0L,
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
                        txIndex = txIndex?.toLong(),
                        logIndex = firstLogIndex + transferIndex,
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
}
