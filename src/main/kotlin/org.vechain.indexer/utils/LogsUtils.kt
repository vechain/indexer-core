package org.vechain.indexer.utils

import org.vechain.indexer.thor.model.*

// TODO: Unit test all of this

object LogsUtils {
    /** Checks if an event log matches the given criteria. */
    fun eventMatchesCriteria(
        event: TxEvent,
        criteria: EventCriteria,
    ): Boolean =
        listOf(
                criteria.address == null || event.address == criteria.address,
                criteria.topic0 == null || event.topics.getOrNull(0) == criteria.topic0,
                criteria.topic1 == null || event.topics.getOrNull(1) == criteria.topic1,
                criteria.topic2 == null || event.topics.getOrNull(2) == criteria.topic2,
                criteria.topic3 == null || event.topics.getOrNull(3) == criteria.topic3,
                criteria.topic4 == null || event.topics.getOrNull(4) == criteria.topic4,
            )
            .all { it }

    /** Checks if a transfer log matches the given criteria. */
    fun transferMatchesCriteria(
        transfer: TxTransfer,
        criteria: TransferCriteria,
        tx: Transaction,
    ): Boolean {
        if (criteria.sender != null && transfer.sender != criteria.sender) return false
        if (criteria.recipient != null && transfer.recipient != criteria.recipient) return false
        if (criteria.txOrigin != null && tx.origin != criteria.txOrigin) return false
        return true
    }

    /**
     * Extracts and filters event logs from a transaction output.
     *
     * This function processes all event logs found in the given transaction output. If event
     * filtering criteria (`eventCriteriaSet`) are provided, only logs matching the criteria are
     * included.
     *
     * @param output The transaction output containing event logs.
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the output.
     * @return A list of event logs that match the defined criteria (or all logs if no criteria
     *   exist).
     */
    fun extractEventLogs(
        output: TxOutputs,
        block: Block,
        tx: Transaction,
        clauseIndex: Int,
        eventCriteriaSet: List<EventCriteria>? = null,
    ): List<EventLog> {
        val eventLogs = mutableListOf<EventLog>()

        output.events.forEach { event ->
            if (eventCriteriaSet.isNullOrEmpty()) {
                eventLogs.add(event.toEventLog(block, tx, clauseIndex))
            } else {
                if (eventCriteriaSet.any { eventMatchesCriteria(event, it) }) {
                    eventLogs.add(event.toEventLog(block, tx, clauseIndex))
                }
            }
        }
        return eventLogs
    }

    /**
     * Extracts and filters transfer logs from a transaction output.
     *
     * This function processes all token/VET transfers found in the transaction output. If filtering
     * criteria (`transferCriteriaSet`) are provided, only transfers matching the criteria are
     * included.
     *
     * @param output The transaction output containing transfer logs.
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the output.
     * @return A list of transfer logs that match the defined criteria (or all logs if no criteria
     *   exist).
     */
    fun extractTransferLogs(
        output: TxOutputs,
        block: Block,
        tx: Transaction,
        clauseIndex: Int,
        transferCriteriaSet: List<TransferCriteria>? = null,
    ): List<TransferLog> {
        val transferLogs = mutableListOf<TransferLog>()

        output.transfers.forEach { transfer ->
            if (transferCriteriaSet.isNullOrEmpty()) {
                transferLogs.add(transfer.toTransferLog(block, tx, clauseIndex))
            } else {
                if (transferCriteriaSet.any { transferMatchesCriteria(transfer, it, tx) }) {
                    transferLogs.add(transfer.toTransferLog(block, tx, clauseIndex))
                }
            }
        }
        return transferLogs
    }

    /**
     * Converts a transaction event into an EventLog.
     *
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the event.
     * @return The formatted EventLog.
     */
    private fun TxEvent.toEventLog(
        block: Block,
        tx: Transaction,
        clauseIndex: Int = 0,
    ): EventLog =
        EventLog(
            address = this.address,
            topics = this.topics,
            data = this.data,
            meta =
                EventMeta(
                    blockID = block.id,
                    blockNumber = block.number,
                    blockTimestamp = block.timestamp,
                    txID = tx.id,
                    txOrigin = tx.origin,
                    clauseIndex = clauseIndex,
                ),
        )

    /**
     * Converts a transaction transfer into a TransferLog.
     *
     * @param block The block containing the transaction.
     * @param tx The transaction associated with the transfer.
     * @return The formatted TransferLog.
     */
    private fun TxTransfer.toTransferLog(
        block: Block,
        tx: Transaction,
        clauseIndex: Int = 0,
    ): TransferLog =
        TransferLog(
            sender = this.sender,
            recipient = this.recipient,
            amount = this.amount,
            meta =
                EventMeta(
                    blockID = block.id,
                    blockNumber = block.number,
                    blockTimestamp = block.timestamp,
                    txID = tx.id,
                    txOrigin = tx.origin,
                    clauseIndex = clauseIndex,
                ),
        )
}
