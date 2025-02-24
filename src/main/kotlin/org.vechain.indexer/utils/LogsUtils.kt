package org.vechain.indexer.utils

import org.vechain.indexer.thor.model.*

/**
 * Checks if an event log matches the given criteria.
 */
fun matchesEventCriteria(
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
    ).all { it }

/**
 * Checks if a transfer log matches the given criteria.
 */
fun matchesTransferCriteria(
    transfer: TxTransfer,
    criteria: TransferCriteria,
    tx: Transaction,
): Boolean =
    listOf(
        criteria.sender == null || transfer.sender == criteria.sender,
        criteria.recipient == null || transfer.recipient == criteria.recipient,
        criteria.txOrigin == null || tx.origin == criteria.txOrigin,
    ).all { it }
