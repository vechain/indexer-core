package org.vechain.indexer.event.model.business

import org.vechain.indexer.event.model.enums.Operator

data class Rule(
    val firstEventName: String,
    val firstEventProperty: String,
    val secondEventName: String,
    val secondEventProperty: String,
    val operator: Operator,
    val isNumber: Boolean = false,
)
