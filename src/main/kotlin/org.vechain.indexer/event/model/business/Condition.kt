package org.vechain.indexer.event.model.business

import org.vechain.indexer.event.model.enums.Operator

data class Condition(
    val firstOperand: String,
    val isFirstStatic: Boolean = false,
    val secondOperand: String,
    val isSecondStatic: Boolean = false,
    val operator: Operator,
    val isNumber: Boolean = false,
)
