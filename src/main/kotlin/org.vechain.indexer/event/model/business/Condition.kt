package org.vechain.indexer.event.model.business

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.vechain.indexer.event.model.enums.Operator

@JsonIgnoreProperties(ignoreUnknown = true)
data class Condition(
    val firstOperand: String = "",
    val isFirstStatic: Boolean = false,
    val secondOperand: String = "",
    val isSecondStatic: Boolean = false,
    val operator: Operator = Operator.EQ,
    val isNumber: Boolean = false,
)
