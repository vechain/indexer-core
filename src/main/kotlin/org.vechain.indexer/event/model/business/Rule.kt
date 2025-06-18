package org.vechain.indexer.event.model.business

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.vechain.indexer.event.model.enums.Operator

@JsonIgnoreProperties(ignoreUnknown = true)
data class Rule(
    val firstEventName: String = "",
    val firstEventProperty: String = "",
    val secondEventName: String = "",
    val secondEventProperty: String = "",
    val operator: Operator = Operator.EQ,
    val isNumber: Boolean = false,
)
