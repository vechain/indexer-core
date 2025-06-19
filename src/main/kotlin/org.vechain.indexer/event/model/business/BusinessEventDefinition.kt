package org.vechain.indexer.event.model.business

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class BusinessEventDefinition(
    val name: String = "",
    val sameClause: Boolean? = null,
    val checkAllCombinations: Boolean? = null,
    val maxAttempts: Int? = 10,
    val events: List<Event> = emptyList(),
    val rules: List<Rule> = emptyList(),
    val paramsDefinition: List<ParamDefinition> = emptyList(),
)
