package org.vechain.indexer.event.model.business

data class BusinessEventDefinition(
    val name: String,
    val sameClause: Boolean? = null,
    val checkAllCombinations: Boolean? = null,
    val maxAttempts: Int? = 10,
    val events: List<Event>,
    val rules: List<Rule>,
    val paramsDefinition: List<ParamDefinition>,
)
