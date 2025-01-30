package org.vechain.indexer.event.model.business

data class BusinessEventDefinition(
    val name: String,
    val sameClause: Boolean? = null,
    val events: List<Event>,
    val rules: List<Rule>,
    val paramsDefinition: List<ParamDefinition>,
)
