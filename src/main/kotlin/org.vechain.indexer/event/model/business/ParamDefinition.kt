package org.vechain.indexer.event.model.business

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class ParamDefinition(
    val name: String = "",
    val eventName: String = "",
    val businessEventName: String = "",
)
