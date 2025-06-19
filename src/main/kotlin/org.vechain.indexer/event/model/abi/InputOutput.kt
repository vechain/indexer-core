package org.vechain.indexer.event.model.abi

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class InputOutput(
    val internalType: String? = null,
    val name: String = "",
    val type: String = "",
    val indexed: Boolean = false,
    val components: List<InputOutput>? = null,
)
