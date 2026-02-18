package org.vechain.indexer.thor.model

data class BlockIdentifier(
    val number: Long,
    // Sometimes we only have access to the block number so this field is optional
    val id: String? = null,
)
