package org.vechain.indexer.thor.model

data class Transaction(
    val id: String,
    val type: Long?,
    val chainTag: Long,
    val blockRef: String,
    val expiration: Long,
    val clauses: List<Clause>,
    val gasPriceCoef: Long? = null,
    val gas: Long,
    val maxFeePerGas: String? = null,
    val maxPriorityFeePerGas: String? = null,
    val origin: String,
    val delegator: String? = null,
    val nonce: String,
    val dependsOn: String? = null,
    val size: Long,
    val gasUsed: Long,
    val gasPayer: String,
    val paid: String,
    val reward: String,
    val reverted: Boolean,
    val outputs: List<TxOutputs>
)
