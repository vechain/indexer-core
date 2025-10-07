package org.vechain.indexer.fixtures

import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.Transaction
import org.vechain.indexer.utils.JsonUtils

object BlockFixtures {
    private val objectMapper = JsonUtils.mapper

    val BLOCK_STRINGS = buildBlockFixture("blocks/string_event_block.json")
    val BLOCK_TRANSFERS = buildBlockFixture("blocks/transfers.json")
    val BLOCK_B3TR_ACTION = buildBlockFixture("blocks/b3tr_action_block.json")
    val BLOCK_TOKEN_EXCHANGE = buildBlockFixture("blocks/token_exchange_block.json")
    val BLOCK_WITH_INDEXED_ARRAY = buildBlockFixture("blocks/indexed_array_block.json")
    val BLOCK_DYNAMIC_FEES = buildBlockFixture("blocks/dynamic_fees_block.json")
    val BLOCK_STARGATE_BASE_REWARD = buildBlockFixture("blocks/stargate_claim_base_rewards.json")
    val BLOCK_STARGATE_STAKE = buildBlockFixture("blocks/stargate_stake.json")
    val BLOCK_STARGATE_UNSTAKE = buildBlockFixture("blocks/stargate_unstake.json")
    val BLOCK_STARGATE_STAKE_AND_DELEGATE =
        buildBlockFixture("blocks/stargate_stake_and_delegate.json")
    val BLOCK_NFT_TRANSFER = buildBlockFixture("blocks/nft_transfer.json")
    val BLOCK_STARGATE_STAKE_DELEGATE = buildBlockFixture("blocks/stargate_stake_delegate.json")
    val BLOCK_STARGATE_VTHO_REFUND = buildBlockFixture("blocks/stargate_vtho_refund.json")

    private fun buildBlockFixture(name: String): Block {
        val resource =
            BlockFixtures::class.java.classLoader.getResource(name)
                ?: throw IllegalStateException("Resource not found: $name")

        return resource.openStream().use { inputStream ->
            objectMapper.readValue(inputStream, Block::class.java)
        }
    }

    fun testBlock(number: Long): Block =
        Block(
            number = number,
            id = "block-$number",
            size = 1,
            parentID = "parent-${number - 1}",
            timestamp = number,
            gasLimit = 1,
            baseFeePerGas = null,
            beneficiary = "beneficiary",
            gasUsed = 1,
            totalScore = 1,
            txsRoot = "root",
            txsFeatures = 0,
            stateRoot = "state",
            receiptsRoot = "receipts",
            com = false,
            signer = "signer",
            isTrunk = true,
            isFinalized = true,
            transactions = emptyList<Transaction>(),
        )
}
