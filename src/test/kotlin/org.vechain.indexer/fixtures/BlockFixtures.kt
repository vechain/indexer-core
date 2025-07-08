package org.vechain.indexer.fixtures

import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.utils.JsonUtils

object BlockFixtures {
    private val objectMapper = JsonUtils.mapper

    val BLOCK_STRINGS = buildBlockFixture("blocks/string_event_block.json")
    val BLOCK_B3TR_ACTION = buildBlockFixture("blocks/b3tr_action_block.json")
    val BLOCK_TOKEN_EXCHANGE = buildBlockFixture("blocks/token_exchange_block.json")
    val BLOCK_WITH_INDEXED_ARRAY = buildBlockFixture("blocks/indexed_array_block.json")
    val BLOCK_DYNAMIC_FEES = buildBlockFixture("blocks/dynamic_fees_block.json")
    val BLOCK_STARGATE_BASE_REWARD = buildBlockFixture("blocks/stargate_claim_base_rewards.json")

    private fun buildBlockFixture(name: String): Block {
        val resource =
            BlockFixtures::class.java.classLoader.getResource(name)
                ?: throw IllegalStateException("Resource not found: $name")

        return resource.openStream().use { inputStream ->
            objectMapper.readValue(inputStream, Block::class.java)
        }
    }
}
