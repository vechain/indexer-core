package org.vechain.indexer.fixtures

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.utils.JsonUtils

object EventLogFixtures {
    private val objectMapper = JsonUtils.mapper

    val LOGS_STRINGS = buildBlockFixture("logs/string_event_block_logs.json")
    val LOGS_B3TR_ACTION = buildBlockFixture("logs/b3tr_actions_block_logs.json")
    val LOGS_TOKEN_EXCHANGE = buildBlockFixture("logs/token_exchange_block_logs.json")
    val LOGS_NFT_MINT = buildBlockFixture("logs/nft_mint.json")
    val LOGS_BATCH_TRANSFER = buildBlockFixture("logs/batch_transfers.json")
    val LOGS_MULTIPLE_EVENTS = buildBlockFixture("logs/multiple_txs.json")

    private fun buildBlockFixture(name: String): List<EventLog> {
        val resource =
            EventLogFixtures::class.java.classLoader.getResource(name)
                ?: throw IllegalStateException("Resource not found: $name")

        return resource.openStream().use { inputStream ->
            objectMapper.readValue(inputStream, object : TypeReference<List<EventLog>>() {})
        }
    }
}
