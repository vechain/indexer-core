package org.vechain.indexer.fixtures

import com.fasterxml.jackson.core.type.TypeReference
import org.vechain.indexer.thor.model.TransferLog
import org.vechain.indexer.utils.JsonUtils

object TransferLogFixtures {
    private val objectMapper = JsonUtils.mapper

    val LOGS_VET_TRANSFER = buildBlockFixture("logs/vet_transfers_block_logs.json")

    private fun buildBlockFixture(name: String): List<TransferLog> {
        val resource =
            TransferLogFixtures::class.java.classLoader.getResource(name)
                ?: throw IllegalStateException("Resource not found: $name")

        return resource.openStream().use { inputStream ->
            objectMapper.readValue(inputStream, object : TypeReference<List<TransferLog>>() {})
        }
    }
}
