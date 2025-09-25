package org.vechain.indexer.thor.client

import com.fasterxml.jackson.core.type.TypeReference
import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.result.Result
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.thor.model.*
import org.vechain.indexer.utils.JsonUtils

/**
 * Default implementation of the {@link org.vechain.indexer.thor.client.ThorClient.class ThorClient}
 * using the Fuel HTTP library and Jackson JSON mapper.
 *
 * @see <a href="https://github.com/kittinunf/fuel">Fuel Library</a>
 */
class DefaultThorClient(
    private val baseUrl: String,
    private vararg val headers: Pair<String, Any>,
) : ThorClient {
    private val objectMapper = JsonUtils.mapper

    override suspend fun getBlock(blockNumber: Long): Block =
        withContext(Dispatchers.IO) {
            val (_, _, result) =
                Fuel.get("$baseUrl/blocks/$blockNumber?expanded=true")
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure ->
                        throw Exception(
                            "Get block $blockNumber request failed with error: ${result.error}",
                        )
                }

            if (responseBody.isEmpty() || responseBody.trim() == "null") {
                throw BlockNotFoundException("Block $blockNumber not found")
            }

            return@withContext objectMapper.readValue(responseBody, Block::class.java)
        }

    override suspend fun getBestBlock(): Block =
        withContext(Dispatchers.IO) {
            val (_, _, result) =
                Fuel.get("$baseUrl/blocks/best?expanded=true").appendHeader(*headers).response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure ->
                        throw Exception("Get best block request failed with error: ${result.error}")
                }

            return@withContext objectMapper.readValue(responseBody, Block::class.java)
        }

    override suspend fun getFinalizedBlock(): Block =
        withContext(Dispatchers.IO) {
            val (_, _, result) =
                Fuel.get("$baseUrl/blocks/finalized?expanded=true")
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure ->
                        throw Exception(
                            "Get best finalized request failed with error: ${result.error}"
                        )
                }

            return@withContext objectMapper.readValue(responseBody, Block::class.java)
        }

    override suspend fun getEventLogs(req: EventLogsRequest): List<EventLog> =
        withContext(Dispatchers.IO) {
            val (_, _, result) =
                Fuel.post("$baseUrl/logs/event")
                    .body(JsonUtils.mapper.writeValueAsBytes(req))
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure ->
                        throw Exception("Get logs request failed with error: ${result.error}")
                }

            return@withContext objectMapper.readValue(
                responseBody,
                object : TypeReference<List<EventLog>>() {}
            )
        }

    override suspend fun getVetTransfers(req: TransferLogsRequest): List<TransferLog> =
        withContext(Dispatchers.IO) {
            val (_, _, result) =
                Fuel.post("$baseUrl/logs/transfer")
                    .body(JsonUtils.mapper.writeValueAsBytes(req))
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure ->
                        throw Exception(
                            "Get transfer logs request failed with error: ${result.error}"
                        )
                }

            return@withContext objectMapper.readValue(
                responseBody,
                object : TypeReference<List<TransferLog>>() {}
            )
        }

    override suspend fun inspectClauses(
        clauses: List<Clause>,
        blockID: String
    ): List<InspectionResult> {
        return withContext(Dispatchers.IO) {
            val req = InspectionRequest(clauses)
            val body = JsonUtils.mapper.writeValueAsBytes(req)
            val (_, _, result) =
                Fuel.post("$baseUrl/accounts/*?revision=$blockID")
                    .body(body)
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure ->
                        throw Exception("Inspect clauses request failed with error: ${result.error}")
                }

            return@withContext objectMapper.readValue(
                responseBody,
                object : TypeReference<List<InspectionResult>>() {}
            )
        }
    }
}
