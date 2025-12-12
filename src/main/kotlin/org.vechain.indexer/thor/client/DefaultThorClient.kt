package org.vechain.indexer.thor.client

import com.fasterxml.jackson.core.type.TypeReference
import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.result.Result
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.exception.RateLimitException
import org.vechain.indexer.thor.model.*
import org.vechain.indexer.utils.JsonUtils

private const val TIP_POLL_MIN_DELAY_MS = 1_000L
private const val TIP_POLL_INITIAL_DELAY_MS = 4_000L
private const val TIP_POLL_DELAY_STEP_MS = 500L
private const val TIP_POLL_ERROR_DELAY_MS = 10_000L
private const val RATE_LIMIT_DELAY_MS = 30_000L
private const val HTTP_TOO_MANY_REQUESTS = 429

/**
 * Default implementation of the {@link org.vechain.indexer.thor.client.ThorClient.class ThorClient}
 * using the Fuel HTTP library and Jackson JSON mapper.
 *
 * @see <a href="https://github.com/kittinunf/fuel">Fuel Library</a>
 */
open class DefaultThorClient(
    private val baseUrl: String,
    private vararg val headers: Pair<String, Any>,
) : ThorClient {

    private val logger = LoggerFactory.getLogger(DefaultThorClient::class.java)
    private val objectMapper = JsonUtils.mapper

    override suspend fun getBlock(blockNumber: Long): Block =
        withContext(Dispatchers.IO) {
            val (_, response, result) =
                Fuel.get("$baseUrl/blocks/$blockNumber?expanded=true")
                    .appendHeader(*headers)
                    .response()

            if (response.statusCode == HTTP_TOO_MANY_REQUESTS) {
                throw RateLimitException("Rate limited fetching block $blockNumber")
            }

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure -> throw result.error
                }

            if (responseBody.isEmpty() || responseBody.trim() == "null") {
                throw BlockNotFoundException("Block $blockNumber not found")
            }

            return@withContext objectMapper.readValue(responseBody, Block::class.java)
        }

    override suspend fun waitForBlock(blockNumber: Long): Block {
        val startTime = System.currentTimeMillis()
        var delayMs = TIP_POLL_INITIAL_DELAY_MS
        var attempts = 0
        while (true) {
            attempts++
            try {
                val block = getBlock(blockNumber)
                val totalTime = System.currentTimeMillis() - startTime
                if (attempts > 1) {
                    logger.info(
                        "Block {} fetched after {} attempts, total wait: {}ms",
                        blockNumber,
                        attempts,
                        totalTime
                    )
                }
                return block
            } catch (e: BlockNotFoundException) {
                logger.info(
                    "Block {} not yet available, waiting {}ms (attempt {})",
                    blockNumber,
                    delayMs,
                    attempts
                )
                delay(delayMs)
                delayMs = (delayMs - TIP_POLL_DELAY_STEP_MS).coerceAtLeast(TIP_POLL_MIN_DELAY_MS)
            } catch (e: RateLimitException) {
                logger.warn(
                    "Rate limited on block {}, backing off {}ms (attempt {})",
                    blockNumber,
                    RATE_LIMIT_DELAY_MS,
                    attempts
                )
                delay(RATE_LIMIT_DELAY_MS)
            } catch (e: Exception) {
                logger.warn(
                    "Error fetching block {} (attempt {}), retrying in {}ms...",
                    blockNumber,
                    attempts,
                    TIP_POLL_ERROR_DELAY_MS,
                    e
                )
                delay(TIP_POLL_ERROR_DELAY_MS)
            }
        }
    }

    override suspend fun getBestBlock(): Block =
        withContext(Dispatchers.IO) {
            val (_, _, result) =
                Fuel.get("$baseUrl/blocks/best?expanded=true").appendHeader(*headers).response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure -> throw result.error
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
                    is Result.Failure -> throw result.error
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
                    is Result.Failure -> throw result.error
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
                    is Result.Failure -> throw result.error
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
                    is Result.Failure -> throw result.error
                }

            return@withContext objectMapper.readValue(
                responseBody,
                object : TypeReference<List<InspectionResult>>() {}
            )
        }
    }
}
