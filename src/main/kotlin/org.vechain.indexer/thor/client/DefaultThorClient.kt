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

    override suspend fun getBlock(revision: BlockRevision): Block =
        fetchBlock(revision = revision, expanded = true, targetClass = Block::class.java)

    override suspend fun getBlockUnexpanded(revision: BlockRevision): BlockUnexpanded =
        fetchBlock(revision = revision, expanded = false, targetClass = BlockUnexpanded::class.java)

    override suspend fun waitForBlock(revision: BlockRevision): Block =
        waitForBlockInternal(revision) { getBlock(revision) }

    override suspend fun waitForBlockUnexpanded(revision: BlockRevision): BlockUnexpanded =
        waitForBlockInternal(revision) { getBlockUnexpanded(revision) }

    private suspend fun <T> fetchBlock(
        revision: BlockRevision,
        expanded: Boolean,
        targetClass: Class<T>,
    ): T =
        withContext(Dispatchers.IO) {
            val params = listOf("expanded" to expanded)
            val (_, response, result) =
                Fuel.get(path = "$baseUrl/blocks/${revision.value}", parameters = params)
                    .appendHeader(*headers)
                    .response()

            if (response.statusCode == HTTP_TOO_MANY_REQUESTS) {
                throw RateLimitException("Rate limited fetching block ${revision.value}")
            }

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure -> throw result.error
                }

            if (responseBody.isEmpty() || responseBody.trim() == "null") {
                throw BlockNotFoundException("Block ${revision.value} not found")
            }

            return@withContext objectMapper.readValue(responseBody, targetClass)
        }

    private suspend fun <T> waitForBlockInternal(
        revision: BlockRevision,
        fetch: suspend () -> T,
    ): T {
        val startTime = System.currentTimeMillis()
        var delayMs = TIP_POLL_INITIAL_DELAY_MS
        var attempts = 0
        while (true) {
            attempts++
            try {
                val block = fetch()
                val totalTime = System.currentTimeMillis() - startTime
                if (attempts > 1) {
                    logger.info(
                        "Block {} fetched after {} attempts, total wait: {}ms",
                        revision.value,
                        attempts,
                        totalTime
                    )
                }
                return block
            } catch (e: BlockNotFoundException) {
                logger.info(
                    "Block {} not yet available, waiting {}ms (attempt {})",
                    revision.value,
                    delayMs,
                    attempts
                )
                delay(delayMs)
                delayMs = (delayMs - TIP_POLL_DELAY_STEP_MS).coerceAtLeast(TIP_POLL_MIN_DELAY_MS)
            } catch (e: RateLimitException) {
                logger.warn(
                    "Rate limited on block {}, backing off {}ms (attempt {})",
                    revision.value,
                    RATE_LIMIT_DELAY_MS,
                    attempts
                )
                delay(RATE_LIMIT_DELAY_MS)
            } catch (e: Exception) {
                logger.warn(
                    "Error fetching block {} (attempt {}), retrying in {}ms...",
                    revision.value,
                    attempts,
                    TIP_POLL_ERROR_DELAY_MS,
                    e
                )
                delay(TIP_POLL_ERROR_DELAY_MS)
            }
        }
    }

    override suspend fun getEventLogs(req: EventLogsRequest): List<EventLog> =
        withContext(Dispatchers.IO) {
            val (_, response, result) =
                Fuel.post("$baseUrl/logs/event")
                    .body(JsonUtils.mapper.writeValueAsBytes(req))
                    .appendHeader(*headers)
                    .response()

            if (response.statusCode == HTTP_TOO_MANY_REQUESTS) {
                throw RateLimitException("Rate limited fetching event logs")
            }

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
            val (_, response, result) =
                Fuel.post("$baseUrl/logs/transfer")
                    .body(JsonUtils.mapper.writeValueAsBytes(req))
                    .appendHeader(*headers)
                    .response()

            if (response.statusCode == HTTP_TOO_MANY_REQUESTS) {
                throw RateLimitException("Rate limited fetching VET transfers")
            }

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
        revision: BlockRevision?
    ): List<InspectionResult> {
        return withContext(Dispatchers.IO) {
            val req = InspectionRequest(clauses)
            val body = JsonUtils.mapper.writeValueAsBytes(req)
            val params = if (revision != null) listOf("revision" to revision.value) else emptyList()
            val (_, response, result) =
                Fuel.post(path = "$baseUrl/accounts/*", parameters = params)
                    .body(body)
                    .appendHeader(*headers)
                    .response()

            if (response.statusCode == HTTP_TOO_MANY_REQUESTS) {
                throw RateLimitException("Rate limited inspecting clauses")
            }

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

    override suspend fun getAccountState(
        address: String,
        revision: BlockRevision?
    ): ExecuteAccountResponse {
        return withContext(Dispatchers.IO) {
            val params = if (revision != null) listOf("revision" to revision.value) else emptyList()
            val (_, _, result) =
                Fuel.get(path = "$baseUrl/accounts/$address", parameters = params)
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure -> throw result.error
                }

            return@withContext objectMapper.readValue(
                responseBody,
                ExecuteAccountResponse::class.java
            )
        }
    }

    override suspend fun getAccountCode(
        address: String,
        revision: BlockRevision?
    ): AccountCodeResponse {
        return withContext(Dispatchers.IO) {
            val params = if (revision != null) listOf("revision" to revision.value) else emptyList()
            val (_, _, result) =
                Fuel.get(path = "$baseUrl/accounts/$address/code", parameters = params)
                    .appendHeader(*headers)
                    .response()

            val responseBody =
                when (result) {
                    is Result.Success -> result.get().toString(Charsets.UTF_8)
                    is Result.Failure -> throw result.error
                }

            return@withContext objectMapper.readValue(responseBody, AccountCodeResponse::class.java)
        }
    }
}
