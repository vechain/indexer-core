package org.vechain.indexer.thor.client

import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.fuel.core.FuelError
import com.github.kittinunf.result.Result
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.utils.JsonUtils
import kotlin.math.min

/**
 * Default implementation of the {@link org.vechain.indexer.thor.client.ThorClient.class ThorClient}
 * using the Fuel HTTP library and Jackson JSON mapper.
 *
 * @see <a href="https://github.com/kittinunf/fuel">Fuel Library</a>
 */
class DefaultThorClient(
    private val baseUrl: String,
    private vararg val headers: Pair<String, Any>,
    private val maxRetries: Int = 5,
    private val initialDelay: Long = 1000L,
    private val maxDelay: Long = 16000L
) : ThorClient {

    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val objectMapper = JsonUtils.mapper

    override suspend fun getBlock(blockNumber: Long): Block =
        executeWithRetry { fetchBlock("$baseUrl/blocks/$blockNumber?expanded=true") }

    override suspend fun getBestBlock(): Block =
        executeWithRetry { fetchBlock("$baseUrl/blocks/best?expanded=true") }

    internal fun fetchBlock(url: String): Block {
        val (_, _, result) = Fuel.get(url).appendHeader(*headers).response()
        val responseBody = when (result) {
            is Result.Success -> result.get().toString(Charsets.UTF_8)
            is Result.Failure -> throw Exception("Request failed with error: ${result.error}")
            else -> null
        }

        if (responseBody.isNullOrEmpty() || responseBody.trim() == "null") {
            throw BlockNotFoundException("Block not found")
        }

        return objectMapper.readValue(responseBody, Block::class.java)
    }

    /**
     * Executes a suspendable block of code with retry logic for handling transient errors,
     * such as HTTP 429 (Too Many Requests). Implements exponential backoff between retries.
     *
     * @param block The suspendable block to execute.
     * @param T The return type of the block.
     * @return The result of the block if execution succeeds.
     * @throws Exception If retries are exhausted or a non-retryable error occurs.
     */
    private suspend fun <T> executeWithRetry(block: suspend () -> T): T {
        var remainingRetries = maxRetries
        var delayTime = initialDelay

        while (remainingRetries > 0) {
            try {
                return block()
            } catch (e: FuelError) {
                if (e.response.statusCode == 429) {
                   logger.warn("HTTP 429 received. Retrying... Remaining retries: $remainingRetries")
                    delay(delayTime)
                    delayTime = min(delayTime * 2, maxDelay)
                    remainingRetries--
                } else {
                    throw e // Non-429 errors are not retried
                }
            } catch (e: Exception) {
                throw e // Any other exceptions should propagate
            }
        }

        logger.error("Max retries exhausted due to HTTP 429. Throwing exception.")
        throw Exception("Max retries exhausted due to HTTP 429")
    }
}