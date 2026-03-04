package org.vechain.indexer.utils

import kotlin.random.Random
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import org.vechain.indexer.exception.ReorgException

private val logger = LoggerFactory.getLogger("org.vechain.indexer.utils.RetryUtils")

suspend fun <T> retryOnFailure(
    initialDelayMs: Long = 1_000L,
    maxDelayMs: Long = 30_000L,
    multiplier: Double = 2.0,
    random: Random = Random.Default,
    operation: suspend () -> T,
): T {
    var currentDelay = initialDelayMs
    while (true) {
        try {
            return operation()
        } catch (e: CancellationException) {
            throw e
        } catch (e: ReorgException) {
            logger.error("Reorg detected, propagating to restart indexers", e)
            throw e
        } catch (e: Exception) {
            val jitter = random.nextLong(0, currentDelay)
            val totalDelay = currentDelay + jitter
            logger.error("Operation failed, retrying in ${totalDelay}ms...", e)
            delay(totalDelay)
            currentDelay = (currentDelay * multiplier).toLong().coerceAtMost(maxDelayMs)
        }
    }
}
