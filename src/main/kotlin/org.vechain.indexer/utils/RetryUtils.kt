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

/**
 * Like [retryOnFailure] but gives up after [maxAttempts] failed attempts and hands control to
 * [onGiveUp] instead of looping forever. Intended for non-idempotent operations (e.g. per-block
 * indexer processing against a non-transactional store) where a permanent failure must escape the
 * retry layer so a higher level can restore a clean state and restart.
 *
 * [CancellationException] and [ReorgException] still short-circuit the retry layer, exactly as in
 * [retryOnFailure]. [onGiveUp] returns [Nothing], so the caller decides how to escape (typically by
 * performing recovery side effects and throwing a domain-specific exception with the original as
 * cause).
 */
suspend fun <T> retryOnFailureBounded(
    maxAttempts: Int,
    onGiveUp: (Throwable) -> Nothing,
    initialDelayMs: Long = 1_000L,
    maxDelayMs: Long = 30_000L,
    multiplier: Double = 2.0,
    random: Random = Random.Default,
    operation: suspend () -> T,
): T {
    require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
    var currentDelay = initialDelayMs
    var attempts = 0
    while (true) {
        try {
            return operation()
        } catch (e: CancellationException) {
            throw e
        } catch (e: ReorgException) {
            logger.error("Reorg detected, propagating to restart indexers", e)
            throw e
        } catch (e: Exception) {
            attempts++
            if (attempts >= maxAttempts) onGiveUp(e)
            val jitter = random.nextLong(0, currentDelay)
            val totalDelay = currentDelay + jitter
            logger.error(
                "Operation failed (attempt $attempts/$maxAttempts), retrying in ${totalDelay}ms...",
                e,
            )
            delay(totalDelay)
            currentDelay = (currentDelay * multiplier).toLong().coerceAtMost(maxDelayMs)
        }
    }
}
