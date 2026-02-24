package org.vechain.indexer.utils

import kotlin.random.Random
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.exception.ReorgException
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo
import strikt.assertions.isLessThan

@OptIn(ExperimentalCoroutinesApi::class)
internal class RetryUtilsTest {

    @Nested
    inner class SuccessCases {

        @Test
        fun `returns result on first success`() = runTest {
            val result = retryOnFailure { 42 }

            expectThat(result).isEqualTo(42)
        }

        @Test
        fun `returns result after transient failures`() = runTest {
            var attempts = 0
            val result = retryOnFailure {
                attempts++
                if (attempts < 3) throw RuntimeException("transient")
                "success"
            }

            expectThat(result).isEqualTo("success")
            expectThat(attempts).isEqualTo(3)
        }
    }

    @Nested
    inner class ExceptionPropagation {

        @Test
        fun `propagates CancellationException immediately`() = runTest {
            var attempts = 0
            assertThrows<CancellationException> {
                retryOnFailure {
                    attempts++
                    throw CancellationException("cancelled")
                }
            }

            expectThat(attempts).isEqualTo(1)
        }

        @Test
        fun `propagates ReorgException immediately`() = runTest {
            var attempts = 0
            assertThrows<ReorgException> {
                retryOnFailure {
                    attempts++
                    throw ReorgException("reorg")
                }
            }

            expectThat(attempts).isEqualTo(1)
        }
    }

    @Nested
    inner class BackoffBehavior {

        @Test
        fun `delay increases between retries`() = runTest {
            val random = Random(42)
            val delays = mutableListOf<Long>()
            var attempts = 0
            val startTimes = mutableListOf<Long>()

            val result =
                retryOnFailure(initialDelayMs = 1_000L, multiplier = 2.0, random = random) {
                    startTimes.add(testScheduler.currentTime)
                    attempts++
                    if (attempts <= 3) throw RuntimeException("fail")
                    "success"
                }

            expectThat(result).isEqualTo("success")
            expectThat(attempts).isEqualTo(4)

            for (i in 1 until startTimes.size) {
                delays.add(startTimes[i] - startTimes[i - 1])
            }

            // Each delay >= base (base + jitter where jitter >= 0)
            expectThat(delays[0]).isGreaterThanOrEqualTo(1_000L)
            expectThat(delays[1]).isGreaterThanOrEqualTo(2_000L)
            expectThat(delays[2]).isGreaterThanOrEqualTo(4_000L)
            // Each delay < 2x base (jitter < base)
            expectThat(delays[0]).isLessThan(2_000L)
            expectThat(delays[1]).isLessThan(4_000L)
            expectThat(delays[2]).isLessThan(8_000L)
        }

        @Test
        fun `delay capped at maxDelayMs`() = runTest {
            val random = Random(42)
            var attempts = 0
            val startTimes = mutableListOf<Long>()

            retryOnFailure(
                initialDelayMs = 1_000L,
                maxDelayMs = 2_000L,
                multiplier = 10.0,
                random = random,
            ) {
                startTimes.add(testScheduler.currentTime)
                attempts++
                if (attempts <= 3) throw RuntimeException("fail")
            }

            // First retry: base=1000 (uncapped), delay in [1000, 2000)
            val firstDelay = startTimes[1] - startTimes[0]
            expectThat(firstDelay).isGreaterThanOrEqualTo(1_000L)
            expectThat(firstDelay).isLessThan(2_000L)

            // Second retry: base=min(1000*10, 2000)=2000 (capped), delay in [2000, 4000)
            val secondDelay = startTimes[2] - startTimes[1]
            expectThat(secondDelay).isGreaterThanOrEqualTo(2_000L)
            expectThat(secondDelay).isLessThan(4_000L)

            // Third retry: base still capped at 2000, delay in [2000, 4000)
            val thirdDelay = startTimes[3] - startTimes[2]
            expectThat(thirdDelay).isGreaterThanOrEqualTo(2_000L)
            expectThat(thirdDelay).isLessThan(4_000L)
        }

        @Test
        fun `custom parameters work`() = runTest {
            val random = Random(42)
            var attempts = 0
            val startTimes = mutableListOf<Long>()

            retryOnFailure(
                initialDelayMs = 500L,
                maxDelayMs = 1_000L,
                multiplier = 3.0,
                random = random,
            ) {
                startTimes.add(testScheduler.currentTime)
                attempts++
                if (attempts <= 2) throw RuntimeException("fail")
            }

            val firstDelay = startTimes[1] - startTimes[0]
            expectThat(firstDelay).isGreaterThanOrEqualTo(500L)
            expectThat(firstDelay).isLessThan(1_000L)

            // base = min(500*3, 1000) = 1000 + jitter [0,1000)
            val secondDelay = startTimes[2] - startTimes[1]
            expectThat(secondDelay).isGreaterThanOrEqualTo(1_000L)
            expectThat(secondDelay).isLessThan(2_000L)
        }
    }
}
