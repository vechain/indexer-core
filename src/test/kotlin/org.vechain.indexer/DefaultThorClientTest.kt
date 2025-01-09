package org.vechain.indexer.thor.client

import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.fuel.core.FuelError
import com.github.kittinunf.fuel.core.Request
import com.github.kittinunf.fuel.core.Response
import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.vechain.indexer.BlockTestBuilder
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo

class DefaultThorClientTest {

    private lateinit var thorClient: DefaultThorClient
    private val logger = mockk<Logger>(relaxed = true)

    private val baseUrl = "http://mock-base-url"
    private val headers = arrayOf("Authorization" to "mock-token")

    @BeforeEach
    fun setup() {
        mockkObject(Fuel)
        thorClient = spyk(
            DefaultThorClient(
                baseUrl,
                *headers
            ).apply {
                this::class.java.getDeclaredField("logger").also {
                    it.isAccessible = true
                    it.set(this, logger)
                }
            }
        )
    }

    @Test
    fun `should fetch block by number successfully`() = runBlocking {
        val blockNumber = 12345L
        val mockBlock = BlockTestBuilder.buildBlock(blockNumber)
        val mockResponse = """
        {
            "id": "${mockBlock.id}",
            "number": ${mockBlock.number},
            "timestamp": ${mockBlock.timestamp},
            "size": ${mockBlock.size},
            "gasUsed": ${mockBlock.gasUsed},
            "gasLimit": ${mockBlock.gasLimit},
            "parentID": "${mockBlock.parentID}",
            "beneficiary": "${mockBlock.beneficiary}",
            "totalScore": ${mockBlock.totalScore},
            "txsRoot": "${mockBlock.txsRoot}",
            "stateRoot": "${mockBlock.stateRoot}",
            "receiptsRoot": "${mockBlock.receiptsRoot}",
            "txsFeatures": ${mockBlock.txsFeatures},
            "com": ${mockBlock.com},
            "signer": "${mockBlock.signer}",
            "isTrunk": ${mockBlock.isTrunk},
            "isFinalized": ${mockBlock.isFinalized},
            "transactions": []
        }
    """.trimIndent()

        every {
            Fuel.get("$baseUrl/blocks/$blockNumber?expanded=true")
                .appendHeader(*headers)
                .response()
        } returns Triple(
            mockk<Request>(),
            mockk<Response>(),
            com.github.kittinunf.result.Result.success(mockResponse.toByteArray())
        )

        val result = thorClient.getBlock(blockNumber)

        expectThat(result).isEqualTo(mockBlock)
        unmockkObject(Fuel)
    }

    @Test
    fun `should fetch the best block successfully`() = runBlocking {
        val blockNumber = 12345L
        val mockBlock = BlockTestBuilder.buildBlock(blockNumber)
        val mockResponse = """
        {
            "id": "${mockBlock.id}",
            "number": ${mockBlock.number},
            "timestamp": ${mockBlock.timestamp},
            "size": ${mockBlock.size},
            "gasUsed": ${mockBlock.gasUsed},
            "gasLimit": ${mockBlock.gasLimit},
            "parentID": "${mockBlock.parentID}",
            "beneficiary": "${mockBlock.beneficiary}",
            "totalScore": ${mockBlock.totalScore},
            "txsRoot": "${mockBlock.txsRoot}",
            "stateRoot": "${mockBlock.stateRoot}",
            "receiptsRoot": "${mockBlock.receiptsRoot}",
            "txsFeatures": ${mockBlock.txsFeatures},
            "com": ${mockBlock.com},
            "signer": "${mockBlock.signer}",
            "isTrunk": ${mockBlock.isTrunk},
            "isFinalized": ${mockBlock.isFinalized},
            "transactions": []
        }
    """.trimIndent()

        every {
            Fuel.get("$baseUrl/blocks/best?expanded=true")
                .appendHeader(*headers)
                .response()
        } returns Triple(
            mockk<Request>(),
            mockk<Response>(),
            com.github.kittinunf.result.Result.success(mockResponse.toByteArray())
        )

        val result = thorClient.getBestBlock()

        expectThat(result).isEqualTo(mockBlock)
        unmockkObject(Fuel)
    }
    @Test
    fun `should retry on HTTP 429 and eventually succeed`() = runBlocking {
        val blockNumber = 12345L
        val maxRetries = 5
        var attemptCount = 0

        // Mock block data
        val mockBlock = BlockTestBuilder.buildBlock(blockNumber)

        // Spy on DefaultThorClient
        val thorClientSpy = spyk(thorClient)

        // Mock fetchBlock to throw FuelError 429 for the first `maxRetries` attempts, then return success
        coEvery { thorClientSpy.fetchBlock(any()) } coAnswers {
            attemptCount++
            if (attemptCount < maxRetries) {
                throw FuelError.wrap(
                    Exception("Too Many Requests"),
                    response = mockk {
                        every { statusCode } returns 429
                    }
                )
            }
            mockBlock // Return the mock block after retries
        }

        // Call the getBlock method, which internally calls fetchBlock
        val result = thorClientSpy.getBlock(blockNumber)

        // Verify the block is fetched successfully after retries
        expectThat(result).isEqualTo(mockBlock)

        // Verify fetchBlock was called the expected number of times
        coVerify(exactly = maxRetries) { thorClientSpy.fetchBlock(any()) }
    }
    @Test
    fun `should throw exception when max retries are exhausted`() = runBlocking {
        val blockNumber = 12345L
        val maxRetries = 5
        var attemptCount = 0

        // Spy on DefaultThorClient
        val thorClientSpy = spyk(thorClient)

        // Mock fetchBlock to always throw 429
        coEvery { thorClientSpy.fetchBlock(any()) } coAnswers {
            attemptCount++
            throw FuelError.wrap(
                Exception("Too Many Requests"),
                response = mockk {
                    every { statusCode } returns 429
                }
            )
        }

        // Call getBlock and expect an exception
        val exception = kotlin.runCatching { thorClientSpy.getBlock(blockNumber) }.exceptionOrNull()

        // Verify the exception is thrown after max retries
        expectThat(exception).isA<Exception>().and {
            get { message }.isEqualTo("Max retries exhausted due to HTTP 429")
        }
        coVerify(exactly = maxRetries) { thorClientSpy.fetchBlock(any()) }
    }

    @Test
    fun `should not retry on non-429 errors`() = runBlocking {
        val blockNumber = 12345L
        var attemptCount = 0

        // Spy on DefaultThorClient
        val thorClientSpy = spyk(thorClient)

        // Mock fetchBlock to throw a non-429 error
        coEvery { thorClientSpy.fetchBlock(any()) } coAnswers {
            attemptCount++
            throw FuelError.wrap(
                Exception("Internal Server Error"),
                response = mockk {
                    every { statusCode } returns 500
                }
            )
        }

        // Call getBlock and expect an exception
        val exception = kotlin.runCatching { thorClientSpy.getBlock(blockNumber) }.exceptionOrNull()

        // Verify the error is thrown and no retries are performed
        expectThat(exception).isA<FuelError>().and {
            get { response.statusCode }.isEqualTo(500)
        }
        coVerify(exactly = 1) { thorClientSpy.fetchBlock(any()) }
    }

    @Test
    fun `should apply exponential backoff delays between retries`() = runBlocking {
        val blockNumber = 12345L
        val initialDelay = 1000L
        val maxRetries = 5 // Ensure this matches your DefaultThorClient configuration
        val delayTimes = mutableListOf<Long>()

        // Spy on DefaultThorClient
        val thorClientSpy = spyk(thorClient)

        // Mock fetchBlock to throw 429 for all attempts
        coEvery { thorClientSpy.fetchBlock(any()) } coAnswers {
            throw FuelError.wrap(
                Exception("Too Many Requests"),
                response = mockk {
                    every { statusCode } returns 429
                }
            )
        }

        // Mock delay to capture the delay times
        mockkStatic("kotlinx.coroutines.DelayKt")
        coEvery { delay(capture(delayTimes)) } just Runs

        // Call getBlock and expect an exception
        kotlin.runCatching { thorClientSpy.getBlock(blockNumber) }

        // Calculate the expected delay times
        val expectedDelays = (0 until maxRetries).fold(mutableListOf<Long>()) { acc, i ->
            acc.apply { add(minOf(initialDelay * (1 shl i), 16000L)) } // Ensure it caps at maxDelay
        }

        // Verify the delay times follow exponential backoff
        expectThat(delayTimes).isEqualTo(expectedDelays)

        // Unmock delay
        unmockkStatic("kotlinx.coroutines.DelayKt")
    }

    @Test
    fun `should succeed on the last retry`() = runBlocking {
        val blockNumber = 12345L
        val maxRetries = 5
        var attemptCount = 0

        // Mock block data
        val mockBlock = BlockTestBuilder.buildBlock(blockNumber)

        // Spy on DefaultThorClient
        val thorClientSpy = spyk(thorClient)

        // Mock fetchBlock to throw 429 until the last retry
        coEvery { thorClientSpy.fetchBlock(any()) } coAnswers {
            attemptCount++
            if (attemptCount < maxRetries) {
                throw FuelError.wrap(
                    Exception("Too Many Requests"),
                    response = mockk {
                        every { statusCode } returns 429
                    }
                )
            }
            mockBlock // Return the mock block on the last retry
        }

        // Call the getBlock method
        val result = thorClientSpy.getBlock(blockNumber)

        // Verify the block is fetched successfully on the last retry
        expectThat(result).isEqualTo(mockBlock)

        // Verify fetchBlock was called the expected number of times
        coVerify(exactly = maxRetries) { thorClientSpy.fetchBlock(any()) }
    }
}
