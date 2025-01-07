package org.vechain.indexer

import com.github.kittinunf.fuel.core.FuelError
import com.github.kittinunf.fuel.core.Response
import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.isEqualTo

@ExtendWith(MockKExtension::class)
internal class IndexerTest {

    @MockK private lateinit var responseMocker: IndexerResponseMocker

    @MockK private lateinit var thorClient: ThorClient

    private lateinit var indexer: Indexer

    private val getBlockNumberSlot = slot<Long>()
    private val processBlockSlot = slot<Block>()

    @BeforeEach
    fun setup() {
        every { responseMocker.rollback(any()) } just Runs

        indexer = IndexerMock(responseMocker, thorClient)
    }

    @Nested
    inner class IndexerStart {

        @Test
        fun `Start indexer should initialise with rolling back last synced block`() = runBlocking {
            val indexerIterationsNumber = 1L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
            every { responseMocker.getLastSyncedBlock() } returns BlockIdentifier(number = 100L, id = "0x100") andThen
                    BlockIdentifier(number = 99L, id = "0x99")
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the rollback is performed once
                verify(exactly = 1) { responseMocker.rollback(100L) }
            }
        }

        @Test
        fun `Start indexer should initialise with rolling back the startBlock when no last synced block found`() = runBlocking {
            val indexerIterationsNumber = 1L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the rollback is performed once
                verify(exactly = 1) { responseMocker.rollback(0L) }
            }
        }

        @Test
        fun `Start indexer should process blocks`() = runBlocking {
            val indexerIterationsNumber = 100L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify the correct number of processing of blocks
                verify(atLeast = indexer.currentBlockNumber.toInt()) {
                    responseMocker.processBlock(any())
                }
            }
        }

        @Test
        fun `Start indexer should perform post process blocks`() = runBlocking {
            val indexerIterationsNumber = 100L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // Verify post-processing increments the current block number
                that(indexer.currentBlockNumber).isEqualTo(indexerIterationsNumber)
            }
        }
    }

    @Nested
    inner class IndexerRetry {
        @Test
        fun `Indexer should set status to ERROR when retries are exhausted`() = runBlocking {
            val tooManyRequestsBlockNumber = 100L
            val maxRetries = 5
            var attempts = 0

            // Simulate 429 Too Many Requests on all attempts
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                if (getBlockNumberSlot.captured == tooManyRequestsBlockNumber) {
                    attempts++
                    throw FuelError.wrap(
                        Exception("Too Many Requests"),
                        response = mockk {
                            every { statusCode } returns 429
                        }
                    )
                }
                buildBlock(getBlockNumberSlot.captured)
            }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(iterations = tooManyRequestsBlockNumber + 1) }
            job.join()

            expect {
                // Verify the block fetch was retried the maximum number of times
                coVerify(exactly = maxRetries) { thorClient.getBlock(tooManyRequestsBlockNumber) }
                // Verify the indexer status switched to ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
                // Verify no exception is thrown
                that(attempts).isEqualTo(maxRetries)
            }
        }

        @Test
        fun `Indexer should resume processing after resolving 429 Too Many Requests`() = runBlocking {
            val tooManyRequestsBlockNumber = 100L
            var attempts = 0

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                if (getBlockNumberSlot.captured == tooManyRequestsBlockNumber && attempts < 2) {
                    attempts++
                    throw FuelError.wrap(
                        Exception("Too Many Requests"),
                        response = mockk {
                            every { statusCode } returns 429
                        }
                    )
                }
                buildBlock(getBlockNumberSlot.captured)
            }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(iterations = tooManyRequestsBlockNumber + 2) }
            job.join()
            job.cancelAndJoin()

            expect {
                coVerify(atLeast = 3, atMost = 4) { thorClient.getBlock(tooManyRequestsBlockNumber) }
                that(indexer.currentBlockNumber).isEqualTo(102)
                that(indexer.status).isEqualTo(Status.SYNCING)
            }
        }

        @Test
        fun `Indexer should retry with exponential backoff before restarting after rate limit is hit`() =
            runBlocking {
                val tooManyRequestsBlockNumber = 100L

                // Simulate 429 Too Many Requests on the first two attempts
                var attempts = 0
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                    if (getBlockNumberSlot.captured == tooManyRequestsBlockNumber && attempts < 2) {
                        attempts++
                        throw FuelError.wrap(
                            Exception("Too Many Requests"), // Provide a Throwable
                            response = mockk {
                                every { statusCode } returns 429 // Mock the response with statusCode 429
                            }
                        )
                    }
                    buildBlock(getBlockNumberSlot.captured)
                }

                every { responseMocker.getLastSyncedBlock() } returns null
                every { responseMocker.processBlock(any()) } just Runs

                val job = launch { indexer.start(tooManyRequestsBlockNumber + 1) }
                job.join()

                expect {
                    // Verify that retries were attempted before succeeding
                    that(attempts).isEqualTo(2)
                    // Verify that processing continued successfully after retries
                    that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber + 1)
                    // Verify the status is SYNCING after retries
                    that(indexer.status).isEqualTo(Status.SYNCING)
                }
            }

        @Test
        fun `Indexer should switch to ERROR status after exhausting retries for rate limit exception`() = runBlocking {
            val tooManyRequestsBlockNumber = 100L
            val maxRetries = 5

            // Simulate 429 Too Many Requests on all attempts
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                if (getBlockNumberSlot.captured == tooManyRequestsBlockNumber) {
                    throw FuelError.wrap(
                        Exception("Too Many Requests"),
                        response = mockk {
                            every { statusCode } returns 429 // Mock the response with statusCode 429
                        }
                    )
                }
                buildBlock(getBlockNumberSlot.captured)
            }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            // Start the indexer
            val job = launch { indexer.start(iterations = tooManyRequestsBlockNumber + 1) }
            job.join()

            expect {
                // Use coVerify for suspend function verification
                coVerify(exactly = maxRetries) { thorClient.getBlock(tooManyRequestsBlockNumber) }

                // Verify the current block number is still the one that caused the rate limit exception
                that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber)

                // Verify the indexer status switched to ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
            }
        }

        @Test
        fun `Indexer should not retry on non-429 errors`() = runBlocking {
            val errorBlockNumber = 100L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                if (getBlockNumberSlot.captured == errorBlockNumber) {
                    throw Exception("Some non-429 error")
                }
                buildBlock(getBlockNumberSlot.captured)
            }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(iterations = errorBlockNumber + 1) }
            job.join()

            expect {
                // Verify the block fetch was not retried
                coVerify(exactly = 1) { thorClient.getBlock(errorBlockNumber) }

                // Verify the current block number is still the one that caused the error
                that(indexer.currentBlockNumber).isEqualTo(errorBlockNumber)

                // Verify the indexer status switched to ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
            }
        }

        @Test
        fun `Indexer should stop retrying after reaching max retries`() = runBlocking {
            val tooManyRequestsBlockNumber = 100L
            val maxRetries = 5
            var attempts = 0

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                if (getBlockNumberSlot.captured == tooManyRequestsBlockNumber && attempts < maxRetries) {
                    attempts++
                    throw FuelError.wrap(
                        Exception("Too Many Requests"),
                        response = mockk {
                            every { statusCode } returns 429
                        }
                    )
                }
                buildBlock(getBlockNumberSlot.captured)
            }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(iterations = tooManyRequestsBlockNumber + 1) }
            job.join()

            expect {
                // Verify the block fetch was retried the maximum number of times
                coVerify(exactly = maxRetries) { thorClient.getBlock(tooManyRequestsBlockNumber) }

                // Verify the current block number is still the one that caused the error
                that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber)

                // Verify the indexer status switched to ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
            }
        }

        @Test
        fun `Indexer should apply exponential backoff delays between retries`() = runBlocking {
            val tooManyRequestsBlockNumber = 100L
            val maxRetries = 3
            val initialDelay = 1000L
            var attempts = 0

            val delayTimes = mutableListOf<Long>()
            mockkStatic("kotlinx.coroutines.DelayKt")
            coEvery { delay(capture(delayTimes)) } just Runs

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                if (getBlockNumberSlot.captured == tooManyRequestsBlockNumber && attempts < maxRetries) {
                    attempts++
                    throw FuelError.wrap(
                        Exception("Too Many Requests"),
                        response = mockk {
                            every { statusCode } returns 429
                        }
                    )
                }
                buildBlock(getBlockNumberSlot.captured)
            }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(iterations = tooManyRequestsBlockNumber + 1) }
            job.join()

            expect {
                // Verify delays are applied as exponential backoff
                that(delayTimes).isEqualTo(
                    listOf(initialDelay, initialDelay * 2, initialDelay * 4).toMutableList()
                )
            }

            unmockkStatic("kotlinx.coroutines.DelayKt")
        }
    }

    @Nested
    inner class IndexerRestart {

        @Test
        fun `Indexer should restart at current block when unknown exception is thrown`() =
            runBlocking {
                val finalBlock = BlockIdentifier(number = 99L, id = "0x99")
                val errorBlockNumber = 100L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                        {
                            buildBlock(getBlockNumberSlot.captured)
                        }
                every { responseMocker.getLastSyncedBlock() } returns
                        null andThen
                        null andThen
                        finalBlock
                var calledAlready = false
                every { responseMocker.processBlock(capture(processBlockSlot)) } answers
                        {
                            if (
                                !calledAlready &&
                                processBlockSlot.captured.number == errorBlockNumber
                            ) {
                                calledAlready = true
                                throw Exception("Unknown exception")
                            }
                        }

                // Run the indexer for another two iterations after the error block
                val job = launch { indexer.start(errorBlockNumber + 2) }
                job.join()

                expect {
                    // Indexer should have advanced processing after successfully restarting
                    // processing of faulty block
                    that(indexer.currentBlockNumber).isEqualTo(errorBlockNumber + 1)
                    // Indexer should switch back to SYNCING status error detection
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Indexer should restart & rollback processing at the error block number
                    verify(exactly = 1) { responseMocker.rollback(errorBlockNumber) }
                }
            }

        @Test
        fun `Indexer should restart at block previous to current block when a re-organization is detected`() =
            runBlocking {

                val finalBlock = BlockIdentifier(number = 98L, id = "0x98")
                val reorgBlockNumber = 100L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                        {
                            // At block 100, the parent id is invalid
                            val parentId =
                                if (getBlockNumberSlot.captured == reorgBlockNumber) "0x02321321"
                                else "0x${maxOf(getBlockNumberSlot.captured - 1, 0)}"
                            buildBlock(getBlockNumberSlot.captured, parentId)
                        }

                every { responseMocker.getLastSyncedBlock() } returns
                        null andThen
                        null andThen
                        finalBlock
                every { responseMocker.processBlock(any()) } just Runs

                // Run indexer for a few iterations more after re-organization detected
                val job = launch { indexer.start(reorgBlockNumber + 2) }
                job.join()

                expect {
                    // Indexer should have advanced processing after successfully restarting
                    // processing of faulty block
                    that(indexer.currentBlockNumber).isEqualTo(reorgBlockNumber)
                    // Indexer should switch back to SYNCING status after re-organization detection
                    expectThat(indexer.status).isEqualTo(Status.SYNCING)
                    // The re-organization at block reorgBlockNumber should trigger a rollback of
                    // block
                    // (reorgBlockNumber - 1) data
                    verify(exactly = 1) { responseMocker.rollback(reorgBlockNumber - 1) }
                }
            }
    }

    @Nested
    inner class IndexerStatus {
        @Test
        fun `Indexer starting & processing block is at the SYNCING status`() = runBlocking {
            val iterations = 100L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(iterations) }
            job.join()

            expect {
                // Current block should correspond to number of iterations of indexer run
                that(indexer.currentBlockNumber).isEqualTo(iterations)
                // Status should be SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
                // First initialise should roll back to start block
                verify(exactly = 1) { responseMocker.rollback(0L) }
                // Number of processed blocks should correspond to current block number
                verify(exactly = indexer.currentBlockNumber.toInt()) {
                    responseMocker.processBlock(any())
                }
            }
        }

        @Test
        fun `Indexer should switch to FULLY_SYNCED status when block not found`() = runBlocking {
            val blockNotFound = BlockIdentifier(number = 99L, id = "0x99")

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        if (getBlockNumberSlot.captured >= blockNotFound.number) {
                            throw BlockNotFoundException("Block not found")
                        }
                        buildBlock(getBlockNumberSlot.captured)
                    }
            coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(blockNotFound.number) }
            every { responseMocker.getLastSyncedBlock() } returns null andThen
                    null andThen
                    blockNotFound
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(blockNotFound.number + 1) }
            job.join()

            expect {
                // The current block remains at the one not found
                that(indexer.currentBlockNumber).isEqualTo(blockNotFound.number)
                // Status should switch to FULLY_SYNCED
                that(indexer.status).isEqualTo(Status.FULLY_SYNCED)
            }
        }

        @Test
        fun `Indexer should ensure whether it is FULLY_SYNCED and switch back to SYNCING`() =
            runBlocking {
                val iterations = 101L
                val blockNotFound = BlockIdentifier(number = 99L, id = "0x99")

                // Block is not found the first time indexer tries to fetch it
                var calledAlready = false
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                        {
                            if (!calledAlready && getBlockNumberSlot.captured == blockNotFound.number) {
                                calledAlready = true
                                throw BlockNotFoundException("Block not found")
                            }
                            buildBlock(getBlockNumberSlot.captured)
                        }
                // Simulate a gap between last synced and current best block from chain
                coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(iterations) }
                every { responseMocker.getLastSyncedBlock() } returns null andThen
                        null andThen
                        blockNotFound
                every { responseMocker.processBlock(any()) } just Runs

                // Iterations + (1 iteration) where block is not found to trigger the FULLY_SYNCED
                // status
                val job = launch { indexer.start(iterations + 1) }
                job.join()

                expect {
                    // Current block number should match the number of iterations after we run
                    // indexer (iterations + 1) number of times
                    that(indexer.currentBlockNumber).isEqualTo(iterations)
                    // Status should switch back to syncing after we detect indexer is behind best
                    // on-chain block
                    that(indexer.status).isEqualTo(Status.SYNCING)
                }
            }

        @Test
        fun `Indexer should switch to REORG status upon chain re-organization`() = runBlocking {
            val reorgBlock = 100L
            val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

            // Simulate re-organization by detecting invalid parent block id at reorgBlock
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        // At reorgBlock, the parent id is invalid
                        val parentId =
                            if (getBlockNumberSlot.captured == reorgBlock) "0x02321321"
                            else "0x${maxOf(getBlockNumberSlot.captured - 1, 0)}"
                        buildBlock(getBlockNumberSlot.captured, parentId)
                    }

            every { responseMocker.getLastSyncedBlock() } returns null andThen
                    null andThen
                    lastSyncedBlock
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(reorgBlock + 1) }
            job.join()

            expect {
                // The current block number should match the re-organization block
                that(indexer.currentBlockNumber).isEqualTo(reorgBlock)
                // The indexer status should switch to REORG
                that(indexer.status).isEqualTo(Status.REORG)
            }
        }

        @Test
        fun `Indexer should not trigger a REORG when previous block is null`() = runBlocking {

            // Simulate re-organization by detecting invalid parent block id at reorgBlock
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }

            every { responseMocker.getLastSyncedBlock() } returns null
            every { responseMocker.processBlock(any()) } just Runs

            val job = launch { indexer.start(1L) }
            job.join()

            expect {
                // The current block number should match the re-organization block
                that(indexer.currentBlockNumber).isEqualTo(1L)
                // The indexer status should remain SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
            }
        }

        @Test
        fun `Indexer should switch to ERROR status upon unknown exception thrown`() = runBlocking {
            val unknownExceptionBlock = 100L
            val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
            every { responseMocker.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    lastSyncedBlock
            // Exception is thrown when processing block unknownExceptionBlock
            every { responseMocker.processBlock(capture(processBlockSlot)) } answers
                    {
                        if (processBlockSlot.captured.number == unknownExceptionBlock) {
                            throw Exception("Unknown exception")
                        }
                    }

            val job = launch { indexer.start(unknownExceptionBlock + 1) }
            job.join()

            expect {
                // The current block number should match the exception block
                that(indexer.currentBlockNumber).isEqualTo(unknownExceptionBlock)
                // The indexer status should switch to ERROR
                that(indexer.status).isEqualTo(Status.ERROR)
            }
        }

        @Test
        fun `Indexer should switch to ERROR status upon rate limit exception when fetching block`() =
            runBlocking {
                val tooManyRequestsBlockNumber = 100L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

                // Exception is thrown when attempting to fetch block tooManyRequestsBlockNumber
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                        {
                            if (getBlockNumberSlot.captured != tooManyRequestsBlockNumber)
                                buildBlock(getBlockNumberSlot.captured)
                            else throw Exception("Too Many Requests")
                        }

                every { responseMocker.getLastSyncedBlock() } returns
                        null andThen
                        null andThen
                        lastSyncedBlock
                every { responseMocker.processBlock(capture(processBlockSlot)) } just Runs

                val job = launch { indexer.start(tooManyRequestsBlockNumber + 1) }
                job.join()

                expect {
                    // The current block number should match the exception block
                    that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber)
                    // The indexer status should switch to ERROR
                    that(indexer.status).isEqualTo(Status.ERROR)
                }
            }
    }
}
