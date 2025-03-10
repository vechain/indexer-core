package org.vechain.indexer

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_B3TR_ACTION
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STRINGS
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_TOKEN_EXCHANGE
import org.vechain.indexer.helpers.FileLoaderHelper
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockIdentifier
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
import java.math.BigInteger

@ExtendWith(MockKExtension::class)
internal class IndexerTest {
    @MockK private lateinit var responseMocker: IndexerResponseMocker

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var abiManager: AbiManager

    @MockK private lateinit var businessEventManager: BusinessEventManager

    private lateinit var indexer: IndexerMock

    private val getBlockNumberSlot = slot<Long>()
    private val processBlockSlot = slot<Block>()

    @BeforeEach
    fun setup() {
        every { responseMocker.rollback(any()) } just Runs

        indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)
    }

    @Nested
    inner class IndexerStart {
        @Test
        fun `Start indexer should initialise with rolling back last synced block`() =
            runBlocking {
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
        fun `Start indexer should initialise with rolling back the startBlock when no last synced block found`() =
            runBlocking {
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
        fun `Start indexer should process blocks`() =
            runBlocking {
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
        fun `Start indexer should perform post process blocks`() =
            runBlocking {
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
        fun `Indexer should restart at current block when thor node rate limit is hit`() =
            runBlocking {
                val finalBlock = BlockIdentifier(number = 99L, id = "0x99")
                val tooManyRequestsBlockNumber = 100L

                var rateLimitedAlready = false
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        if (
                            !rateLimitedAlready &&
                            getBlockNumberSlot.captured == tooManyRequestsBlockNumber
                        ) {
                            rateLimitedAlready = true
                            throw Exception("Too Many Requests")
                        }
                        buildBlock(getBlockNumberSlot.captured)
                    }
                every { responseMocker.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    finalBlock

                every { responseMocker.processBlock(any()) } just Runs

                // Run the indexer for another two iterations after the rate limited block number
                val job = launch { indexer.start(tooManyRequestsBlockNumber + 2) }
                job.join()

                expect {
                    // Indexer should have advanced processing after successfully restarting
                    // processing of faulty block
                    that(indexer.currentBlockNumber).isEqualTo(tooManyRequestsBlockNumber + 1)
                    // Indexer should switch back to SYNCING status error detection
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Indexer should restart & rollback processing at the error block number
                    verify(exactly = 1) { responseMocker.rollback(tooManyRequestsBlockNumber) }
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
                            if (getBlockNumberSlot.captured == reorgBlockNumber) {
                                "0x02321321"
                            } else {
                                "0x${maxOf(getBlockNumberSlot.captured - 1, 0)}"
                            }
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
        fun `Indexer starting & processing block is at the SYNCING status`() =
            runBlocking {
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
        fun `Indexer should switch to FULLY_SYNCED status when block not found`() =
            runBlocking {
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
        fun `Indexer should switch to REORG status upon chain re-organization`() =
            runBlocking {
                val reorgBlock = 100L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

                // Simulate re-organization by detecting invalid parent block id at reorgBlock
                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        // At reorgBlock, the parent id is invalid
                        val parentId =
                            if (getBlockNumberSlot.captured == reorgBlock) {
                                "0x02321321"
                            } else {
                                "0x${maxOf(getBlockNumberSlot.captured - 1, 0)}"
                            }
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
        fun `Indexer should not trigger a REORG when previous block is null`() =
            runBlocking {
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
        fun `Indexer should switch to ERROR status upon unknown exception thrown`() =
            runBlocking {
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
                        if (getBlockNumberSlot.captured != tooManyRequestsBlockNumber) {
                            buildBlock(getBlockNumberSlot.captured)
                        } else {
                            throw Exception("Too Many Requests")
                        }
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

    @Nested
    inner class ProcessEvents {
        @Test
        fun `processAllEvents processes all events when no filters are provided`() {
            // Create block with transfer event and a different event
            val block =
                EventMockFactory.createMockBlockWithTransactions(
                    listOf(
                        EventMockFactory.createMockTransaction(
                            listOf(
                                EventMockFactory.arrayEventClause,
                                EventMockFactory.transferEventClause,
                            ),
                        ),
                    ),
                )
            every {
                abiManager.getAbis()
            } returns
                mapOf(
                    "ERC20" to listOf(EventMockFactory.transferAbiElement),
                    "ARRAY" to listOf(EventMockFactory.arrayAbiElement),
                )

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to EventMockFactory.vot3SwapEventDefinition)
            every { businessEventManager.updateCriteriaWithBusinessEvents(any()) } returns FilterCriteria()

            // Act
            val result = indexer.processAllEvents(block)

            // Assert
            expectThat(result.size).isEqualTo(2)
            expectThat(result[0].first.eventType).isEqualTo("AllocationVoteCast")
            expectThat(result[1].first.eventType).isEqualTo("Transfer")
        }

        @Test
        fun `processAllEvents skips business event processing when manager is null`() {
            indexer = IndexerMock(responseMocker, thorClient, abiManager, null)

            val block =
                EventMockFactory.createMockBlockWithTransactions(
                    listOf(
                        EventMockFactory.createMockTransaction(
                            listOf(
                                EventMockFactory.arrayEventClause,
                                EventMockFactory.transferEventClause,
                            ),
                        ),
                    ),
                )
            every {
                abiManager.getAbis()
            } returns
                mapOf(
                    "ERC20" to listOf(EventMockFactory.transferAbiElement),
                    "ARRAY" to listOf(EventMockFactory.arrayAbiElement),
                )

            val criteria = FilterCriteria()

            // Act
            val result = indexer.processAllEvents(block, criteria)

            // Assert
            expectThat(result.size).isNotEqualTo(0)
            verify(exactly = 0) { businessEventManager.getBusinessGenericEventNames(any()) }
            verify(exactly = 0) { businessEventManager.getBusinessEventsByNames(any()) }
        }
    }

    @Nested
    inner class ProcessGenericEvents {
        @Test
        fun `processBlockGenericEvents processes all events when no filters are provided`() {
            // Create block with transfer event and a different event
            val block =
                EventMockFactory.createMockBlockWithTransactions(
                    listOf(
                        EventMockFactory.createMockTransaction(
                            listOf(
                                EventMockFactory.arrayEventClause,
                                EventMockFactory.transferEventClause,
                            ),
                        ),
                    ),
                )
            every {
                abiManager.getAbis()
            } returns
                mapOf(
                    "ERC20" to listOf(EventMockFactory.transferAbiElement),
                    "ARRAY" to listOf(EventMockFactory.arrayAbiElement),
                )

            // Act
            val result = indexer.processBlockGenericEvents(block)

            // Assert
            expectThat(result.size).isEqualTo(2)
            expectThat(result[0].first.eventType).isEqualTo("AllocationVoteCast")
            expectThat(result[1].first.eventType).isEqualTo("Transfer")
        }

        @Test
        fun `processBlockGenericEvents returns empty list when manager is null`() {
            indexer = IndexerMock(responseMocker, thorClient, null, null)

            val block =
                EventMockFactory.createMockBlockWithTransactions(
                    listOf(
                        EventMockFactory.createMockTransaction(
                            listOf(
                                EventMockFactory.arrayEventClause,
                                EventMockFactory.transferEventClause,
                            ),
                        ),
                    ),
                )

            val criteria = FilterCriteria()

            // Act
            val result = indexer.processAllEvents(block, criteria)

            // Assert
            expectThat(result.size).isEqualTo(0)
            verify(exactly = 0) { abiManager.getAbis() }
        }

        @Test
        fun `processBlockGenericEvents correctly returns results with filters`() {
            // Create block with transfer event and a different event
            val block =
                EventMockFactory.createMockBlockWithTransactions(
                    listOf(
                        EventMockFactory.createMockTransaction(
                            listOf(
                                EventMockFactory.arrayEventClause,
                                EventMockFactory.transferEventClause,
                            ),
                        ),
                    ),
                )
            every {
                abiManager.getAbis()
            } returns
                mapOf(
                    "ERC20" to listOf(EventMockFactory.transferAbiElement),
                    "ARRAY" to listOf(EventMockFactory.arrayAbiElement),
                )

            // Act
            val result =
                indexer.processBlockGenericEvents(
                    block,
                    FilterCriteria(
                        eventNames = listOf("Transfer"),
                    ),
                )

            // Assert
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].first.eventType).isEqualTo("Transfer")
        }

        @Test
        fun `should get latest business events and generic events`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_B3TR_ACTION

            // Process the block
            val result = indexer.processAllEvents(block)

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "B3TR_ActionReward",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should return all business events and all generic events if remove duplicates is false `() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_B3TR_ACTION

            // Process the block
            val result = indexer.processAllEvents(block, FilterCriteria(removeDuplicates = false))

            val expectedEventTypes =
                listOf(
                    "RewardDistributed",
                    "Transfer",
                    "RewardDistributed",
                    "Transfer",
                    "Transfer",
                    "B3TR_ActionReward",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should return all VET transfers as events if vet transfers is set to true`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    block,
                    FilterCriteria(
                        vetTransfers = true,
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "VET_TRANSFER",
                    "VET_TRANSFER",
                    "B3TR_ActionReward",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should filter events based on names if event names to process was passed into filter criteria`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    block,
                    FilterCriteria(
                        eventNames = listOf("Transfer"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "Transfer",
                    "Transfer",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should filter events based on abi names if passed into filter criteria`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_STRINGS

            // Process the block
            val result =
                indexer.processAllEvents(
                    block,
                    FilterCriteria(
                        abiNames = listOf("stringsAbis"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "AddressChanged",
                    "AddrChanged",
                    "AddressChanged",
                    "NameChanged",
                    "TextChanged",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should filter events based on contract address if passed into filter criteria`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    block,
                    FilterCriteria(
                        contractAddresses = listOf("0x6bee7ddab6c99d5b2af0554eaea484ce18f52631"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "RewardDistributed",
                    "RewardDistributed",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should return empty result if no events for contract address`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    block,
                    FilterCriteria(
                        contractAddresses = listOf("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"),
                    ),
                )

            // Expect no events to be returned
            Assertions.assertTrue(result.isEmpty(), "Result should be empty")
        }

        @Test
        fun `should apply multiple filters if multiple are passed in`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_STRINGS

            // Process the block
            val result =
                indexer.processAllEvents(
                    block,
                    FilterCriteria(
                        contractAddresses = listOf("0xabac49445584c8b6c1472b030b1076ac3901d7cf"),
                        eventNames = listOf("TextChanged", "NameChanged"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "NameChanged",
                    "TextChanged",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should get latest block and process events correctly`() {
            val businessEventManager = mockk<BusinessEventManager>()
            val abiManager = AbiManager()
            every { businessEventManager.updateCriteriaWithBusinessEvents(any()) } returns FilterCriteria()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)

            // Input block to process
            val block: Block = BLOCK_STRINGS

            // Expected event types
            val expectedEventTypes =
                listOf(
                    "RewardDistributed",
                    "Transfer",
                    "RewardDistributed",
                    "Transfer",
                    "RewardDistributed",
                    "Transfer",
                    "RewardDistributed",
                    "Transfer",
                    "Transfer",
                    "RewardDistributed",
                    "Transfer",
                    "Approval",
                    "Transfer",
                    "Transfer",
                    "Sync",
                    "Swap",
                    "AddressChanged",
                    "AddrChanged",
                    "AddressChanged",
                    "NameChanged",
                    "RewardDistributed",
                    "Transfer",
                    "Upgraded",
                    "Initialized",
                    "Transfer",
                    "Transfer",
                    "Approval",
                    "Transfer",
                    "Transfer",
                    "Sync",
                    "Swap",
                    "RewardDistributed",
                    "Transfer",
                    "Transfer",
                    "Transfer",
                    "Sync",
                    "Swap",
                    "Transfer",
                    "TextChanged",
                    "RewardDistributed",
                    "Transfer",
                    "RewardDistributed",
                    "Transfer",
                )

            // Process the block
            val result = indexer.processAllEvents(block)

            // Extract event types from the result
            val eventTypes = result.map { it.first.eventType }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should process business events correctly and map to correct one`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager)

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val block: Block = BLOCK_TOKEN_EXCHANGE

            // Process the block
            val events =
                indexer.processBlockGenericEvents(
                    block,
                    FilterCriteria(),
                )

            val result = indexer.processBlockBusinessEvents(events)

            val expectedEventTypes =
                listOf(
                    "Token_FTSwap",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }
    }

    @Nested
    inner class ProcessBlockBusinessEvents {
        @Test
        fun `processBlockBusinessEvents processes all events when no filters are provided`() {
            val b3trSwapVot3IndexedEvent =
                EventMockFactory.createIndexedEvent(
                    "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                    0,
                    EventMockFactory.b3trSwapVot3Event,
                )
            val b3trSwapB3trIndexedEvent =
                EventMockFactory.createIndexedEvent(
                    "0x5ef79995fe8a89e0812330e4378eb2660cede699",
                    0,
                    EventMockFactory.b3trSwapB3trEvent,
                )

            val events =
                listOf(
                    b3trSwapB3trIndexedEvent to EventMockFactory.b3trSwapB3trEvent,
                    b3trSwapVot3IndexedEvent to EventMockFactory.b3trSwapVot3Event,
                )

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to EventMockFactory.vot3SwapEventDefinition)
            every { businessEventManager.updateCriteriaWithBusinessEvents(any()) } returns FilterCriteria()

            val indexer = IndexerMock(responseMocker, thorClient, abiManager, businessEventManager, true)
            // Act
            val result = indexer.processBlockBusinessEvents(events)
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].second.getEventType()).isEqualTo("B3trVot3Swap")
            expectThat(result[0].second.getReturnValues()).isEqualTo(
                mapOf(
                    "user" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "amountB3TR" to BigInteger("50000000000000000000"),
                    "amountVOT3" to BigInteger("50000000000000000000"),
                ),
            )
        }

        @Test
        fun `processBlockBusinessEvents returns empty list when manager is null`() {
            indexer = IndexerMock(responseMocker, thorClient, null, null)

            val b3trSwapVot3IndexedEvent =
                EventMockFactory.createIndexedEvent(
                    "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                    0,
                    EventMockFactory.b3trSwapVot3Event,
                )
            val b3trSwapB3trIndexedEvent =
                EventMockFactory.createIndexedEvent(
                    "0x5ef79995fe8a89e0812330e4378eb2660cede699",
                    0,
                    EventMockFactory.b3trSwapB3trEvent,
                )

            val events =
                listOf(
                    b3trSwapB3trIndexedEvent to EventMockFactory.b3trSwapB3trEvent,
                    b3trSwapVot3IndexedEvent to EventMockFactory.b3trSwapVot3Event,
                )

            val criteria = FilterCriteria()

            // Act
            val result = indexer.processBlockBusinessEvents(events, criteria)

            // Assert
            expectThat(result.size).isEqualTo(0)
            verify(exactly = 0) { businessEventManager.getAllBusinessEvents() }
        }
    }
}
