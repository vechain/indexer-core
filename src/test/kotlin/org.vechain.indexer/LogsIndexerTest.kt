package org.vechain.indexer

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_BASE_REWARD
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STRINGS
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_TOKEN_EXCHANGE
import org.vechain.indexer.fixtures.EventLogFixtures.LOGS_B3TR_ACTION
import org.vechain.indexer.fixtures.EventLogFixtures.LOGS_STRINGS
import org.vechain.indexer.fixtures.EventLogFixtures.LOGS_TOKEN_EXCHANGE
import org.vechain.indexer.fixtures.FileFixtures.abiFiles
import org.vechain.indexer.fixtures.FileFixtures.businessEventFiles
import org.vechain.indexer.fixtures.TransferLogFixtures.LOGS_VET_TRANSFER
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.enums.LogType
import org.vechain.indexer.thor.model.*
import strikt.api.expect
import strikt.assertions.isEqualTo

@ExtendWith(MockKExtension::class)
internal class LogsIndexerTest {
    @MockK private lateinit var responseMocker: LogsIndexerResponseMocker

    @MockK private lateinit var thorClient: ThorClient

    @MockK private lateinit var abiManager: AbiManager

    private var blockBatchSize: Long = 1

    private var logFetchLimit: Long = 100

    @MockK private lateinit var businessEventManager: BusinessEventManager

    private lateinit var indexer: LogIndexerMock
    private lateinit var transferIndexer: LogIndexerMock

    private val getBlockNumberSlot = slot<Long>()
    private val processLogsSlot = slot<List<EventLog>>()

    @BeforeEach
    fun setup() {
        every { responseMocker.rollback(any()) } just Runs

        indexer =
            LogIndexerMock(
                responseMocker,
                setOf(LogType.EVENT),
                blockBatchSize,
                logFetchLimit,
                thorClient,
                abiManager,
                businessEventManager,
            )

        transferIndexer =
            LogIndexerMock(
                responseMocker,
                setOf(LogType.TRANSFER),
                blockBatchSize,
                logFetchLimit,
                thorClient,
                abiManager,
                businessEventManager,
            )
    }

    @Nested
    inner class LogsIndexerStart {
        @Test
        fun `Start indexer should initialise and status should be set to SYNCING`() = runBlocking {
            val indexerIterationsNumber = 1L

            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    buildBlock(getBlockNumberSlot.captured)
                }
            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(10000L)
            coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

            every { responseMocker.getLastSyncedBlock() } returns
                BlockIdentifier(
                    number = 100L,
                    id = "0x100",
                ) andThen
                BlockIdentifier(number = 99L, id = "0x99")
            every { responseMocker.processLogs(any(), any()) } just Runs
            every { responseMocker.processBlock(any()) } just Runs
            every { responseMocker.rollback(any()) } just Runs

            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join()

            expect {
                // Verify the status is SYNCING
                that(indexer.status).isEqualTo(Status.SYNCING)
            }
        }

        @Test
        fun `Start indexer should initialise with status SYNCING when no last synced block found`() =
            runBlocking {
                val indexerIterationsNumber = 1L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
                coEvery { thorClient.getFinalizedBlock() } returns buildBlock(10000L)
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }
                every { responseMocker.getLastSyncedBlock() } returns null
                every { responseMocker.processLogs(any(), any()) } just Runs
                every { responseMocker.processBlock(any()) } just Runs
                every { responseMocker.rollback(any()) } just Runs

                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join()

                expect {
                    // Verify the status is SYNCING
                    that(indexer.status).isEqualTo(Status.SYNCING)
                }
            }

        @Test
        fun `Start indexer should process event logs`() = runBlocking {
            val indexerIterationsNumber = 1L

            // Mock fetching event logs
            coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(100000L)
            coEvery { thorClient.getBlock(any()) } returns buildBlock(100000L)

            // Mock last synced block (so it starts from the beginning)
            coEvery { responseMocker.getLastSyncedBlock() } returns null

            // Mock the processLogs() call
            coEvery { responseMocker.processLogs(any(), any()) } just Runs
            every { responseMocker.rollback(any()) } just Runs

            coEvery { responseMocker.processBlock(any()) } just Runs

            // Run the indexer
            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join() // Wait for completion

            expect {
                // Verify the status is SYNCING after start
                that(indexer.status).isEqualTo(Status.SYNCING)

                // Ensure logs were processed at least once
                verify(exactly = 1) { responseMocker.processLogs(any(), any()) }
            }
        }

        @Test
        fun `Start indexer should process transfer logs`() = runBlocking {
            val indexerIterationsNumber = 1L

            // Mock fetching event logs
            coEvery { thorClient.getVetTransfers(any()) } coAnswers { LOGS_VET_TRANSFER }

            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(100000L)
            coEvery { thorClient.getBlock(any()) } returns buildBlock(100000L)

            // Mock last synced block (so it starts from the beginning)
            coEvery { responseMocker.getLastSyncedBlock() } returns null

            // Mock the processLogs() call
            coEvery { responseMocker.processLogs(any(), any()) } just Runs

            coEvery { responseMocker.processBlock(any()) } just Runs
            every { responseMocker.rollback(any()) } just Runs

            // Run the indexer
            val job = launch { transferIndexer.start(indexerIterationsNumber) }
            job.join() // Wait for completion

            expect {
                // Verify the status is SYNCING after start
                that(transferIndexer.status).isEqualTo(Status.SYNCING)

                // Ensure logs were processed at least once
                verify(exactly = 1) { responseMocker.processLogs(any(), any()) }
            }
        }

        @Test
        fun `Indexer should process at a block level when getting near best block`() = runBlocking {
            val indexerIterationsNumber = 1L

            // Mock fetching event logs
            coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(2L)
            coEvery { thorClient.getBlock(any()) } returns buildBlock(100000L)

            // Mock last synced block (so it starts from the beginning)
            coEvery { responseMocker.getLastSyncedBlock() } returns null

            // Mock the processLogs() call
            coEvery { responseMocker.processLogs(any(), any()) } just Runs

            coEvery { responseMocker.processBlock(any()) } just Runs
            every { responseMocker.rollback(any()) } just Runs

            // Run the indexer
            val job = launch { indexer.start(indexerIterationsNumber) }
            job.join() // Wait for completion

            expect {
                // Verify the status is SYNCING after start
                that(indexer.status).isEqualTo(Status.SYNCING)

                // Ensure logs were processed at least once
                verify(exactly = 1) { responseMocker.processLogs(any(), any()) }

                // Ensure processBlock was called
                verify(exactly = 1) { responseMocker.processBlock(any()) }
            }
        }

        @Test
        fun `Indexer should switch to FULLY_SNCED when fully synced`() = runBlocking {
            val blockNotFound = BlockIdentifier(number = 99L, id = "0x99")
            // Mock fetching event logs
            coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }
            coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(blockNotFound.number) }
            coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                {
                    if (getBlockNumberSlot.captured >= blockNotFound.number) {
                        throw BlockNotFoundException("Block not found")
                    }
                    buildBlock(getBlockNumberSlot.captured)
                }
            coEvery { thorClient.getFinalizedBlock() } coAnswers { buildBlock(1) }

            every { responseMocker.getLastSyncedBlock() } returns
                null andThen
                null andThen
                blockNotFound
            every { responseMocker.processBlock(any()) } just Runs
            // Mock the processLogs() call
            coEvery { responseMocker.processLogs(any(), any()) } just Runs
            every { responseMocker.rollback(any()) } just Runs

            // Run the indexer
            val job = launch { indexer.start(blockNotFound.number + 1) }
            job.join() // Wait for completion

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
                val iterations = 10L
                val blockNotFound = BlockIdentifier(number = 9L, id = "0x9")

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
                coEvery { thorClient.getFinalizedBlock() } coAnswers { buildBlock(0L) }
                coEvery { thorClient.getBestBlock() } returns buildBlock(10000L)
                every { responseMocker.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    blockNotFound
                every { responseMocker.processBlock(any()) } just Runs
                coEvery { responseMocker.processLogs(any(), any()) } just Runs
                every { responseMocker.rollback(any()) } just Runs

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
            val reorgBlock = 3L
            val lastSyncedBlock = BlockIdentifier(number = 2L, id = "0x2")

            // Simulate re-organization by detecting invalid parent block id at reorgBlock
            coEvery { thorClient.getFinalizedBlock() } coAnswers { buildBlock(0L) }
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

            every { responseMocker.getLastSyncedBlock() } returns
                null andThen
                null andThen
                lastSyncedBlock
            every { responseMocker.processBlock(any()) } just Runs
            coEvery { responseMocker.processLogs(any(), any()) } just Runs
            every { responseMocker.rollback(any()) } just Runs

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
        fun `LogsIndexer should switch to ERROR status upon unknown exception thrown`() =
            runBlocking {
                val unknownExceptionBlock = 2L
                val lastSyncedBlock = BlockIdentifier(number = 1L, id = "0x1")

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }

                coEvery { thorClient.getFinalizedBlock() } returns buildBlock(unknownExceptionBlock)

                coEvery { responseMocker.getLastSyncedBlock() } returns
                    null andThen
                    null andThen
                    lastSyncedBlock

                every { responseMocker.rollback(any()) } just Runs

                coEvery { thorClient.getEventLogs(any()) } returns
                    listOf(
                        EventLog(
                            address = "0x123",
                            topics = listOf("0xabc"),
                            data = "0xdata",
                            meta =
                                EventMeta(
                                    blockID = "0x2",
                                    blockNumber = unknownExceptionBlock,
                                    blockTimestamp = 123456789,
                                    txID = "0xtx",
                                    txOrigin = "0xorigin",
                                    clauseIndex = 0,
                                ),
                        ),
                    )

                coEvery {
                    responseMocker.processLogs(capture(processLogsSlot), emptyList())
                } answers
                    {
                        if (
                            processLogsSlot.captured.any {
                                it.meta.blockNumber == unknownExceptionBlock
                            }
                        ) {
                            throw Exception("Unknown exception")
                        }
                    }

                val job = launch { indexer.start(1) }
                job.join()

                expect {
                    // Ensure the indexer stopped at 0 as this is the first block in the batch where
                    // exception was thrown
                    that(indexer.currentBlockNumber).isEqualTo(0)

                    // Ensure the status switched to ERROR
                    that(indexer.status).isEqualTo(Status.ERROR)
                }
            }
    }

    @Nested
    inner class ProcessBlocks {
        @Test
        fun `Indexer should process events at a block level when getting near best block`() =
            runBlocking {
                val indexer =
                    LogIndexerMock(
                        responseMocker,
                        setOf(LogType.EVENT),
                        blockBatchSize,
                        logFetchLimit,
                        thorClient,
                        abiManager,
                        businessEventManager,
                        emptyList(),
                        emptyList(),
                        false,
                    )

                val indexerIterationsNumber = 1L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

                // Mock fetching event logs
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

                coEvery { thorClient.getFinalizedBlock() } returns buildBlock(0L)
                coEvery { thorClient.getBlock(any()) } coAnswers { BLOCK_STRINGS }

                // Mock last synced block (so it starts from the beginning)
                coEvery { responseMocker.getLastSyncedBlock() } returns null andThen lastSyncedBlock

                // Mock the processLogs() call
                coEvery { responseMocker.processLogs(any(), any()) } just Runs
                coEvery { responseMocker.processBlock(any()) } just Runs
                every { responseMocker.rollback(any()) } just Runs

                // Run the indexer
                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join() // Wait for completion

                expect {
                    // Verify the status is SYNCING after start
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Ensure logs were processed at least once
                    verify(exactly = 1) { responseMocker.processLogs(any(), any()) }
                }
            }

        @Test
        fun `Indexer should process transfers at a block level when getting near best block`() =
            runBlocking {
                val indexer =
                    LogIndexerMock(
                        responseMocker,
                        setOf(LogType.TRANSFER),
                        blockBatchSize,
                        logFetchLimit,
                        thorClient,
                        abiManager,
                        businessEventManager,
                        emptyList(),
                        emptyList(),
                        false,
                    )

                val indexerIterationsNumber = 1L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

                // Mock fetching event logs
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

                coEvery { thorClient.getFinalizedBlock() } returns buildBlock(0L)
                coEvery { thorClient.getBlock(any()) } coAnswers { BLOCK_TOKEN_EXCHANGE }

                // Mock last synced block (so it starts from the beginning)
                coEvery { responseMocker.getLastSyncedBlock() } returns null andThen lastSyncedBlock

                // Mock the processLogs() call
                coEvery { responseMocker.processLogs(any(), any()) } just Runs
                coEvery { responseMocker.processBlock(any()) } just Runs
                every { responseMocker.rollback(any()) } just Runs

                // Run the indexer
                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join() // Wait for completion

                expect {
                    // Verify the status is SYNCING after start
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Ensure logs were processed at least once
                    verify(exactly = 1) { responseMocker.processLogs(any(), any()) }
                }
            }

        @Test
        fun `Indexer should process all clauses when operating at a block level when getting near best block`() =
            runBlocking {
                val businessEventManager = BusinessEventManager(businessEventFiles)
                val abiManager = AbiManager(abiFiles)

                val indexer =
                    LogIndexerMock(
                        responseMocker,
                        setOf(LogType.EVENT),
                        blockBatchSize,
                        logFetchLimit,
                        thorClient,
                        abiManager,
                        businessEventManager,
                        emptyList(),
                        emptyList(),
                        false,
                    )

                val indexerIterationsNumber = 1L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }
                coEvery { thorClient.getFinalizedBlock() } returns buildBlock(0L)
                coEvery { thorClient.getBlock(any()) } coAnswers { BLOCK_STARGATE_BASE_REWARD }

                coEvery { responseMocker.getLastSyncedBlock() } returns null andThen lastSyncedBlock

                // --- Capture block and logs passed to processLogs
                val logSlot = slot<List<EventLog>>()
                val transferSlot = slot<List<TransferLog>>()
                coEvery { responseMocker.processLogs(capture(logSlot), capture(transferSlot)) } just
                    Runs
                coEvery { responseMocker.processBlock(any()) } just Runs
                every { responseMocker.rollback(any()) } just Runs

                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join()

                expect {
                    that(indexer.status).isEqualTo(Status.SYNCING)

                    verify(exactly = 1) { responseMocker.processLogs(any(), any()) }

                    // Additional assertions on captured block and logs
                    val capturedLogs = logSlot.captured
                    val capturedTransfers = transferSlot.captured

                    val events =
                        indexer.processBlockGenericEvents(
                            capturedLogs,
                            capturedTransfers,
                            FilterCriteria(
                                vetTransfers = false,
                            ),
                        )
                    val result = indexer.processOnlyBusinessEvents(events)
                    expect {
                        that(result.size).isEqualTo(3)
                        that(result[0].params.getEventType())
                            .isEqualTo("STARGATE_CLAIM_REWARDS_BASE")
                        that(result[1].params.getEventType())
                            .isEqualTo("STARGATE_CLAIM_REWARDS_BASE")
                        that(result[2].params.getEventType())
                            .isEqualTo("STARGATE_CLAIM_REWARDS_BASE")
                    }
                }
            }

        fun `Indexer should prcoess with filters at a block level when getting near best block`() =
            runBlocking {
                val eventCriteria =
                    listOf(
                        EventCriteria(
                            address = "0x45429a2255e7248e57fce99e7239aed3f84b7a53",
                            topic0 =
                                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                            topic2 =
                                "0x000000000000000000000000b2e4fc26e1ce8bd223559b4e82c4c136c4051277",
                        ),
                    )
                val transferCriteria =
                    listOf(
                        TransferCriteria(
                            txOrigin = "0xeeb0b1ead396b75c820130dafdee2898be939cf6",
                            sender = "0xf9b02b47694fd635a413f16dc7b38af06cc16fe5",
                            recipient = "0x349ede93b675c0f0f8d7cdad74ecf1419943e6ac",
                        ),
                    )
                val indexer =
                    LogIndexerMock(
                        responseMocker,
                        setOf(LogType.TRANSFER, LogType.EVENT),
                        blockBatchSize,
                        logFetchLimit,
                        thorClient,
                        abiManager,
                        businessEventManager,
                        eventCriteria,
                        transferCriteria,
                        false,
                    )

                val indexerIterationsNumber = 1L
                val lastSyncedBlock = BlockIdentifier(number = 99L, id = "0x99")

                coEvery { thorClient.getFinalizedBlock() } returns buildBlock(0L)
                coEvery { thorClient.getBlock(any()) } coAnswers { BLOCK_TOKEN_EXCHANGE }

                // Mock last synced block (so it starts from the beginning)
                coEvery { responseMocker.getLastSyncedBlock() } returns null andThen lastSyncedBlock

                // Mock the processLogs() call
                coEvery { responseMocker.processLogs(any(), any()) } just Runs
                coEvery { responseMocker.processBlock(any()) } just Runs
                every { responseMocker.rollback(any()) } just Runs

                // Run the indexer
                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join() // Wait for completion

                expect {
                    // Verify the status is SYNCING after start
                    that(indexer.status).isEqualTo(Status.SYNCING)
                    // Ensure logs were processed at least once
                    verify(exactly = 1) { responseMocker.processLogs(any(), any()) }
                }
            }
    }

    @Nested
    inner class ProcessEvents {
        @Test
        fun `should return empty list if abi manager not defined`() {
            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    null,
                    null,
                )

            every { responseMocker.rollback(any()) } just Runs

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result = indexer.processAllEvents(logs, emptyList())
            // Assert that all expected events are present and in the correct order
            expect { that(result).isEqualTo(emptyList()) }
        }

        @Test
        fun `should get latest business events and generic events`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            every { responseMocker.rollback(any()) } just Runs

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result = indexer.processAllEvents(logs, emptyList())

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should return all business events and all generic events if remove duplicates is false `() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    eventLogs = logs,
                    criteria = FilterCriteria(removeDuplicates = false),
                )

            val expectedEventTypes =
                listOf(
                    "RewardDistributed",
                    "Transfer",
                    "Transfer",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should return all VET transfers as events if vet transfers is set to true`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input logs to process
            val eventLogs: List<EventLog> = LOGS_B3TR_ACTION
            val transferLogs: List<TransferLog> = LOGS_VET_TRANSFER

            // Process the block
            val result =
                indexer.processAllEvents(
                    eventLogs = eventLogs,
                    transferLogs = transferLogs,
                    FilterCriteria(
                        vetTransfers = true,
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "VET_TRANSFER",
                    "VET_TRANSFER",
                    "VET_TRANSFER",
                    "VET_TRANSFER",
                    "VET_TRANSFER",
                    "VET_TRANSFER",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should filter events based on names if event names to process was passed into filter criteria`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input logs to process
            val eventLogs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    eventLogs,
                    emptyList(),
                    FilterCriteria(
                        eventNames = listOf("Transfer"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "Transfer",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should filter events based on abi names if passed into filter criteria`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input logs to process
            val logs: List<EventLog> = LOGS_STRINGS

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
                    emptyList(),
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
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should filter events based on contract address if passed into filter criteria`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input block to process
            val logs = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
                    emptyList(),
                    FilterCriteria(
                        contractAddresses = listOf("0x6bee7ddab6c99d5b2af0554eaea484ce18f52631"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "RewardDistributed",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should return empty result if no events for contract address`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input block to process
            val logs = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
                    emptyList(),
                    FilterCriteria(
                        contractAddresses = listOf("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"),
                    ),
                )

            // Expect empty result
            Assertions.assertTrue(result.isEmpty(), "Result should be empty")
        }

        @Test
        fun `should apply multiple filters if multiple are passed in`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input logs to process
            val logs = LOGS_STRINGS

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
                    emptyList(),
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
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should process business events correctly and map to correct one`() {
            val businessEventManager = BusinessEventManager(businessEventFiles)
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input block to process
            val eventsLogs = LOGS_TOKEN_EXCHANGE
            val transferLogs = LOGS_VET_TRANSFER

            // Process the block
            val events =
                indexer.processBlockGenericEvents(
                    eventsLogs,
                    transferLogs,
                    FilterCriteria(
                        vetTransfers = true,
                    ),
                )

            val result = indexer.processOnlyBusinessEvents(events)

            val expectedEventTypes =
                listOf(
                    "Token_FTSwap",
                    "FT_VET_Swap",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.params.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }

        @Test
        fun `should get latest block and process events correctly`() {
            val businessEventManager = mockk<BusinessEventManager>()
            val abiManager = AbiManager(abiFiles)

            every { responseMocker.rollback(any()) } just Runs

            every { businessEventManager.updateCriteriaWithBusinessEvents(any()) } returns
                FilterCriteria()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    setOf(LogType.EVENT, LogType.TRANSFER),
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            // Input block to process
            val logs = LOGS_STRINGS

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
                    "Transfer",
                    "AddressChanged",
                    "AddrChanged",
                    "AddressChanged",
                    "NameChanged",
                    "Transfer",
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
            val result = indexer.processAllEvents(logs)

            // Extract event types from the result
            val eventTypes = result.map { it.eventType }

            println(eventTypes)

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(
                expectedEventTypes,
                eventTypes,
                "Event types do not match expected list",
            )
        }
    }
}
