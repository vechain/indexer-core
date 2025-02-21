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
import org.vechain.indexer.fixtures.LogFixtures.LOGS_B3TR_ACTION
import org.vechain.indexer.fixtures.LogFixtures.LOGS_STRINGS
import org.vechain.indexer.helpers.FileLoaderHelper
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.EventMeta
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

    private val getBlockNumberSlot = slot<Long>()
    private val processLogsSlot = slot<List<EventLog>>()

    @BeforeEach
    fun setup() {
        every { responseMocker.rollback(any()) } just Runs

        indexer =
            LogIndexerMock(
                responseMocker,
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
        fun `Start indexer should initialise and status should be set to SYNCING`() =
            runBlocking {
                val indexerIterationsNumber = 1L

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers
                    {
                        buildBlock(getBlockNumberSlot.captured)
                    }
                coEvery { thorClient.getBestBlock() } returns buildBlock(10000L)
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

                every { responseMocker.getLastSyncedBlock() } returns
                    BlockIdentifier(
                        number = 100L,
                        id = "0x100",
                    ) andThen
                    BlockIdentifier(number = 99L, id = "0x99")
                every { responseMocker.processLogs(any()) } just Runs
                every { responseMocker.processBlock(any()) } just Runs

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
                coEvery { thorClient.getBestBlock() } returns buildBlock(10000L)
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }
                every { responseMocker.getLastSyncedBlock() } returns null
                every { responseMocker.processLogs(any()) } just Runs
                every { responseMocker.processBlock(any()) } just Runs

                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join()

                expect {
                    // Verify the status is SYNCING
                    that(indexer.status).isEqualTo(Status.SYNCING)
                }
            }

        @Test
        fun `Start indexer should process logs`() =
            runBlocking {
                val indexerIterationsNumber = 1L

                // Mock fetching event logs
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

                coEvery { thorClient.getBestBlock() } returns buildBlock(100000L)
                coEvery { thorClient.getBlock(any()) } returns buildBlock(100000L)

                // Mock last synced block (so it starts from the beginning)
                coEvery { responseMocker.getLastSyncedBlock() } returns null

                // Mock the processLogs() call
                coEvery { responseMocker.processLogs(any()) } just Runs

                coEvery { responseMocker.processBlock(any()) } just Runs

                // Run the indexer
                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join() // Wait for completion

                expect {
                    // Verify the status is SYNCING after start
                    that(indexer.status).isEqualTo(Status.SYNCING)

                    // Ensure logs were processed at least once
                    verify(exactly = 1) { responseMocker.processLogs(any()) }
                }
            }

        @Test
        fun `Indexer should process at a block level when getting near best block`() =
            runBlocking {
                val indexerIterationsNumber = 1L

                // Mock fetching event logs
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

                coEvery { thorClient.getBestBlock() } returns buildBlock(1001L)
                coEvery { thorClient.getBlock(any()) } returns buildBlock(100000L)

                // Mock last synced block (so it starts from the beginning)
                coEvery { responseMocker.getLastSyncedBlock() } returns null

                // Mock the processLogs() call
                coEvery { responseMocker.processLogs(any()) } just Runs

                coEvery { responseMocker.processBlock(any()) } just Runs

                // Run the indexer
                val job = launch { indexer.start(indexerIterationsNumber) }
                job.join() // Wait for completion

                expect {
                    // Verify the status is SYNCING after start
                    that(indexer.status).isEqualTo(Status.SYNCING)

                    // Ensure logs were processed at least once
                    verify(exactly = 1) { responseMocker.processLogs(any()) }

                    // Ensure processBlock was called
                    verify(exactly = 1) { responseMocker.processBlock(any()) }
                }
            }

        @Test
        fun `Indexer should switch to FULLY_SNCED when fully synced`() =
            runBlocking {
                val blockNotFound = BlockIdentifier(number = 99L, id = "0x99")
                // Mock fetching event logs
                coEvery { thorClient.getEventLogs(any()) } coAnswers { LOGS_STRINGS }

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
                // Mock the processLogs() call
                coEvery { responseMocker.processLogs(any()) } just Runs
                coEvery { responseMocker.processBlock(any()) } just Runs

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
                coEvery { responseMocker.processLogs(any()) } just Runs

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
                coEvery { thorClient.getBestBlock() } coAnswers { buildBlock(99L) }
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
                coEvery { responseMocker.processLogs(any()) } just Runs

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

                coEvery { thorClient.getBlock(capture(getBlockNumberSlot)) } coAnswers {
                    buildBlock(getBlockNumberSlot.captured)
                }

                coEvery { thorClient.getBestBlock() } returns buildBlock(unknownExceptionBlock + 1001)

                coEvery { responseMocker.getLastSyncedBlock() } returns
                    null andThen null andThen lastSyncedBlock

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

                coEvery { responseMocker.processLogs(capture(processLogsSlot)) } answers {
                    if (processLogsSlot.captured.any { it.meta.blockNumber == unknownExceptionBlock }) {
                        throw Exception("Unknown exception")
                    }
                }

                val job = launch { indexer.start(1) }
                job.join()

                expect {
                    // Ensure the indexer stopped at 0 as this is the first block in the batch where exception was thrown
                    that(indexer.currentBlockNumber).isEqualTo(0)

                    // Ensure the status switched to ERROR
                    that(indexer.status).isEqualTo(Status.ERROR)
                }
            }
    }

    @Nested
    inner class ProcessEvents {
        @Test
        fun `should get latest business events and generic events`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )
            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result = indexer.processAllEvents(logs)

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should return all business events and all generic events if remove duplicates is false `() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result = indexer.processAllEvents(logs, FilterCriteria(removeDuplicates = false))

            val expectedEventTypes =
                listOf(
                    "RewardDistributed",
                    "Transfer",
                    "Transfer",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should return all VET transfers as events if vet transfers is set to true`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
                    FilterCriteria(
                        vetTransfers = true,
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should filter events based on names if event names to process was passed into filter criteria`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input logs to process
            val logs: List<EventLog> = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
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
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should filter events based on abi names if passed into filter criteria`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input logs to process
            val logs: List<EventLog> = LOGS_STRINGS

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
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
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should filter events based on contract address if passed into filter criteria`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input block to process
            val logs = LOGS_B3TR_ACTION

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
                    FilterCriteria(
                        contractAddresses = listOf("0x6bee7ddab6c99d5b2af0554eaea484ce18f52631"),
                    ),
                )

            val expectedEventTypes =
                listOf(
                    "Transfer",
                    "B3TR_ActionReward",
                )

            // Extract event types from the result
            val eventTypes = result.map { it.second.getEventType() }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should apply multiple filters if multiple are passed in`() {
            val businessEventManager = BusinessEventManager()
            val abiManager = AbiManager()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )

            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")
            val fileStreamsBusiness = FileLoaderHelper.loadJsonFilesFromPath("business-events")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)
            // Load business events
            businessEventManager.loadBusinessEvents(fileStreamsBusiness)

            // Input logs to process
            val logs = LOGS_STRINGS

            // Process the block
            val result =
                indexer.processAllEvents(
                    logs,
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
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }

        @Test
        fun `should get latest block and process events correctly`() {
            val businessEventManager = mockk<BusinessEventManager>()
            val abiManager = AbiManager()
            every { businessEventManager.updateCriteriaWithBusinessEvents(any()) } returns FilterCriteria()

            // Create the indexer with mocked dependencies
            val indexer =
                LogIndexerMock(
                    responseMocker,
                    blockBatchSize,
                    logFetchLimit,
                    thorClient,
                    abiManager,
                    businessEventManager,
                )
            val fileStreamsAbis = FileLoaderHelper.loadJsonFilesFromPath("test-abis")

            // Load ABIs required for decoding
            abiManager.loadAbis(fileStreamsAbis)

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
            val result = indexer.processAllEvents(logs)

            // Extract event types from the result
            val eventTypes = result.map { it.first.eventType }

            // Assert that all expected events are present and in the correct order
            Assertions.assertEquals(expectedEventTypes, eventTypes, "Event types do not match expected list")
        }
    }
}
