package org.vechain.indexer

import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.BlockTestBuilder.Companion.buildBlock
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockIdentifier
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.containsExactly
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo

@ExtendWith(MockKExtension::class)
internal class BlockCoordinatorTest {

    @MockK private lateinit var thorClient: ThorClient

    @BeforeEach
    fun setup() {
        MockKAnnotations.init(this)
    }

    @Nested
    inner class ExecutionOrder {

        @Test
        fun `coordinator respects dependency order`() = runTest {
            val processed = mutableListOf<Pair<String, Long>>()
            val indexerB = recordingIndexer("B", processed)
            val indexerA = recordingIndexer("A", processed, dependantIndexers = setOf(indexerB))

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val number = it.invocation.args[0] as Long
                    buildBlock(number)
                }
            coEvery { thorClient.getBestBlock() } answers { buildBlock(1_000L) }

            val indexerCoordinator = IndexerCoordinator(thorClient, batchSize = 2)
            indexerCoordinator.run(
                indexers = listOf(indexerA, indexerB),
                dependencies = mapOf(indexerA to setOf(indexerB)),
                maxBlocks = 3,
            )

            expectThat(processed)
                .containsExactly(
                    "B" to 0L,
                    "A" to 0L,
                    "B" to 1L,
                    "A" to 1L,
                    "B" to 2L,
                    "A" to 2L,
                )
        }

        @Test
        fun `block coordinator launch honours dependencies`() = runTest {
            val processed = mutableListOf<Pair<String, Long>>()
            val indexerB = recordingIndexer("B", processed)
            val indexerA = recordingIndexer("A", processed, dependantIndexers = setOf(indexerB))

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val number = it.invocation.args[0] as Long
                    buildBlock(number)
                }
            coEvery { thorClient.getBestBlock() } answers { buildBlock(1_000L) }

            val job =
                IndexerCoordinator.launch(
                    scope = this,
                    thorClient = thorClient,
                    indexers = listOf(indexerA, indexerB),
                    blockBatchSize = 2,
                    maxBlocks = 3,
                )

            job.join()

            expectThat(processed)
                .containsExactly(
                    "B" to 0L,
                    "A" to 0L,
                    "B" to 1L,
                    "A" to 1L,
                    "B" to 2L,
                    "A" to 2L,
                )
        }

        @Test
        fun `detects cycles`() = runTest {
            val indexerA = recordingIndexer("A")
            val indexerB = recordingIndexer("B")

            val indexerCoordinator = IndexerCoordinator(thorClient)

            expectThrows<IllegalArgumentException> {
                indexerCoordinator.run(
                    indexers = listOf(indexerA, indexerB),
                    dependencies =
                        mapOf(
                            indexerA to setOf(indexerB),
                            indexerB to setOf(indexerA),
                        ),
                    maxBlocks = 1,
                )
            }
        }

        @Test
        fun `logs indexer with dependencies skips fast sync`() = runTest {
            val processed = mutableListOf<Pair<String, Long>>()
            val prereq = recordingIndexer("Prereq", processed)
            val logs = recordingLogsIndexer("Logs", processed, dependsOn = setOf(prereq))

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val number = it.invocation.args[0] as Long
                    buildBlock(number)
                }
            coEvery { thorClient.getBestBlock() } answers { buildBlock(1000L) }
            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(5L)

            val indexerCoordinator = IndexerCoordinator(thorClient)
            indexerCoordinator.run(indexers = listOf(logs, prereq), maxBlocks = 1)

            expectThat(logs.isFastSyncEnabled()).isEqualTo(false)
            expectThat(logs.syncCalls).isEmpty()
        }

        @Test
        fun `logs indexer with dependants skips fast sync`() = runTest {
            val processed = mutableListOf<Pair<String, Long>>()
            val logs = recordingLogsIndexer("Logs", processed)
            val dependant = recordingIndexer("A", processed, dependantIndexers = setOf(logs))

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val number = it.invocation.args[0] as Long
                    buildBlock(number)
                }
            coEvery { thorClient.getBestBlock() } answers { buildBlock(1000L) }
            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(5L)

            val indexerCoordinator = IndexerCoordinator(thorClient)
            indexerCoordinator.run(indexers = listOf(logs, dependant), maxBlocks = 1)

            expectThat(logs.isFastSyncEnabled()).isEqualTo(false)
            expectThat(logs.syncCalls).isEmpty()
        }

        @Test
        fun `standalone logs indexer performs fast sync before coordination`() = runTest {
            val processed = mutableListOf<Pair<String, Long>>()
            val logs = recordingLogsIndexer("Logs", processed)

            coEvery { thorClient.getBlock(any()) } answers
                {
                    val number = it.invocation.args[0] as Long
                    buildBlock(number)
                }
            coEvery { thorClient.getBestBlock() } answers { buildBlock(1000L) }
            coEvery { thorClient.getFinalizedBlock() } returns buildBlock(5L)

            val indexerCoordinator = IndexerCoordinator(thorClient)
            indexerCoordinator.run(indexers = listOf(logs), maxBlocks = 1)

            expectThat(logs.isFastSyncEnabled()).isEqualTo(true)
            expectThat(logs.syncCalls).containsExactly(5L)
        }
    }

    private fun recordingIndexer(
        name: String,
        processed: MutableList<Pair<String, Long>> = mutableListOf(),
        dependantIndexers: Set<BlockIndexer> = emptySet(),
    ): BlockIndexer {
        val processor = RecordingProcessor(name, processed)
        return RecordingIndexer(
            name = name,
            thorClient = thorClient,
            processor = processor,
            dependantIndexers = dependantIndexers,
        )
    }

    private fun recordingLogsIndexer(
        name: String,
        processed: MutableList<Pair<String, Long>> = mutableListOf(),
        dependsOn: Set<BlockIndexer> = emptySet(),
    ): RecordingLogsIndexer {
        val processor = RecordingProcessor(name, processed)
        return RecordingLogsIndexer(
            name = name,
            thorClient = thorClient,
            processor = processor,
            dependsOn = dependsOn,
        )
    }

    private class RecordingProcessor(
        private val name: String,
        private val processed: MutableList<Pair<String, Long>>,
    ) : IndexerProcessor {
        override fun getLastSyncedBlock(): BlockIdentifier? = null

        override fun rollback(blockNumber: Long) {}

        override fun process(entry: IndexingResult) {
            processed += name to entry.latestBlockNumber()
        }
    }

    private class RecordingIndexer(
        name: String,
        thorClient: ThorClient,
        processor: IndexerProcessor,
        dependantIndexers: Set<BlockIndexer>,
    ) :
        BlockIndexer(
            name = name,
            thorClient = thorClient,
            processor = processor,
            startBlock = 0L,
            syncLoggerInterval = Long.MAX_VALUE,
            eventProcessor = null,
            inspectionClauses = null,
            pruner = null,
            prunerInterval = Long.MAX_VALUE,
            dependantIndexers = dependantIndexers,
            batchSize = 1,
        )
}

private class RecordingLogsIndexer(
    name: String,
    thorClient: ThorClient,
    processor: IndexerProcessor,
    dependsOn: Set<BlockIndexer>,
) :
    LogsIndexer(
        name = name,
        thorClient = thorClient,
        processor = processor,
        startBlock = 0L,
        syncLoggerInterval = Long.MAX_VALUE,
        excludeVetTransfers = false,
        blockBatchSize = 1L,
        logFetchLimit = 1L,
        eventCriteriaSet = null,
        transferCriteriaSet = null,
        eventProcessor = null,
        pruner = null,
        prunerInterval = Long.MAX_VALUE,
        dependantIndexers = dependsOn,
    ) {
    val syncCalls = mutableListOf<Long>()

    override suspend fun sync(toBlock: Long) {
        syncCalls += toBlock
        currentBlockNumber = toBlock + 1
    }
}
