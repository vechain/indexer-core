package org.vechain.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.slot
import io.mockk.unmockkConstructor
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.thor.client.LogClient
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.EventCriteria
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull

internal class IndexerFactoryTest {

    private fun baseFactory(): IndexerFactory =
        IndexerFactory()
            .name("child")
            .thorClient(mockk<ThorClient>(relaxed = true))
            .processor(mockk<IndexerProcessor>(relaxed = true))

    private fun parentIndexer(name: String = "parent", startBlock: Long): Indexer =
        mockk<Indexer>(relaxed = true) {
            every { this@mockk.name } returns name
            every { this@mockk.startBlock } returns startBlock
        }

    @Nested
    inner class StartBlockResolution {

        @Test
        fun `defaults to 0 when neither child nor parent is set`() {
            val indexer = baseFactory().build()
            expectThat(indexer.startBlock).isEqualTo(0L)
        }

        @Test
        fun `uses explicit child startBlock when no parent`() {
            val indexer = baseFactory().startBlock(500L).build()
            expectThat(indexer.startBlock).isEqualTo(500L)
        }

        @Test
        fun `inherits parent startBlock when child is unset`() {
            val parent = parentIndexer(startBlock = 100L)
            val indexer = baseFactory().dependsOn(parent).build()
            expectThat(indexer.startBlock).isEqualTo(100L)
        }

        @Test
        fun `keeps matching startBlock when child equals parent`() {
            val parent = parentIndexer(startBlock = 100L)
            val indexer = baseFactory().dependsOn(parent).startBlock(100L).build()
            expectThat(indexer.startBlock).isEqualTo(100L)
        }

        @Test
        fun `keeps child startBlock when later than parent (delayed dependant)`() {
            // Parent starts at 100, child starts at 500: parent runs alone until block 500, then
            // child joins. Runtime's skip path handles the gap; no factory-level override.
            val parent = parentIndexer(startBlock = 100L)
            val indexer = baseFactory().dependsOn(parent).startBlock(500L).build()
            expectThat(indexer.startBlock).isEqualTo(500L)
        }

        @Test
        fun `keeps child startBlock when earlier than parent (pre-dependency work)`() {
            // Child legitimately starts at 100 to do its own work before parent's data becomes
            // relevant at 500. Consumer is responsible for not reading parent's state in [100,
            // 500).
            val parent = parentIndexer(startBlock = 500L)
            val indexer = baseFactory().dependsOn(parent).startBlock(100L).build()
            expectThat(indexer.startBlock).isEqualTo(100L)
        }

        @Test
        fun `throws when explicit child startBlock is negative`() {
            val ex =
                assertThrows<IllegalArgumentException> { baseFactory().startBlock(-5L).build() }
            expectThat(ex.message!!).contains("startBlock must be >= 0")
            expectThat(ex.message!!).contains("-5")
        }
    }

    @Nested
    inner class EventCriteriaSetWiring {

        // mockkConstructor swaps the real LogClient (constructed inside LogsIndexer.init) for a
        // mock so we can intercept fetchEventLogs and capture the criteria the factory wired in.
        // Drives the LogsIndexer through one sync iteration; the empty-logs return short-circuits
        // processBatch into the hasNoLogs branch, advancing the block cursor and exiting the loop.

        @AfterEach
        fun unmock() {
            unmockkConstructor(LogClient::class)
        }

        @Test
        fun `disableEventCriteria sends empty criteriaSet to LogClient`() {
            val captured = slot<List<EventCriteria>?>()
            mockkConstructor(LogClient::class)
            coEvery {
                anyConstructed<LogClient>()
                    .fetchEventLogs(any(), any(), any(), captureNullable(captured))
            } returns emptyList()

            val indexer =
                baseFactory()
                    .abis("test-abis/tokens")
                    .abiEventNames(listOf("Transfer", "TransferSingle", "TransferBatch"))
                    .disableEventCriteria()
                    .build() as LogsIndexer

            runBlocking { indexer.sync(BlockIdentifier(number = 1L, id = "0x01")) }

            expectThat(captured.captured).isNotNull().isEmpty()
        }

        @Test
        fun `default wiring sends auto-derived topic0 criteriaSet to LogClient`() {
            val captured = slot<List<EventCriteria>?>()
            mockkConstructor(LogClient::class)
            coEvery {
                anyConstructed<LogClient>()
                    .fetchEventLogs(any(), any(), any(), captureNullable(captured))
            } returns emptyList()

            val indexer =
                baseFactory()
                    .abis("test-abis/tokens")
                    .abiEventNames(listOf("Transfer", "TransferSingle", "TransferBatch"))
                    .build() as LogsIndexer

            runBlocking { indexer.sync(BlockIdentifier(number = 1L, id = "0x01")) }

            // 5 distinct signatures across the token ABIs; ERC1155 + vip210 each define their own
            // TransferSingle / TransferBatch variants, plus the shared ERC20/721/vip180 Transfer.
            expectThat(captured.captured).isNotNull().hasSize(5)
        }
    }
}
