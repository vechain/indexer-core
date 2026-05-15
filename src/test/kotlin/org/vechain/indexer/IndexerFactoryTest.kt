package org.vechain.indexer

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.thor.client.ThorClient
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isEqualTo

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
            // relevant at 500. Consumer is responsible for not reading parent's state in [100, 500).
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
}
