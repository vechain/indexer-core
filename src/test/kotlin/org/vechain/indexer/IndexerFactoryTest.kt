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
        fun `overrides child startBlock to parent's when child is later`() {
            // Pulling the child back to the parent keeps the dependency component in lockstep.
            val parent = parentIndexer(startBlock = 100L)
            val indexer = baseFactory().dependsOn(parent).startBlock(500L).build()
            expectThat(indexer.startBlock).isEqualTo(100L)
        }

        @Test
        fun `throws when child startBlock is earlier than parent's`() {
            val parent = parentIndexer(name = "p", startBlock = 500L)
            val ex =
                assertThrows<IllegalArgumentException> {
                    baseFactory().dependsOn(parent).startBlock(100L).build()
                }
            expectThat(ex.message!!).contains("cannot start before its parent")
        }
    }
}
