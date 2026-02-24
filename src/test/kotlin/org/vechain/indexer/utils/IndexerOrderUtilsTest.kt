package org.vechain.indexer.utils

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.Indexer
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo

internal class IndexerOrderUtilsTest {

    private fun createMockIndexer(name: String, dependsOn: Indexer? = null): Indexer {
        return mockk<Indexer>(relaxed = true) {
            every { this@mockk.name } returns name
            every { this@mockk.dependsOn } returns dependsOn
        }
    }

    @Nested
    inner class TopologicalOrder {

        @Test
        fun `should return empty list when given empty list`() {
            val result = IndexerOrderUtils.topologicalOrder(emptyList())

            expectThat(result).isEmpty()
        }

        @Test
        fun `should return single group for indexer with no dependencies`() {
            val indexer1 = createMockIndexer("indexer1")

            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer1))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1)
        }

        @Test
        fun `should return single group for multiple indexers with no dependencies`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")
            val indexer3 = createMockIndexer("indexer3")

            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3)
        }

        @Test
        fun `should return one group for simple linear dependency`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)

            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2)
        }

        @Test
        fun `should return one group for chain of dependencies`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer2)

            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3)
        }

        @Test
        fun `should group independent indexers with same dependency in same group`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer1)

            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3)
        }

        @Test
        fun `should handle complex dependency tree with multiple levels`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer1)
            val indexer4 = createMockIndexer("indexer4", dependsOn = indexer2)
            val indexer5 = createMockIndexer("indexer5", dependsOn = indexer3)
            val indexer6 = createMockIndexer("indexer6", dependsOn = indexer3)

            val result =
                IndexerOrderUtils.topologicalOrder(
                    listOf(indexer1, indexer2, indexer3, indexer4, indexer5, indexer6)
                )

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0])
                .containsExactly(indexer1, indexer2, indexer3, indexer4, indexer5, indexer6)
        }

        @Test
        fun `should handle diamond dependency pattern`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer1)
            val indexer4 = createMockIndexer("indexer4", dependsOn = indexer2)

            val result =
                IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3, indexer4))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3, indexer4)
        }

        @Test
        fun `should work regardless of input order`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer2)

            // Pass in reverse order
            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer3, indexer2, indexer1))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3)
        }

        @Test
        fun `should throw exception for circular dependency - self reference`() {
            val indexer1 = createMockIndexer("indexer1")
            every { indexer1.dependsOn } returns indexer1

            val exception =
                assertThrows<IllegalStateException> {
                    IndexerOrderUtils.topologicalOrder(listOf(indexer1))
                }

            expectThat(exception.message)
                .isEqualTo("Circular dependency detected involving indexer indexer1")
        }

        @Test
        fun `should throw exception for circular dependency - two indexers`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            every { indexer1.dependsOn } returns indexer2

            val exception =
                assertThrows<IllegalStateException> {
                    IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2))
                }

            // The error can be for either indexer depending on visit order
            expectThat(exception.message?.contains("Circular dependency detected")).isEqualTo(true)
        }

        @Test
        fun `should throw exception for circular dependency - three indexers`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer2)
            every { indexer1.dependsOn } returns indexer3

            val exception =
                assertThrows<IllegalStateException> {
                    IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3))
                }

            // The error can be for any indexer depending on visit order
            expectThat(exception.message?.contains("Circular dependency detected")).isEqualTo(true)
        }

        @Test
        fun `should throw exception when dependency is not in provided list`() {
            val externalIndexer = createMockIndexer("external")
            val indexer1 = createMockIndexer("indexer1", dependsOn = externalIndexer)

            val exception =
                assertThrows<IllegalArgumentException> {
                    IndexerOrderUtils.topologicalOrder(listOf(indexer1))
                }

            expectThat(exception.message)
                .isEqualTo("Dependency external for indexer1 is not part of the provided indexers")
        }

        @Test
        fun `should handle mixed independent and dependent indexers`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer1)
            val indexer4 = createMockIndexer("indexer4")

            val result =
                IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3, indexer4))

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3, indexer4)
        }

        @Test
        fun `should handle long dependency chain`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer2)
            val indexer4 = createMockIndexer("indexer4", dependsOn = indexer3)
            val indexer5 = createMockIndexer("indexer5", dependsOn = indexer4)

            val result =
                IndexerOrderUtils.topologicalOrder(
                    listOf(indexer1, indexer2, indexer3, indexer4, indexer5)
                )

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]).containsExactly(indexer1, indexer2, indexer3, indexer4, indexer5)
        }

        @Test
        fun `should handle wide dependency tree`() {
            val root = createMockIndexer("root")
            val child1 = createMockIndexer("child1", dependsOn = root)
            val child2 = createMockIndexer("child2", dependsOn = root)
            val child3 = createMockIndexer("child3", dependsOn = root)
            val child4 = createMockIndexer("child4", dependsOn = root)
            val grandchild1 = createMockIndexer("grandchild1", dependsOn = child1)
            val grandchild2 = createMockIndexer("grandchild2", dependsOn = child2)

            val result =
                IndexerOrderUtils.topologicalOrder(
                    listOf(root, child1, child2, child3, child4, grandchild1, grandchild2)
                )

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0])
                .containsExactly(root, child1, child2, child3, child4, grandchild1, grandchild2)
        }
    }
}
