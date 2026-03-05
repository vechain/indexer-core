package org.vechain.indexer.utils

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.Indexer
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo

internal class IndexerOrderUtilsTest {

    private fun createMockIndexer(
        name: String,
        dependsOn: Indexer? = null,
        currentBlock: Long = 0L,
    ): Indexer {
        return mockk<Indexer>(relaxed = true) {
            every { this@mockk.name } returns name
            every { this@mockk.dependsOn } returns dependsOn
            every { this@mockk.getCurrentBlockNumber() } returns currentBlock
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
        fun `should return separate groups for multiple indexers with no dependencies`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")
            val indexer3 = createMockIndexer("indexer3")

            val result = IndexerOrderUtils.topologicalOrder(listOf(indexer1, indexer2, indexer3))

            expectThat(result.size).isEqualTo(3)
            expectThat(result[0]).containsExactly(indexer1)
            expectThat(result[1]).containsExactly(indexer2)
            expectThat(result[2]).containsExactly(indexer3)
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

            // Two connected components: {1,3,5,6} and {2,4}
            expectThat(result.size).isEqualTo(2)
            expectThat(result[0]).containsExactly(indexer1, indexer3, indexer5, indexer6)
            expectThat(result[1]).containsExactly(indexer2, indexer4)
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

            // Three groups: {1,3} chain, {2} independent, {4} independent
            expectThat(result.size).isEqualTo(3)
            expectThat(result[0]).containsExactly(indexer1, indexer3)
            expectThat(result[1]).containsExactly(indexer2)
            expectThat(result[2]).containsExactly(indexer4)
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

    @Nested
    inner class DependencySort {

        @Test
        fun `should return empty list for empty input`() {
            val result = IndexerOrderUtils.dependencySort(emptyList())
            expectThat(result).isEmpty()
        }

        @Test
        fun `should return single indexer unchanged`() {
            val indexer1 = createMockIndexer("indexer1")
            val result = IndexerOrderUtils.dependencySort(listOf(indexer1))
            expectThat(result).containsExactly(indexer1)
        }

        @Test
        fun `should order dependencies before dependents`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            val indexer3 = createMockIndexer("indexer3", dependsOn = indexer2)

            val result = IndexerOrderUtils.dependencySort(listOf(indexer3, indexer2, indexer1))
            expectThat(result).containsExactly(indexer1, indexer2, indexer3)
        }

        @Test
        fun `should detect circular dependency`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2", dependsOn = indexer1)
            every { indexer1.dependsOn } returns indexer2

            assertThrows<IllegalStateException> {
                IndexerOrderUtils.dependencySort(listOf(indexer1, indexer2))
            }
        }

        @Test
        fun `should reject missing dependency`() {
            val external = createMockIndexer("external")
            val indexer1 = createMockIndexer("indexer1", dependsOn = external)

            assertThrows<IllegalArgumentException> {
                IndexerOrderUtils.dependencySort(listOf(indexer1))
            }
        }
    }

    @Nested
    inner class ConnectedComponents {

        @Test
        fun `should assign each independent indexer its own component`() {
            val a = createMockIndexer("a")
            val b = createMockIndexer("b")
            val c = createMockIndexer("c")

            val result = IndexerOrderUtils.connectedComponents(listOf(a, b, c))

            val roots = result.values.toSet()
            expectThat(roots).hasSize(3)
        }

        @Test
        fun `should group dependency chain into one component`() {
            val a = createMockIndexer("a")
            val b = createMockIndexer("b", dependsOn = a)
            val c = createMockIndexer("c", dependsOn = b)

            val result = IndexerOrderUtils.connectedComponents(listOf(a, b, c))

            expectThat(result.getValue(a)).isEqualTo(result.getValue(b))
            expectThat(result.getValue(b)).isEqualTo(result.getValue(c))
        }

        @Test
        fun `should separate independent chains`() {
            val a = createMockIndexer("a")
            val b = createMockIndexer("b", dependsOn = a)
            val c = createMockIndexer("c")
            val d = createMockIndexer("d", dependsOn = c)

            val result = IndexerOrderUtils.connectedComponents(listOf(a, b, c, d))

            expectThat(result.getValue(a)).isEqualTo(result.getValue(b))
            expectThat(result.getValue(c)).isEqualTo(result.getValue(d))
            expectThat(result.getValue(a)).isNotEqualTo(result.getValue(c))
        }

        @Test
        fun `should group diamond pattern into one component`() {
            val root = createMockIndexer("root")
            val left = createMockIndexer("left", dependsOn = root)
            val right = createMockIndexer("right", dependsOn = root)
            val bottom = createMockIndexer("bottom", dependsOn = left)

            val result = IndexerOrderUtils.connectedComponents(listOf(root, left, right, bottom))

            val roots = result.values.toSet()
            expectThat(roots).hasSize(1)
        }
    }

    @Nested
    inner class ProximityGroups {

        @Test
        fun `empty list returns empty result`() {
            val result = IndexerOrderUtils.proximityGroups(emptyList(), 100)
            expectThat(result).isEmpty()
        }

        @Test
        fun `single indexer returns single group`() {
            val indexer = createMockIndexer("a", currentBlock = 50)
            val result = IndexerOrderUtils.proximityGroups(listOf(indexer), 100)

            expectThat(result).hasSize(1)
            expectThat(result[0]).containsExactly(indexer)
        }

        @Test
        fun `all within threshold returns single group`() {
            val a = createMockIndexer("a", currentBlock = 0)
            val b = createMockIndexer("b", currentBlock = 50)
            val c = createMockIndexer("c", currentBlock = 100)

            val result = IndexerOrderUtils.proximityGroups(listOf(a, b, c), 100)

            expectThat(result).hasSize(1)
        }

        @Test
        fun `gap exceeding threshold creates separate groups`() {
            val a = createMockIndexer("a", currentBlock = 0)
            val b = createMockIndexer("b", currentBlock = 50)
            val c = createMockIndexer("c", currentBlock = 1000)
            val d = createMockIndexer("d", currentBlock = 1050)

            val result = IndexerOrderUtils.proximityGroups(listOf(a, b, c, d), 100)

            expectThat(result).hasSize(2)
            expectThat(result[0]).containsExactly(a, b)
            expectThat(result[1]).containsExactly(c, d)
        }

        @Test
        fun `cross-group dependency chain extracted leaves single standalone group`() {
            // parent at block 0, child at block 1000 — initially two proximity groups
            // Chain extraction removes both from their original groups (leaving them empty),
            // so the chain becomes the sole standalone group
            val parent = createMockIndexer("parent", currentBlock = 0)
            val child = createMockIndexer("child", dependsOn = parent, currentBlock = 1000)

            val result = IndexerOrderUtils.proximityGroups(listOf(parent, child), 100)

            // Both original groups emptied after extraction; chain is the only group
            expectThat(result).hasSize(1)
            // Should be topologically ordered: parent before child
            expectThat(result[0]).containsExactly(parent, child)
        }

        @Test
        fun `cross-group dependency with no close group becomes standalone`() {
            val a = createMockIndexer("a", currentBlock = 0)
            val b = createMockIndexer("b", currentBlock = 50)
            val parent = createMockIndexer("parent", currentBlock = 5000)
            val child = createMockIndexer("child", dependsOn = parent, currentBlock = 10000)

            val result = IndexerOrderUtils.proximityGroups(listOf(a, b, parent, child), 100)

            // a,b in one group; parent+child chain: gap(5000) to group a,b (0) = 5000 > 100
            // so chain becomes standalone
            expectThat(result).hasSize(2)
            expectThat(result[0]).containsExactly(a, b)
            expectThat(result[1]).containsExactly(parent, child)
        }

        @Test
        fun `downstream dependents are extracted along with their chain`() {
            val root = createMockIndexer("root", currentBlock = 0)
            val mid = createMockIndexer("mid", dependsOn = root, currentBlock = 5000)
            val leaf = createMockIndexer("leaf", dependsOn = mid, currentBlock = 5010)

            val result = IndexerOrderUtils.proximityGroups(listOf(root, mid, leaf), 100)

            // All three form a dependency chain and get extracted together
            expectThat(result).hasSize(1)
            expectThat(result[0]).containsExactly(root, mid, leaf)
        }

        @Test
        fun `groups are topologically ordered internally`() {
            val parent = createMockIndexer("parent", currentBlock = 10)
            val child = createMockIndexer("child", dependsOn = parent, currentBlock = 20)

            val result = IndexerOrderUtils.proximityGroups(listOf(child, parent), 100)

            expectThat(result).hasSize(1)
            expectThat(result[0]).containsExactly(parent, child)
        }

        @Test
        fun `three separate groups with no cross-group deps stay separate`() {
            val a = createMockIndexer("a", currentBlock = 0)
            val b = createMockIndexer("b", currentBlock = 1000)
            val c = createMockIndexer("c", currentBlock = 2000)

            val result = IndexerOrderUtils.proximityGroups(listOf(a, b, c), 100)

            expectThat(result).hasSize(3)
            expectThat(result[0]).containsExactly(a)
            expectThat(result[1]).containsExactly(b)
            expectThat(result[2]).containsExactly(c)
        }

        @Test
        fun `cross-group chain merges into closest group within threshold`() {
            val a = createMockIndexer("a", currentBlock = 0)
            val b = createMockIndexer("b", currentBlock = 50)
            val c = createMockIndexer("c", currentBlock = 2000)
            // parent in first group, child in second — chain min is 10
            val parent = createMockIndexer("parent", currentBlock = 10)
            val child = createMockIndexer("child", dependsOn = parent, currentBlock = 2010)

            val result = IndexerOrderUtils.proximityGroups(listOf(a, b, c, parent, child), 100)

            // Chain (parent@10, child@2010) min=10, closest group is {a@0,b@50} with min=0
            // gap = |10-0| = 10 <= 100, so merge into that group
            expectThat(result).hasSize(2)
            // First group: a, b, parent, child (topologically ordered)
            val firstGroupNames = result[0].map { it.name }
            expectThat(firstGroupNames.indexOf("parent") < firstGroupNames.indexOf("child"))
                .isEqualTo(true)
            // Second group: c
            expectThat(result[1]).containsExactly(c)
        }
    }
}
