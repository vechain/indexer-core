package org.vechain.indexer.utils

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.vechain.indexer.Indexer
import org.vechain.indexer.thor.model.Clause
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo

internal class ClauseUtilsTest {

    private fun createMockIndexer(
        name: String,
        clauses: List<Clause>? = null,
    ): Indexer {
        return mockk<Indexer>(relaxed = true) {
            every { this@mockk.name } returns name
            every { getInspectionClauses() } returns clauses
        }
    }

    private fun clause(to: String, data: String): Clause =
        Clause(to = to, value = "0x0", data = data)

    @Nested
    inner class BuildClauseListWithMapping {

        @Test
        fun `empty indexer list returns empty results`() {
            val (clauses, mapping) = ClauseUtils.buildClauseListWithMapping(emptyList())

            expectThat(clauses).isEmpty()
            expectThat(mapping).isEmpty()
        }

        @Test
        fun `returns empty when no indexers have clauses`() {
            val indexer1 = createMockIndexer("indexer1")
            val indexer2 = createMockIndexer("indexer2")

            val (clauses, mapping) =
                ClauseUtils.buildClauseListWithMapping(listOf(indexer1, indexer2))

            expectThat(clauses).isEmpty()
            expectThat(mapping).isEmpty()
        }

        @Test
        fun `single indexer with single clause`() {
            val c = clause("0xAddr1", "0x1111")
            val indexer = createMockIndexer("indexer1", listOf(c))

            val (clauses, mapping) = ClauseUtils.buildClauseListWithMapping(listOf(indexer))

            expectThat(clauses).containsExactly(c)
            expectThat(mapping[indexer]).isEqualTo(listOf(0))
        }

        @Test
        fun `single indexer with multiple clauses`() {
            val c1 = clause("0xAddr1", "0x1111")
            val c2 = clause("0xAddr2", "0x2222")
            val c3 = clause("0xAddr3", "0x3333")
            val indexer = createMockIndexer("indexer1", listOf(c1, c2, c3))

            val (clauses, mapping) = ClauseUtils.buildClauseListWithMapping(listOf(indexer))

            expectThat(clauses).containsExactly(c1, c2, c3)
            expectThat(mapping[indexer]).isEqualTo(listOf(0, 1, 2))
        }

        @Test
        fun `maps each indexer to its clause indices`() {
            val c1 = clause("0xAddr1", "0x1111")
            val c2 = clause("0xAddr2", "0x2222")
            val c3 = clause("0xAddr3", "0x3333")

            val indexer1 = createMockIndexer("indexer1", listOf(c1))
            val indexer2 = createMockIndexer("indexer2", listOf(c2, c3))

            val (clauses, mapping) =
                ClauseUtils.buildClauseListWithMapping(listOf(indexer1, indexer2))

            expectThat(clauses).containsExactly(c1, c2, c3)
            expectThat(mapping[indexer1]).isEqualTo(listOf(0))
            expectThat(mapping[indexer2]).isEqualTo(listOf(1, 2))
        }

        @Test
        fun `deduplicates clauses and maps correctly`() {
            val shared = clause("0xShared", "0xAAAA")
            val unique = clause("0xUnique", "0xBBBB")

            val indexer1 = createMockIndexer("indexer1", listOf(shared))
            val indexer2 = createMockIndexer("indexer2", listOf(shared, unique))

            val (clauses, mapping) =
                ClauseUtils.buildClauseListWithMapping(listOf(indexer1, indexer2))

            expectThat(clauses).containsExactly(shared, unique)
            expectThat(mapping[indexer1]).isEqualTo(listOf(0))
            expectThat(mapping[indexer2]).isEqualTo(listOf(0, 1))
        }

        @Test
        fun `handles mix of indexers with and without clauses`() {
            val c1 = clause("0xAddr1", "0x1111")

            val indexer1 = createMockIndexer("indexer1") // null clauses
            val indexer2 = createMockIndexer("indexer2", listOf(c1))
            val indexer3 = createMockIndexer("indexer3") // null clauses

            val (clauses, mapping) =
                ClauseUtils.buildClauseListWithMapping(listOf(indexer1, indexer2, indexer3))

            expectThat(clauses).containsExactly(c1)
            expectThat(mapping.containsKey(indexer1)).isEqualTo(false)
            expectThat(mapping[indexer2]).isEqualTo(listOf(0))
            expectThat(mapping.containsKey(indexer3)).isEqualTo(false)
        }

        @Test
        fun `indexer with empty clause list is excluded from mapping`() {
            val indexer = createMockIndexer("indexer1", emptyList())

            val (clauses, mapping) = ClauseUtils.buildClauseListWithMapping(listOf(indexer))

            expectThat(clauses).isEmpty()
            expectThat(mapping.containsKey(indexer)).isEqualTo(false)
        }

        @Test
        fun `empty clause list and null clauses are treated the same`() {
            val c1 = clause("0xAddr1", "0x1111")

            val nullIndexer = createMockIndexer("nullClauses") // null clauses
            val emptyIndexer = createMockIndexer("emptyClauses", emptyList())
            val normalIndexer = createMockIndexer("normal", listOf(c1))

            val (clauses, mapping) =
                ClauseUtils.buildClauseListWithMapping(
                    listOf(nullIndexer, emptyIndexer, normalIndexer)
                )

            expectThat(clauses).containsExactly(c1)
            // Both null and empty are excluded from mapping
            expectThat(mapping.containsKey(nullIndexer)).isEqualTo(false)
            expectThat(mapping.containsKey(emptyIndexer)).isEqualTo(false)
            expectThat(mapping[normalIndexer]).isEqualTo(listOf(0))
        }

        @Test
        fun `all indexers share exact same clauses`() {
            val c1 = clause("0xAddr1", "0x1111")
            val c2 = clause("0xAddr2", "0x2222")

            val indexer1 = createMockIndexer("indexer1", listOf(c1, c2))
            val indexer2 = createMockIndexer("indexer2", listOf(c1, c2))
            val indexer3 = createMockIndexer("indexer3", listOf(c1, c2))

            val (clauses, mapping) =
                ClauseUtils.buildClauseListWithMapping(listOf(indexer1, indexer2, indexer3))

            // Only 2 unique clauses
            expectThat(clauses).containsExactly(c1, c2)
            // All indexers map to the same indices
            expectThat(mapping[indexer1]).isEqualTo(listOf(0, 1))
            expectThat(mapping[indexer2]).isEqualTo(listOf(0, 1))
            expectThat(mapping[indexer3]).isEqualTo(listOf(0, 1))
        }

        @Test
        fun `clause ordering is preserved by insertion order`() {
            val c1 = clause("0xAddr1", "0x1111")
            val c2 = clause("0xAddr2", "0x2222")
            val c3 = clause("0xAddr3", "0x3333")
            val c4 = clause("0xAddr4", "0x4444")

            val indexer1 = createMockIndexer("indexer1", listOf(c3, c1))
            val indexer2 = createMockIndexer("indexer2", listOf(c4, c2))

            val (clauses, _) = ClauseUtils.buildClauseListWithMapping(listOf(indexer1, indexer2))

            // Order follows first-seen from indexer1 then indexer2
            expectThat(clauses).containsExactly(c3, c1, c4, c2)
        }

        @Test
        fun `large number of indexers and clauses`() {
            val allClauses = (0 until 50).map { clause("0xAddr$it", "0x${it}") }
            val indexers =
                (0 until 10).map { i ->
                    val subset = allClauses.subList(i * 5, i * 5 + 5)
                    createMockIndexer("indexer$i", subset)
                }

            val (clauses, mapping) = ClauseUtils.buildClauseListWithMapping(indexers)

            expectThat(clauses).hasSize(50)
            // Each indexer maps to 5 consecutive indices
            indexers.forEachIndexed { i, indexer ->
                expectThat(mapping[indexer]).isEqualTo((i * 5 until i * 5 + 5).toList())
            }
        }

        @Test
        fun `duplicate clauses within same indexer`() {
            val c1 = clause("0xAddr1", "0x1111")

            val indexer = createMockIndexer("indexer1", listOf(c1, c1, c1))

            val (clauses, mapping) = ClauseUtils.buildClauseListWithMapping(listOf(indexer))

            // Only one unique clause
            expectThat(clauses).containsExactly(c1)
            // All three references point to index 0
            expectThat(mapping[indexer]).isEqualTo(listOf(0, 0, 0))
        }
    }
}
