package org.vechain.indexer.thor.model

import kotlin.test.Test
import kotlin.test.assertFailsWith
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo

class BlockRevisionTest {

    @Test
    fun `parse accepts keywords case-insensitively`() {
        expectThat(BlockRevision.parse("best")).isEqualTo(BlockRevision.Keyword.BEST)
        expectThat(BlockRevision.parse("FINALIZED")).isEqualTo(BlockRevision.Keyword.FINALIZED)
        expectThat(BlockRevision.parse("  justified  ")).isEqualTo(BlockRevision.Keyword.JUSTIFIED)
    }

    @Test
    fun `parse accepts decimal block numbers`() {
        val revision = BlockRevision.parse("123")
        expectThat(revision).isA<BlockRevision.Number>()
        expectThat((revision as BlockRevision.Number).number).isEqualTo(123L)
        expectThat(revision.value).isEqualTo("123")
    }

    @Test
    fun `parse accepts 32-byte block id and normalizes to lowercase`() {
        val id = "0x" + "A1".repeat(32)
        val revision = BlockRevision.parse(id)
        expectThat(revision).isA<BlockRevision.Id>()
        expectThat(revision.value).isEqualTo(id.lowercase())
    }

    @Test
    fun `parse rejects invalid revisions`() {
        assertFailsWith<IllegalArgumentException> { BlockRevision.parse("latest") }
        assertFailsWith<IllegalArgumentException> { BlockRevision.parse("-1") }
        assertFailsWith<IllegalArgumentException> { BlockRevision.parse("0x1234") }
        assertFailsWith<IllegalArgumentException> { BlockRevision.parse("0x" + "00".repeat(31)) }
    }

    @Test
    fun `constructors validate invariants`() {
        assertFailsWith<IllegalArgumentException> { BlockRevision.Number(-1) }
        assertFailsWith<IllegalArgumentException> { BlockRevision.Id("0x1234") }
    }
}
