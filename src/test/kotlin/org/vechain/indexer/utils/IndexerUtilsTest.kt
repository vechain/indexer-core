package org.vechain.indexer.utils

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.vechain.indexer.Status
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.message

internal class IndexerUtilsTest {
    @Test
    fun `does not throw when status is in the allowed set`() {
        val allowedStatuses = enumValues<Status>().toSet()

        allowedStatuses.forEach { status ->
            assertDoesNotThrow { IndexerUtils.ensureStatus(status, allowedStatuses) }
        }
    }

    @Test
    fun `throws when status is not in the allowed set`() {
        val allStatuses = enumValues<Status>()
        val disallowedStatus = allStatuses.first()
        val allowedStatuses = allStatuses.drop(1).toSet()

        expectThrows<IllegalStateException> {
            IndexerUtils.ensureStatus(disallowedStatus, allowedStatuses)
        }
    }

    @Test
    fun `exception message identifies the invalid status`() {
        val status = Status.FAST_SYNCING

        val exception =
            expectThrows<IllegalStateException> {
                IndexerUtils.ensureStatus(status, emptySet())
            }

        expectThat(exception.message.subject).isEqualTo("Invalid status: $status")
    }
}
