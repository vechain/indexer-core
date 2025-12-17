package org.vechain.indexer.thor.model

/**
 * A block "revision" accepted by Thorest endpoints, which can be:
 * - a block number (decimal)
 * - a 32-byte block id (0x + 64 hex chars)
 * - a keyword: best, finalized, justified
 */
sealed interface BlockRevision {
    /** String form used in the request path segment (e.g. `best`, `123`, or `0x...`). */
    val value: String

    data class Number(
        val number: Long,
    ) : BlockRevision {
        init {
            require(number >= 0) { "Block number must be >= 0" }
        }

        override val value: String = number.toString()
    }

    data class Id(
        override val value: String,
    ) : BlockRevision {
        init {
            require(BLOCK_ID_REGEX.matches(value)) { "Block id must match 0x + 64 hex chars" }
        }
    }

    enum class Keyword(
        override val value: String,
    ) : BlockRevision {
        BEST("best"),
        FINALIZED("finalized"),
        JUSTIFIED("justified"),
    }

    companion object {
        private val BLOCK_ID_REGEX = Regex("^0x[0-9a-fA-F]{64}$")
        private val DECIMAL_NUMBER_REGEX = Regex("^\\d+$")

        fun parse(raw: String): BlockRevision {
            val trimmed = raw.trim()
            val normalizedKeyword = trimmed.lowercase()

            Keyword.entries
                .firstOrNull { it.value == normalizedKeyword }
                ?.let {
                    return it
                }

            if (BLOCK_ID_REGEX.matches(trimmed)) {
                return Id(trimmed.lowercase())
            }

            if (DECIMAL_NUMBER_REGEX.matches(trimmed)) {
                val number =
                    trimmed.toLongOrNull()
                        ?: throw IllegalArgumentException("Invalid block number revision: '$raw'")
                return Number(number)
            }

            throw IllegalArgumentException(
                "Invalid revision '$raw': must be a block number, a 32-byte block id (0x + 64 hex chars), or one of ${Keyword.entries.joinToString { it.value }}"
            )
        }
    }
}
