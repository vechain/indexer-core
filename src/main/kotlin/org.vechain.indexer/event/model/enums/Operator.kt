package org.vechain.indexer.event.model.enums

enum class Operator {
    EQ {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = a.equals(b, ignoreCase = true)
    },
    NE {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = a != b
    },
    GT {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = compare(a, b) > 0
    },
    LT {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = compare(a, b) < 0
    },
    GE {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = compare(a, b) >= 0
    },
    LE {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = compare(a, b) <= 0
    },
    ;

    abstract fun evaluate(
        a: String,
        b: String,
    ): Boolean

    protected fun compare(
        a: String,
        b: String,
    ): Int =
        try {
            a.toBigDecimal().compareTo(b.toBigDecimal())
        } catch (e: NumberFormatException) {
            a.compareTo(b)
        }
}
