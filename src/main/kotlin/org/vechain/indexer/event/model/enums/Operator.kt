package org.vechain.indexer.event.model.enums

enum class Operator {
    EQ {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = a == b
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
        ): Boolean = a > b
    },
    LT {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = a < b
    },
    GE {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = a >= b
    },
    LE {
        override fun evaluate(
            a: String,
            b: String,
        ): Boolean = a <= b
    }, ;

    abstract fun evaluate(
        a: String,
        b: String,
    ): Boolean
}
