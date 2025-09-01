package org.vechain.indexer.utils

import java.math.BigDecimal
import java.math.BigInteger
import org.junit.jupiter.api.Test
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.utils.ParamUtils.getAsBigDecimal
import org.vechain.indexer.utils.ParamUtils.getAsBigInteger
import org.vechain.indexer.utils.ParamUtils.getAsBoolean
import org.vechain.indexer.utils.ParamUtils.getAsInt
import org.vechain.indexer.utils.ParamUtils.getAsLong
import org.vechain.indexer.utils.ParamUtils.getAsString
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNull

internal class ParamUtilsTest {

    @Test
    fun `getAsString returns correct string values`() {
        val paramMap: Map<String, Any> =
            mapOf(
                "str" to "hello",
                "empty" to "",
                "int" to 42,
                "bigDecimal" to BigDecimal("123.45"),
                "bigInteger" to BigInteger("123456"),
            )
        val params = AbiEventParameters(paramMap)
        expectThat(params.getAsString("str")).isEqualTo("hello")
        expectThat(params.getAsString("empty")).isEqualTo("")
        expectThat(params.getAsString("int")).isEqualTo("42")
        expectThat(params.getAsString("bigDecimal")).isEqualTo("123.45")
        expectThat(params.getAsString("bigInteger")).isEqualTo("123456")
        expectThat(params.getAsString("null")).isNull()
        expectThat(params.getAsString("missing")).isNull()
    }

    @Test
    fun `getAsInt returns correct int values`() {
        val paramMap: Map<String, Any> =
            mapOf(
                "int" to 42,
                "strInt" to "123",
                "strInvalid" to "abc",
                "long" to 100L,
                "double" to 99.9
            )
        val params = AbiEventParameters(paramMap)
        expectThat(params.getAsInt("int")).isEqualTo(42)
        expectThat(params.getAsInt("strInt")).isEqualTo(123)
        expectThat(params.getAsInt("strInvalid")).isNull()
        expectThat(params.getAsInt("long")).isEqualTo(100)
        expectThat(params.getAsInt("double")).isEqualTo(99)
        expectThat(params.getAsInt("null")).isNull()
        expectThat(params.getAsInt("missing")).isNull()
    }

    @Test
    fun `getAsLong returns correct long values`() {
        val paramMap: Map<String, Any> =
            mapOf(
                "long" to 123456789L,
                "int" to 42,
                "strLong" to "987654321",
                "strInvalid" to "abc",
                "double" to 99.9
            )
        val params = AbiEventParameters(paramMap)
        expectThat(params.getAsLong("long")).isEqualTo(123456789L)
        expectThat(params.getAsLong("int")).isEqualTo(42L)
        expectThat(params.getAsLong("strLong")).isEqualTo(987654321L)
        expectThat(params.getAsLong("strInvalid")).isNull()
        expectThat(params.getAsLong("double")).isEqualTo(99L)
        expectThat(params.getAsLong("null")).isNull()
        expectThat(params.getAsLong("missing")).isNull()
    }

    @Test
    fun `getAsBoolean returns correct boolean values`() {
        val paramMap: Map<String, Any> =
            mapOf(
                "boolTrue" to true,
                "boolFalse" to false,
                "strTrue" to "true",
                "strFalse" to "false",
                "strInvalid" to "notabool"
            )
        val params = AbiEventParameters(paramMap)
        expectThat(params.getAsBoolean("boolTrue")).isEqualTo(true)
        expectThat(params.getAsBoolean("boolFalse")).isEqualTo(false)
        expectThat(params.getAsBoolean("strTrue")).isEqualTo(true)
        expectThat(params.getAsBoolean("strFalse")).isEqualTo(false)
        expectThat(params.getAsBoolean("strInvalid")).isEqualTo(false)
        expectThat(params.getAsBoolean("null")).isNull()
        expectThat(params.getAsBoolean("missing")).isNull()
    }

    @Test
    fun `getAsBigInteger returns correct BigInteger values`() {
        val paramMap: Map<String, Any> =
            mapOf(
                "bigInt" to BigInteger("123456"),
                "long" to 123L,
                "int" to 42,
                "strBigInt" to "789012",
                "strInvalid" to "abc",
                "double" to 99.9
            )
        val params = AbiEventParameters(paramMap)
        expectThat(params.getAsBigInteger("bigInt")).isEqualTo(BigInteger("123456"))
        expectThat(params.getAsBigInteger("long")).isEqualTo(BigInteger("123"))
        expectThat(params.getAsBigInteger("int")).isEqualTo(BigInteger("42"))
        expectThat(params.getAsBigInteger("strBigInt")).isEqualTo(BigInteger("789012"))
        expectThat(params.getAsBigInteger("strInvalid")).isNull()
        expectThat(params.getAsBigInteger("double")).isEqualTo(BigInteger("99"))
        expectThat(params.getAsBigInteger("null")).isNull()
        expectThat(params.getAsBigInteger("missing")).isNull()
    }

    @Test
    fun `getAsBigDecimal returns correct BigDecimal values`() {
        val paramMap: Map<String, Any> =
            mapOf(
                "bigDec" to BigDecimal("123.45"),
                "bigInt" to BigInteger("123456"),
                "long" to 123L,
                "int" to 42,
                "strBigDec" to "789.01",
                "strInvalid" to "abc",
                "double" to 99.9
            )
        val params = AbiEventParameters(paramMap)
        expectThat(params.getAsBigDecimal("bigDec")).isEqualTo(BigDecimal("123.45"))
        expectThat(params.getAsBigDecimal("bigInt")).isEqualTo(BigDecimal("123456"))
        expectThat(params.getAsBigDecimal("long")).isEqualTo(BigDecimal("123"))
        expectThat(params.getAsBigDecimal("int")).isEqualTo(BigDecimal("42"))
        expectThat(params.getAsBigDecimal("strBigDec")).isEqualTo(BigDecimal("789.01"))
        expectThat(params.getAsBigDecimal("strInvalid")).isNull()
        expectThat(params.getAsBigDecimal("double")).isEqualTo(BigDecimal("99"))
        expectThat(params.getAsBigDecimal("null")).isNull()
        expectThat(params.getAsBigDecimal("missing")).isNull()
    }
}
