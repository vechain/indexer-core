package org.vechain.indexer.event.types

import org.vechain.indexer.event.utils.AddressUtils
import org.web3j.abi.TypeDecoder
import org.web3j.abi.datatypes.generated.Bytes32
import org.web3j.utils.Numeric
import java.math.BigInteger

enum class Types {
    ADDRESS {
        override fun isType(typeName: String): Boolean = typeName == "address"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
        ): DecodedValue<T> {
            val decoded = AddressUtils.decode(encoded)
            return DecodedValue(decoded, clazz, clazz.cast(decoded), name)
        }

        override fun getClaas(): Class<*> = String::class.java
    },

    UINT256 {
        override fun isType(typeName: String): Boolean = typeName == "uint256"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
        ): DecodedValue<T> {
            val decoded = Numeric.decodeQuantity(encoded).toString()
            return DecodedValue(decoded, clazz, clazz.cast(BigInteger(decoded)), name)
        }

        override fun getClaas(): Class<*> = BigInteger::class.java
    },

    UINT128 {
        override fun isType(typeName: String): Boolean = typeName == "uint128"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
        ): DecodedValue<T> {
            val decoded = Numeric.decodeQuantity(encoded).toString()
            return DecodedValue(decoded, clazz, clazz.cast(BigInteger(decoded)), name)
        }

        override fun getClaas(): Class<*> = BigInteger::class.java
    },

    BOOL {
        override fun isType(typeName: String): Boolean = typeName == "bool"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
        ): DecodedValue<T> {
            val decoded = TypeDecoder.decodeBool(HexUtils.removePrefix(encoded), 0).value.toString()
            return DecodedValue(decoded, clazz, clazz.cast(java.lang.Boolean.valueOf(decoded)), name)
        }

        override fun getClaas(): Class<*> = java.lang.Boolean::class.java
    },

    BYTES32 {
        override fun isType(typeName: String): Boolean = typeName == "bytes32"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
        ): DecodedValue<T> {
            val hexString = HexUtils.removePrefix("0x657572742d757364000000000000000000000000000000000000000000000000")
            val decodedBytes = TypeDecoder.decode(hexString, 0, Bytes32::class.java) as Bytes32
            val decodedString = String(decodedBytes.value).trim { it <= '\u0000' }
            return DecodedValue(decodedString, clazz, clazz.cast(decodedString), name)
        }

        override fun getClaas(): Class<*> = String::class.java
    }, ;

    abstract fun isType(typeName: String): Boolean

    abstract fun <T> decode(
        encoded: String,
        clazz: Class<T>,
        name: String,
    ): DecodedValue<T>

    abstract fun getClaas(): Class<*>
}

inline fun <reified T> Types.decode(
    encoded: String,
    name: String,
): DecodedValue<T> = decode(encoded, T::class.java, name)
