package org.vechain.indexer.event.types

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.web3j.abi.TypeDecoder
import org.web3j.utils.Numeric
import java.math.BigInteger

enum class Types {
    ADDRESS {
        override fun isType(typeName: String): Boolean = typeName == "address"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
        ): DecodedValue<T> {
            // Ensure the input length is valid
            if (encoded.length < 64) {
                throw IllegalArgumentException("Invalid address length: $encoded")
            }
            val decoded = "0x" + encoded.takeLast(40) // Extract 20 bytes (last 40 characters)
            return DecodedValue(decoded, clazz, clazz.cast(decoded), name)
        }

        override fun getClaas(): Class<*> = String::class.java
    },

    UINT {
        override fun isType(typeName: String): Boolean =
            typeName.startsWith("uint") &&
                (typeName == "uint" || typeName.removePrefix("uint").toIntOrNull() in 8..256)

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
        ): DecodedValue<T> {
            val bitSize = if (name == "uint") 256 else name.removePrefix("uint").toIntOrNull() ?: 256

            // Ensure the bit size is valid
            if (bitSize !in 8..256 || bitSize % 8 != 0) {
                throw IllegalArgumentException("Invalid uint bit size: $bitSize")
            }

            // Decode the value (all uint types are decoded the same way as a BigInteger)
            val decoded = Numeric.decodeQuantity(encoded)
            return DecodedValue(decoded.toString(), clazz, clazz.cast(decoded), name)
        }

        override fun getClaas(): Class<*> = BigInteger::class.java
    },

    BOOL {
        override fun isType(typeName: String): Boolean = typeName == "bool"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
        ): DecodedValue<T> {
            val decoded = TypeDecoder.decodeBool(Numeric.cleanHexPrefix(encoded), 0).value
            return DecodedValue(decoded.toString(), clazz, clazz.cast(decoded), name)
        }

        override fun getClaas(): Class<*> = Boolean::class.java
    },

    BYTES32 {
        override fun isType(typeName: String): Boolean = typeName == "bytes32"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
        ): DecodedValue<T> {
            // Ensure the input is a valid bytes32
            if (encoded.length != 66 || !encoded.startsWith("0x")) {
                throw IllegalArgumentException("Invalid bytes32 value: $encoded")
            }

            // Return the original hex value
            return DecodedValue(encoded, clazz, clazz.cast(encoded), name)
        }

        override fun getClaas(): Class<*> = String::class.java
    },

    STRING {
        override fun isType(typeName: String): Boolean = typeName == "string"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
        ): DecodedValue<T> {
            val dataToDecode = fullData ?: encoded // Use fullData if available; otherwise, fallback to encoded
            decodeAbiString(dataToDecode, startPosition)?.let {
                return DecodedValue(it, clazz, clazz.cast(it), name)
            } ?: throw IllegalArgumentException("Error decoding string at offset: $startPosition")
        }

        override fun getClaas(): Class<*> = String::class.java
    }, ;

    abstract fun isType(typeName: String): Boolean

    abstract fun <T> decode(
        encoded: String,
        clazz: Class<T>,
        name: String,
        fullData: String?,
        startPosition: Int,
    ): DecodedValue<T>

    abstract fun getClaas(): Class<*>

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(this::class.java)

        fun decodeAbiString(
            encodedString: String,
            startPosition: Int,
        ): String? {
            val inputData = Numeric.cleanHexPrefix(encodedString)
            try {
                // Extract the offset at the given startPosition
                if (inputData.length < startPosition + 64) {
                    throw IllegalArgumentException("Data is too short for extracting offset")
                }
                val offsetHex = inputData.substring(startPosition, startPosition + 64)
                val offset = Numeric.decodeQuantity(Numeric.prependHexPrefix(offsetHex)).toInt()

                // Check if offset is valid
                if (offset * 2 + 64 > inputData.length) {
                    throw IllegalArgumentException("Invalid offset or data length")
                }

                // Extract the string length at the offset
                val lengthHex = inputData.substring(offset * 2, offset * 2 + 64)
                val length = Numeric.decodeQuantity(Numeric.prependHexPrefix(lengthHex)).toInt()

                // Extract the actual string content
                val stringStart = offset * 2 + 64
                val stringEnd = stringStart + length * 2

                if (inputData.length < stringEnd) {
                    throw IllegalArgumentException("String end exceeds data length")
                }

                val stringHex = inputData.substring(stringStart, stringEnd)

                return String(
                    Numeric.hexStringToByteArray(Numeric.prependHexPrefix(stringHex)),
                    Charsets.UTF_8,
                )
            } catch (e: Exception) {
                logger.error("Error decoding string at offset: $startPosition", e)
                return null
            }
        }
    }
}
