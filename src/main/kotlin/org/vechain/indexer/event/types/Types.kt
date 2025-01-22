package org.vechain.indexer.event.types

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.abi.InputOutput
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
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            if (encoded.length < 64) {
                throw IllegalArgumentException("Invalid address length: $encoded")
            }
            val decoded = "0x" + encoded.takeLast(40)
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
            components: List<InputOutput>?,
        ): DecodedValue<T> {
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
            components: List<InputOutput>?,
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
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            if (encoded.length != 66 || !encoded.startsWith("0x")) {
                throw IllegalArgumentException("Invalid bytes32 value: $encoded")
            }
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
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            val dataToDecode = fullData ?: encoded
            decodeAbiString(dataToDecode, startPosition)?.let {
                return DecodedValue(it, clazz, clazz.cast(it), name)
            } ?: throw IllegalArgumentException("Error decoding string at offset: $startPosition")
        }

        override fun getClaas(): Class<*> = String::class.java
    },

    TUPLE {
        override fun isType(typeName: String): Boolean = typeName == "tuple"

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            if (components == null) {
                throw IllegalArgumentException("Components must be provided for tuple types")
            }

            val inputData = Numeric.cleanHexPrefix(fullData ?: encoded)
            val decodedComponents = mutableMapOf<String, Any?>()
            var currentOffset = startPosition

            // Decode each component in the tuple
            for (component in components) {
                // Extract the next 32 bytes
                if (inputData.length < currentOffset + 64) {
                    throw IllegalArgumentException("Data too short for tuple component at offset $currentOffset")
                }
                val componentData = inputData.substring(currentOffset, currentOffset + 64)

                // Decode the component based on its type
                val decodedComponent =
                    Types
                        .values()
                        .firstOrNull { it.isType(component.type) }
                        ?.decode(
                            encoded = Numeric.prependHexPrefix(componentData),
                            clazz = Any::class.java,
                            name = component.type,
                            fullData = fullData,
                            startPosition = currentOffset,
                        )
                        ?: throw IllegalArgumentException("Unsupported type in tuple: ${component.type}")

                decodedComponents[component.name] = decodedComponent.actualValue
                currentOffset += 64 // Move to the next 32-byte slot
            }

            return DecodedValue(decodedComponents.toString(), clazz, clazz.cast(decodedComponents), name)
        }

        override fun getClaas(): Class<*> = List::class.java
    },

    ARRAY {
        override fun isType(typeName: String): Boolean = typeName.endsWith("[]") || typeName.matches(Regex(".+\\[\\d+\\]"))

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            val elementType =
                when {
                    name.endsWith("[]") -> name.removeSuffix("[]")
                    name.matches(Regex(".+\\[\\d+\\]")) -> name.substringBeforeLast("[")
                    else -> throw IllegalArgumentException("Invalid array type: $name")
                }

            val decodedElements = decodeArray(elementType, fullData ?: encoded, startPosition)

            // Convert the list to a JSON-like string representation
            val stringRepresentation = decodedElements.joinToString(prefix = "[", postfix = "]") { it.toString() }

            return DecodedValue(stringRepresentation, clazz, clazz.cast(decodedElements), name)
        }

        override fun getClaas(): Class<*> = List::class.java
    }, ;

    abstract fun isType(typeName: String): Boolean

    abstract fun <T> decode(
        encoded: String,
        clazz: Class<T>,
        name: String,
        fullData: String?,
        startPosition: Int,
        components: List<InputOutput>? = null,
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
                if (inputData.length < startPosition + 64) {
                    throw IllegalArgumentException("Data is too short for extracting offset")
                }
                val offsetHex = inputData.substring(startPosition, startPosition + 64)
                val offset = Numeric.decodeQuantity(Numeric.prependHexPrefix(offsetHex)).toInt()

                if (offset * 2 + 64 > inputData.length) {
                    throw IllegalArgumentException("Invalid offset or data length")
                }

                val lengthHex = inputData.substring(offset * 2, offset * 2 + 64)
                val length = Numeric.decodeQuantity(Numeric.prependHexPrefix(lengthHex)).toInt()

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

        fun decodeArray(
            elementType: String,
            fullData: String,
            startPosition: Int,
        ): List<Any> {
            val inputData = Numeric.cleanHexPrefix(fullData)

            val arrayOffsetHex = inputData.substring(startPosition, (startPosition + 64))
            val arrayOffset = Numeric.decodeQuantity(Numeric.prependHexPrefix(arrayOffsetHex)).toInt() * 2

            val arrayLengthHex = inputData.substring(arrayOffset, arrayOffset + 64)
            val arrayLength = Numeric.decodeQuantity(Numeric.prependHexPrefix(arrayLengthHex)).toInt()

            val decodedArray = mutableListOf<Any>()
            var currentOffset = arrayOffset + 64 // Start after the array length field
            for (i in 0 until arrayLength) {
                val elementData = inputData.substring(currentOffset, currentOffset + 64)
                try {
                    val prefixedElementData = Numeric.prependHexPrefix(elementData) // Ensure the `0x` prefix is added
                    val element = decodeBasicType<Any>(elementType, prefixedElementData) // Decode individual elements
                    decodedArray.add(element.actualValue)
                } catch (e: Exception) {
                    logger.error("Error decoding element $i of type $elementType at offset $currentOffset: ${e.message}")
                    throw e
                }

                currentOffset += 64
            }

            return decodedArray
        }

        private inline fun <reified T> decodeBasicType(
            typeName: String,
            encoded: String,
        ): DecodedValue<T> =
            Types
                .values()
                .firstOrNull { it.isType(typeName) }
                ?.decode(encoded, T::class.java, typeName, null, 0) // Pass correct type to decode
                ?: throw IllegalArgumentException("Unsupported type: $typeName")
    }
}
