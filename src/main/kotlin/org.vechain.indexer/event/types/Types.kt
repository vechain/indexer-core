package org.vechain.indexer.event.types

import java.math.BigInteger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.utils.DataUtils

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
            val decoded = DataUtils.decodeQuantity(encoded)
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
            val decodedValue = DataUtils.decodeQuantity(encoded) // Decode the value using HexUtils
            val boolValue =
                decodedValue == BigInteger.ONE // Interpret the decoded value as a boolean
            return DecodedValue(boolValue.toString(), clazz, clazz.cast(boolValue), name)
        }

        override fun getClaas(): Class<*> = Boolean::class.java
    },
    INT {
        override fun isType(typeName: String): Boolean =
            typeName.startsWith("int") &&
                (typeName == "int" || typeName.removePrefix("int").toIntOrNull() in 8..256)

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            val decodedBigInt = DataUtils.decodeQuantity(encoded)
            val bitSize = name.removePrefix("int").toIntOrNull() ?: 256 // Default to int256
            val signedValue = convertToSignedBigInt(decodedBigInt, bitSize)

            return DecodedValue(signedValue.toString(), clazz, clazz.cast(signedValue), name)
        }

        override fun getClaas(): Class<*> = BigInteger::class.java
    },
    BYTES {
        override fun isType(typeName: String): Boolean =
            typeName == "bytes" ||
                (typeName.startsWith("bytes") &&
                    typeName.removePrefix("bytes").toIntOrNull() in 1..32)

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            if (encoded.length % 2 != 0 || !encoded.startsWith("0x")) {
                throw IllegalArgumentException("Invalid bytes value: $encoded")
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
            // Check if fullData is null or empty, indicating an indexed string (Keccak-256 hash in
            // topics)
            if (fullData.isNullOrEmpty()) {
                return DecodedValue(encoded, clazz, clazz.cast(encoded), name)
            }

            decodeAbiString(fullData, startPosition)?.let {
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

            val inputData = DataUtils.removePrefix(fullData ?: encoded)
            val decodedComponents = mutableMapOf<String, Any?>()

            val isDynamicTuple = isTupleDynamic(components)

            val tupleData: String =
                if (isDynamicTuple) {
                    val offsetHex = inputData.substring(startPosition, startPosition + 64)
                    val offset = DataUtils.decodeQuantity("0x$offsetHex").toInt() * 2
                    inputData.substring(offset)
                } else {
                    inputData.substring(startPosition)
                }

            var currentOffset = 0
            for (component in components) {
                if (inputData.length < currentOffset + 64) {
                    throw IllegalArgumentException(
                        "Data too short for tuple component at offset $currentOffset",
                    )
                }

                val componentData = tupleData.substring(currentOffset, currentOffset + 64)
                var startPos = currentOffset
                val decodedComponent =
                    Types.values()
                        .firstOrNull { it.isType(component.type) }
                        ?.decode(
                            encoded = DataUtils.addPrefix(componentData),
                            clazz = Any::class.java,
                            name = component.type,
                            fullData = tupleData,
                            startPosition = startPos,
                            components = component.components, // for nested tuples
                        )
                        ?: throw IllegalArgumentException(
                            "Unsupported type in tuple: ${component.type}",
                        )

                decodedComponents[component.name] = decodedComponent.actualValue
                currentOffset += 64
            }

            return DecodedValue(
                decodedComponents.toString(),
                clazz,
                clazz.cast(decodedComponents),
                name,
            )
        }

        override fun getClaas(): Class<*> = List::class.java
    },
    ARRAY {
        override fun isType(typeName: String): Boolean =
            typeName.endsWith("[]") || typeName.matches(Regex(".+\\[\\d+\\]"))

        override fun <T> decode(
            encoded: String,
            clazz: Class<T>,
            name: String,
            fullData: String?,
            startPosition: Int,
            components: List<InputOutput>?,
        ): DecodedValue<T> {
            val isFixedArray = name.matches(Regex(".+\\[\\d+\\]"))
            val elementType =
                when {
                    name.endsWith("[]") -> name.removeSuffix("[]")
                    isFixedArray -> name.substringBeforeLast("[")
                    else -> throw IllegalArgumentException("Invalid array type: $name")
                }

            val fixedLength =
                if (isFixedArray) {
                    name.substringAfter("[").substringBefore("]").toInt()
                } else {
                    0
                }

            val decodedElements =
                decodeArray(
                    elementType = elementType,
                    fullData = fullData ?: encoded,
                    startPosition = startPosition,
                    isFixedArray = isFixedArray,
                    fixedLength = fixedLength,
                )

            val stringRepresentation =
                decodedElements.joinToString(prefix = "[", postfix = "]") { it.toString() }

            return DecodedValue(stringRepresentation, clazz, clazz.cast(decodedElements), name)
        }

        override fun getClaas(): Class<*> = List::class.java
    },
    ;

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
            val inputData = DataUtils.removePrefix(encodedString)
            try {
                if (inputData.length < startPosition + 64) {
                    throw IllegalArgumentException("Data is too short for extracting offset")
                }
                val offsetHex = inputData.substring(startPosition, startPosition + 64)
                val offset = DataUtils.decodeQuantity(DataUtils.addPrefix(offsetHex)).toInt()
                if (offset * 2 + 64 > inputData.length) {
                    throw IllegalArgumentException("Invalid offset or data length")
                }

                val lengthHex = inputData.substring(offset * 2, offset * 2 + 64)
                val length = DataUtils.decodeQuantity(DataUtils.addPrefix(lengthHex)).toInt()
                val stringStart = offset * 2 + 64
                val stringEnd = stringStart + length * 2

                if (inputData.length < stringEnd) {
                    throw IllegalArgumentException("String end exceeds data length")
                }

                val stringHex = inputData.substring(stringStart, stringEnd)

                return String(
                    DataUtils.hexStringToByteArray(DataUtils.addPrefix(stringHex)),
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
            isFixedArray: Boolean = false,
            fixedLength: Int = 0,
        ): List<Any> {
            val inputData = DataUtils.removePrefix(fullData)
            val decodedArray = mutableListOf<Any>()

            var currentOffset: Int
            var arrayLength: Int

            if (isFixedArray) {
                arrayLength = fixedLength
                currentOffset = startPosition
            } else {
                val arrayOffsetHex = inputData.substring(startPosition, startPosition + 64)
                val arrayOffset = DataUtils.decodeQuantity("0x$arrayOffsetHex").toInt() * 2

                val arrayLengthHex = inputData.substring(arrayOffset, arrayOffset + 64)
                arrayLength = DataUtils.decodeQuantity("0x$arrayLengthHex").toInt()

                currentOffset = arrayOffset + 64
            }

            for (i in 0 until arrayLength) {
                val elementData = inputData.substring(currentOffset, currentOffset + 64)
                val element = decodeBasicType<Any>(elementType, "0x$elementData")
                decodedArray.add(element.actualValue)
                currentOffset += 64
            }

            return decodedArray
        }

        fun convertToSignedBigInt(
            value: BigInteger,
            bitSize: Int,
        ): BigInteger {
            val maxUnsignedValue = BigInteger.ONE.shiftLeft(bitSize) // 2^bitSize
            val maxSignedValue = maxUnsignedValue.shiftRight(1) // 2^(bitSize-1)

            return if (value >= maxSignedValue) {
                value.subtract(maxUnsignedValue) // Convert from two's complement if negative
            } else {
                value
            }
        }

        private inline fun <reified T> decodeBasicType(
            typeName: String,
            encoded: String,
            fullData: String? = null,
            startPosition: Int = 0,
        ): DecodedValue<T> =
            Types.values()
                .firstOrNull { it.isType(typeName) }
                ?.decode(
                    encoded,
                    T::class.java,
                    typeName,
                    fullData,
                    startPosition,
                ) // Pass correct type to decode
            ?: throw IllegalArgumentException("Unsupported type: $typeName")
    }

    fun isTupleDynamic(components: List<InputOutput>): Boolean =
        components.any { input ->
            val type = input.type
            // Dynamic types: string, bytes, dynamic arrays, or dynamic tuples
            type == "string" ||
                type == "bytes" ||
                type.endsWith("[]") ||
                (type == "tuple" && input.components?.let { isTupleDynamic(it) } == true)
        }
}
