package org.vechain.indexer.utils

import java.math.BigInteger

object DataUtils {
    /** Optional prefix 0x */
    private const val REGEX = "^(0x)?[0-9a-fA-F]+$"

    private fun isValid(hex: String): Boolean = hex.matches(Regex(REGEX))

    fun addPrefix(hex: String): String = if (hex.startsWith("0x")) hex else "0x$hex"

    fun removePrefix(hex: String): String = if (hex.startsWith("0x")) hex.substring(2) else hex

    /**
     * Decodes a hexadecimal string into a BigInteger.
     *
     * @param hex The hexadecimal string to decode.
     * @return A BigInteger representation of the hexadecimal value.
     * @throws IllegalArgumentException if the input is not a valid hexadecimal string.
     */
    fun decodeQuantity(hex: String): BigInteger {
        val cleanHex = removePrefix(hex)
        if (!isValid(cleanHex)) {
            throw IllegalArgumentException("Invalid hexadecimal input: $hex")
        }
        return BigInteger(cleanHex, 16)
    }

    /**
     * Converts a hex string into a byte array.
     * @param input The hex string to convert.
     * @return The resulting byte array.
     */
    fun hexStringToByteArray(input: String): ByteArray {
        val cleanInput = removePrefix(input)
        val len = cleanInput.length

        if (len == 0) {
            return byteArrayOf()
        }

        val data: ByteArray
        var startIdx: Int

        if (len % 2 != 0) {
            data = ByteArray((len / 2) + 1)
            data[0] = Character.digit(cleanInput[0], 16).toByte()
            startIdx = 1
        } else {
            data = ByteArray(len / 2)
            startIdx = 0
        }

        for (i in startIdx until len step 2) {
            data[(i + 1) / 2] =
                (
                    (Character.digit(cleanInput[i], 16) shl 4) +
                        Character.digit(cleanInput[i + 1], 16)
                ).toByte()
        }

        return data
    }
}
