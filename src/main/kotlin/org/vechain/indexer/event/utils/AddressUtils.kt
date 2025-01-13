package org.vechain.indexer.event.utils

import org.vechain.indexer.event.model.Address
import org.web3j.abi.FunctionReturnDecoder
import java.math.BigInteger

object AddressUtils {
    /**
     * Converts a hexadecimal address to `BigInteger`.
     *
     * @param address Hexadecimal address as a string.
     * @return The `BigInteger` representation.
     */
    fun toBigInt(address: String): BigInteger = BigInteger(HexUtils.removePrefix(address), 16)

    /**
     * Decodes an address from a string.
     *
     * @param data Input string to decode.
     * @return The decoded address.
     * @throws IllegalArgumentException If decoding fails.
     */
    fun decode(data: String): String {
        if (Address(data).isValid()) return data

        val address = FunctionReturnDecoder.decodeAddress(data)

        require(Address(address).isValid()) { "Failed to decode address for data: $data" }

        return address
    }
}
