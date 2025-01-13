package org.vechain.indexer.event.types

object TypesUtils {
    fun getType(typeName: String): Types =
        when (typeName) {
            "address" -> Types.ADDRESS
            "uint256" -> Types.UINT256
            "uint128" -> Types.UINT128
            "bool" -> Types.BOOL
            "bytes32" -> Types.BYTES32
            else -> throw IllegalArgumentException("Unknown type: $typeName")
        }
}
