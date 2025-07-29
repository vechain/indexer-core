package org.vechain.indexer.utils

import java.math.BigInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.utils.FunctionReturnDecoder

internal class FunctionReturnDecoderTest {
    @Test
    fun `decode string array from getNames output`() {
        val rawOutput =
            "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000d726f6f7461737469632e76657400000000000000000000000000000000000000"

        val outputAbi =
            listOf(
                InputOutput(
                    name = "names",
                    type = "string[]",
                    indexed = false,
                    components = null,
                ),
            )

        val decoded = FunctionReturnDecoder.decode(rawOutput, outputAbi)
        val expected = listOf("rootastic.vet")

        assertEquals(expected, decoded["names"])
    }

    @Test
    fun `decode string fixed int array`() {
        val rawOutput =
            "0x0000000000000000000000000000000000000000000000000000000000000005000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

        val outputAbi =
            listOf(
                InputOutput(
                    name = "tally",
                    type = "uint64[10]",
                    indexed = false,
                    components = null,
                ),
            )

        val decoded = FunctionReturnDecoder.decode(rawOutput, outputAbi)
        val expected =
            listOf(
                BigInteger("5"),
                BigInteger("1"),
                BigInteger("0"),
                BigInteger("0"),
                BigInteger("0"),
                BigInteger("0"),
                BigInteger("0"),
                BigInteger("0"),
                BigInteger("0"),
                BigInteger("0"),
            )

        assertEquals(expected, decoded["tally"])
    }

    @Test
    fun `decode fixed bytes32 array`() {
        val rawOutput =
            "0x00000000000000000000000000000000000000000000000000000000000002200000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000010000000000000000000000003d23a9396b342f82c9dddd2534bc501127d7648e00000000000000000000000000000000000000000000000000000000606c72b60000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000052656475636520697420746f203125000000000000000000000000000000000052656475636520697420746f20352500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002f50726f706f73616c206f6e2041646a757374696e67205665436861696e54686f7220426173652047617350726963650000000000000000000000000000000000"

        val outputAbi =
            listOf(
                InputOutput(
                    name = "title",
                    type = "string",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "checkType",
                    type = "uint8",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "minChecked",
                    type = "uint8",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "maxChecked",
                    type = "uint8",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "creator",
                    type = "address",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "createTime",
                    type = "uint64",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "cancelTime",
                    type = "uint64",
                    indexed = false,
                    components = null,
                ),
                InputOutput(
                    name = "options",
                    type = "bytes32[10]",
                    indexed = false,
                    components = null,
                ),
            )

        val decoded = FunctionReturnDecoder.decode(rawOutput, outputAbi)
        val expected =
            listOf(
                "0x000000000000000000000000000000000052656475636520697420746f203125",
                "0x000000000000000000000000000000000052656475636520697420746f203525",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
            )

        assertEquals(expected, decoded["options"])
    }
}
