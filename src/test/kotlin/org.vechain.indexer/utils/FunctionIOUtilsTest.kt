package org.vechain.indexer.utils

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
}
