package org.vechain.indexer.utils

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.vechain.indexer.EventMockFactory.transferAbiElement
import org.vechain.indexer.EventMockFactory.transferERC721AbiElement
import org.vechain.indexer.EventMockFactory.transferEvent
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.model.generic.GenericEventParameters
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.model.TxEvent
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.message
import java.math.BigInteger
import java.util.*

internal class EventUtilsTest {
    @Nested
    inner class DecodeEvent {
        @Test
        fun `should throw error if number of topics in event does not correspond to number of indexed inputs`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            val exception =
                expectThrows<IllegalArgumentException> {
                    EventUtils.decodeEvent(event, transferERC721AbiElement)
                }

            expectThat(exception.message.subject)
                .isEqualTo("Mismatch between ABI indexed inputs and event topics")
        }

        @Test
        fun `should handle decoding gracefully if unsupported solidity type is trying to be decoded`() {
            val randomAbiElement =
                AbiElement(
                    name = "RandomEvent",
                    type = "event",
                    anonymous = false,
                    stateMutability = null,
                    inputs =
                        listOf(
                            InputOutput("randomType", "from", "randomType", indexed = true),
                        ),
                    outputs = emptyList(),
                    signature = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                )

            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics =
                        listOf(
                            transferEvent.topics[0],
                            transferEvent.topics[1],
                        ),
                    data = transferEvent.data,
                )

            val result = EventUtils.decodeEvent(event, randomAbiElement)
            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues =
                            mapOf(
                                "from" to
                                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                            ),
                        eventType = "RandomEvent",
                    ),
                )
        }

        @Test
        fun `should decode string array correctly `() {
            val randomAbiElement =
                AbiElement(
                    name = "Names",
                    type = "event",
                    anonymous = false,
                    stateMutability = null,
                    inputs =
                        listOf(
                            InputOutput("string[]", "name", "string[]", indexed = false),
                        ),
                    outputs = emptyList(),
                    signature = "54612034f490f8c9efbbf618b99e0dd23834387135bf603e7f77f36ab5a0dc59",
                )

            val event =
                TxEvent(
                    address = "0xd9145cce52d386f254917e481eb44e9943f39138",
                    topics =
                        listOf(
                            "0x54612034f490f8c9efbbf618b99e0dd23834387135bf603e7f77f36ab5a0dc59",
                        ),
                    data =
                        @Suppress("ktlint:standard:max-line-length")
                        "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000e00000000000000000000000000000000000000000000000000000000000000120000000000000000000000000000000000000000000000000000000000000016000000000000000000000000000000000000000000000000000000000000001a000000000000000000000000000000000000000000000000000000000000000054170706c6500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000054d616e676f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000642616e616e61000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000064f72616e6765000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000064772617065730000000000000000000000000000000000000000000000000000",
                )

            val result = EventUtils.decodeEvent(event, randomAbiElement)
            println(result)
            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues =
                            mapOf("name" to listOf("Apple", "Mango", "Banana", "Orange", "Grapes")),
                        eventType = "Names",
                    ),
                )
        }

        @Test
        fun `should decode string correctly `() {
            val randomAbiElement =
                AbiElement(
                    name = "StringTest",
                    type = "event",
                    anonymous = false,
                    stateMutability = null,
                    inputs =
                        listOf(
                            InputOutput("string", "name", "string", indexed = false),
                        ),
                    outputs = emptyList(),
                    signature = "4275a3c6c689ed5bba95c8c92a82908d2a55331ad3a145c52459468a14290696",
                )

            val event =
                TxEvent(
                    address = "0xd9145CCE52D386f254917e481eB44e9943F39138",
                    topics =
                        listOf(
                            "0x4275a3c6c689ed5bba95c8c92a82908d2a55331ad3a145c52459468a14290696",
                        ),
                    data =
                        @Suppress("ktlint:standard:max-line-length")
                        "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000047465737400000000000000000000000000000000000000000000000000000000",
                )

            val result = EventUtils.decodeEvent(event, randomAbiElement)
            println(result)
            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues = mapOf("name" to "test"),
                        eventType = "StringTest",
                    ),
                )
        }

        @Test
        fun `should decode address array correctly `() {
            val randomAbiElement =
                AbiElement(
                    name = "Addresses",
                    type = "event",
                    anonymous = false,
                    stateMutability = null,
                    inputs =
                        listOf(
                            InputOutput("address[]", "addresses", "address[]", indexed = false),
                        ),
                    outputs = emptyList(),
                    signature = "ee22d2969847e9b3ce81f2f1017daba3a00efb92015cd57c4f82b23efd395175",
                )

            val event =
                TxEvent(
                    address = "0xd9145CCE52D386f254917e481eB44e9943F39138",
                    topics =
                        listOf(
                            "0xee22d2969847e9b3ce81f2f1017daba3a00efb92015cd57c4f82b23efd395175",
                        ),
                    data =
                        @Suppress("ktlint:standard:max-line-length")
                        "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000002000000000000000000000000f8e81d47203a594245e36c48e151709f0c19fbe8000000000000000000000000d9145cce52d386f254917e481eb44e9943f39138",
                )

            val result = EventUtils.decodeEvent(event, randomAbiElement)
            println(result)
            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues =
                            mapOf(
                                "addresses" to
                                    listOf(
                                        "0xf8e81D47203A594245E36C48e151709F0C19fBe8",
                                        "0xd9145CCE52D386f254917e481eB44e9943F39138",
                                    ),
                            ),
                        eventType = "Addresses",
                    ),
                )
        }

        @Test
        fun `should decode tuple correctly `() {
            val randomAbiElement =
                AbiElement(
                    name = "StructTest",
                    type = "event",
                    anonymous = false,
                    stateMutability = null,
                    inputs =
                        listOf(
                            InputOutput(
                                type = "tuple",
                                name = "person",
                                internalType = "struct TestEvents.Person",
                                indexed = false,
                                components =
                                    listOf(
                                        InputOutput(
                                            type = "string",
                                            name = "name",
                                            internalType = "string",
                                        ),
                                        InputOutput(
                                            type = "uint256",
                                            name = "age",
                                            internalType = "uint256",
                                        ),
                                        InputOutput(
                                            type = "string[]",
                                            name = "fruit",
                                            internalType = "string[]",
                                        ),
                                        InputOutput(
                                            type = "address[]",
                                            name = "wallets",
                                            internalType = "address[]",
                                        ),
                                    ),
                            ),
                        ),
                    outputs = emptyList(),
                    signature = "yourKeccakHashHere",
                )

            val event =
                TxEvent(
                    address = "0xd9145CCE52D386f254917e481eB44e9943F39138",
                    topics =
                        listOf(
                            "0x47d28b546cfbaa97e724e43a14c2fcfffb8aaf78ce3aaaae2efa7e34944cca50",
                        ),
                    data =
                        @Suppress("ktlint:standard:max-line-length")
                        "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000000000000002c000000000000000000000000000000000000000000000000000000000000000044a6f686e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000e00000000000000000000000000000000000000000000000000000000000000120000000000000000000000000000000000000000000000000000000000000016000000000000000000000000000000000000000000000000000000000000001a000000000000000000000000000000000000000000000000000000000000000054170706c6500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000054d616e676f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000642616e616e61000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000064f72616e67650000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000647726170657300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000f8e81d47203a594245e36c48e151709f0c19fbe8000000000000000000000000d9145cce52d386f254917e481eb44e9943f39138",
                )

            val result = EventUtils.decodeEvent(event, randomAbiElement)
            println(result)
            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues =
                            mapOf(
                                "person" to
                                    mapOf(
                                        "name" to "John",
                                        "age" to BigInteger("10"),
                                        "fruit" to
                                            listOf("Apple", "Mango", "Banana", "Orange", "Grapes"),
                                        "wallets" to
                                            listOf(
                                                "0xf8e81d47203a594245e36c48e151709f0c19fbe8",
                                                "0xd9145cce52d386f254917e481eb44e9943f39138",
                                            ),
                                    ),
                            ),
                        eventType = "StructTest",
                    ),
                )
        }

        @Test
        fun `should events with different types correctly `() {
            val randomAbiElement =
                AbiElement(
                    name = "EventTest",
                    type = "event",
                    anonymous = false,
                    stateMutability = null,
                    inputs =
                        listOf(
                            InputOutput(
                                type = "string",
                                name = "name",
                                internalType = "string",
                                indexed = false,
                            ),
                            InputOutput(
                                type = "string[]",
                                name = "names",
                                internalType = "string[]",
                                indexed = false,
                            ),
                            InputOutput(
                                type = "address[]",
                                name = "addresses",
                                internalType = "address[]",
                                indexed = false,
                            ),
                        ),
                    outputs = emptyList(),
                    signature = "34c98f23004767b9ed37733437325f103ec0d3b244d31fb2989deaf75dbcc94b",
                )

            val event =
                TxEvent(
                    address = "0xd9145CCE52D386f254917e481eB44e9943F39138",
                    topics =
                        listOf(
                            "0x34c98f23004767b9ed37733437325f103ec0d3b244d31fb2989deaf75dbcc94b",
                        ),
                    data =
                        @Suppress("ktlint:standard:max-line-length")
                        "0x000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000002a000000000000000000000000000000000000000000000000000000000000000046a6f686e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000e00000000000000000000000000000000000000000000000000000000000000120000000000000000000000000000000000000000000000000000000000000016000000000000000000000000000000000000000000000000000000000000001a000000000000000000000000000000000000000000000000000000000000000054170706c6500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000054d616e676f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000642616e616e61000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000064f72616e67650000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000647726170657300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000f8e81d47203a594245e36c48e151709f0c19fbe8000000000000000000000000d9145cce52d386f254917e481eb44e9943f39138",
                )

            val result = EventUtils.decodeEvent(event, randomAbiElement)
            println(result)
            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues =
                            mapOf(
                                "name" to "john",
                                "names" to listOf("Apple", "Mango", "Banana", "Orange", "Grapes"),
                                "addresses" to
                                    listOf(
                                        "0xf8e81d47203a594245e36c48e151709f0c19fbe8",
                                        "0xd9145cce52d386f254917e481eb44e9943f39138",
                                    ),
                            ),
                        eventType = "EventTest",
                    ),
                )
        }

        @Test
        fun `should handle gracefully if extracting data is out of bounds`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = "0x",
                )

            val result = EventUtils.decodeEvent(event, transferAbiElement)

            expectThat(result)
                .isEqualTo(
                    GenericEventParameters(
                        returnValues =
                            mapOf(
                                "from" to "0x0000000000000000000000000000000000000000",
                                "to" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                                "value" to "0x",
                            ),
                        eventType = "Transfer",
                    ),
                )
        }
    }

    @Nested
    inner class GetEventSignature {
        @Test
        fun `should return correct event signature given canonical event name`() {
            val canonicalName = "Transfer(address,address,uint256)"
            val expectedSignature =
                "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            EventUtils.getEventSignature(canonicalName).let {
                expectThat(it).isEqualTo(expectedSignature)
            }
        }
    }

    @Nested
    inner class IsEventValid {
        @Test
        fun `should return true if event is valid`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), emptyList()))
                .isEqualTo(true)
        }

        @Test
        fun `should return true if event is valid and the contract address matches`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(
                EventUtils.isEventValid(
                    event,
                    listOf(transferAbiElement),
                    listOf(transferEvent.address),
                ),
            ).isEqualTo(true)
        }

        @Test
        fun `should return true if event is valid and the contract address matches - case insensitive`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(
                EventUtils.isEventValid(
                    event,
                    listOf(transferAbiElement),
                    listOf(transferEvent.address.uppercase(Locale.getDefault())),
                ),
            ).isEqualTo(true)
        }

        @Test
        fun `should return false if not the contract address is not in the list`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(
                EventUtils.isEventValid(
                    event,
                    listOf(transferAbiElement),
                    listOf("0xdeaddeaddeaddeaddeaddeaddead"),
                ),
            ).isEqualTo(false)
        }

        @Test
        fun `should return false if event is invalid`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = listOf("invalid signature"),
                    data = transferEvent.data,
                )

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), emptyList()))
                .isEqualTo(false)
        }
    }
}
