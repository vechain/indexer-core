package org.vechain.indexer.utils

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.vechain.indexer.EventMockFactory.transferAbiElement
import org.vechain.indexer.EventMockFactory.transferERC721AbiElement
import org.vechain.indexer.EventMockFactory.transferEvent
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.model.TxEvent
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.message
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

            expectThat(exception.message.subject).isEqualTo("Mismatch between ABI indexed inputs and event topics")
        }


        @Test
        fun `should throw error if unsupported solidity type is trying to be decoded`() {
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

            val exception =
                expectThrows<IllegalArgumentException> {
                    EventUtils.decodeEvent(event, randomAbiElement)
                }

            expectThat(exception.message.subject).isEqualTo("Unsupported Solidity type: randomType")
        }

        @Test
        fun `should throw error if extracting data is out of bounds`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = "0x",
                )

            val exception =
                expectThrows<IllegalArgumentException> {
                    EventUtils.decodeEvent(event, transferAbiElement)
                }

            expectThat(exception.message.subject).isEqualTo("Data segment out of bounds for index 0")
        }
    }

    @Nested
    inner class GetEventSignature {
        @Test
        fun `should return correct event signature given canonical event name`() {
            val canonicalName = "Transfer(address,address,uint256)"
            val expectedSignature = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
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

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), emptyList())).isEqualTo(true)
        }

        @Test
        fun `should return true if event is valid and the contract address matches`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), listOf(transferEvent.address))).isEqualTo(true)
        }

        @Test
        fun `should return true if event is valid and the contract address matches - case insensitive`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), listOf(transferEvent.address.uppercase(
                Locale.getDefault()
            )))).isEqualTo(true)
        }

        @Test
        fun `should return false if not the contract address is not in the list`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = transferEvent.topics,
                    data = transferEvent.data,
                )

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), listOf("0xdeaddeaddeaddeaddeaddeaddead"))).isEqualTo(false)
        }

        @Test
        fun `should return false if event is invalid`() {
            val event =
                TxEvent(
                    address = transferEvent.address,
                    topics = listOf("invalid signature"),
                    data = transferEvent.data,
                )

            expectThat(EventUtils.isEventValid(event, listOf(transferAbiElement), emptyList())).isEqualTo(false)
        }
    }
}