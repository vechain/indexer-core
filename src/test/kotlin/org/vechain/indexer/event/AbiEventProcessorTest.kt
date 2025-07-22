package org.vechain.indexer.event

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.event.utils.EventUtils.generateEventId
import org.vechain.indexer.fixtures.*
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_BASE_REWARD
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_STAKE
import org.vechain.indexer.fixtures.ContractAddresses.STARGATE_DELEGATION_CONTRACT
import org.vechain.indexer.fixtures.ContractAddresses.STARGATE_NFT_CONTRACT
import org.vechain.indexer.fixtures.ContractAddresses.VTHO_CONTRACT
import org.vechain.indexer.fixtures.EventLogFixtures.LOGS_B3TR_ACTION
import org.vechain.indexer.thor.model.*
import strikt.api.expectThat
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty
import strikt.assertions.isNotNull
import strikt.assertions.isNull

@ExtendWith(MockKExtension::class)
class AbiEventProcessorTest {

    @Nested
    inner class DecodeEvent {
        @Test
        fun `decodeEvent should decode valid event`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/b3tr",
                    eventNames = emptyList(),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val block = BlockFixtures.BLOCK_B3TR_ACTION
            val tx = block.transactions.first()

            val outputIndex = 0
            val eventIndex = 0
            val event = tx.outputs[outputIndex].events[eventIndex]

            val result =
                processor.publicDecodeEvent(
                    event = event,
                    tx = tx,
                    block = block,
                    outputIndex = outputIndex,
                    eventIndex = eventIndex
                )
            expectThat(result).isNotNull().and {
                get { id }.isEqualTo(generateEventId(tx.id, outputIndex, eventIndex, event))
                get { blockId }.isEqualTo(block.id)
                get { blockNumber }.isEqualTo(block.number)
                get { blockTimestamp }.isEqualTo(block.timestamp)
                get { txId }.isEqualTo(tx.id)
                get { origin }.isEqualTo(tx.origin)
                get { paid }.isEqualTo(tx.paid)
                get { gasUsed }.isEqualTo(tx.gasUsed)
                get { gasPayer }.isEqualTo(tx.gasPayer)
                get { raw }.isNotNull()
                get { params.params }.isNotEmpty()
                get { address }.isEqualTo(event.address)
                get { eventType }.isEqualTo("RewardDistributed")
                get { clauseIndex }.isEqualTo(outputIndex.toLong())
            }
        }

        @Test
        fun `decodeEvent return null if no event matches`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stringsabi",
                    eventNames = listOf(),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val block = BlockFixtures.BLOCK_B3TR_ACTION
            val tx = block.transactions.first()
            val outputIndex = 0
            val eventIndex = 0
            val event = tx.outputs[outputIndex].events[eventIndex]

            val result =
                processor.publicDecodeEvent(
                    event = event,
                    tx = tx,
                    block = block,
                    outputIndex = outputIndex,
                    eventIndex = eventIndex
                )

            expectThat(result).isNull()
        }
    }

    @Nested
    inner class DecodeLogEvents {

        @Test
        fun `decodeLogEvents should decode valid log events`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/b3tr",
                    eventNames = listOf("RewardDistributed"),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val block = BlockFixtures.BLOCK_B3TR_ACTION
            val tx = block.transactions.first()
            val logs = LOGS_B3TR_ACTION

            val result = processor.publicDecodeLogEvents(logs)

            expectThat(result).isNotEmpty()
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]) {
                get { blockId }.isEqualTo(block.id)
                get { blockNumber }.isEqualTo(block.number)
                get { blockTimestamp }.isEqualTo(block.timestamp)
                get { txId }.isEqualTo(tx.id)
                get { origin }.isEqualTo(tx.origin)
                get { paid }.isNull()
                get { gasUsed }.isNull()
                get { gasPayer }.isNull()
                get { raw }.isNotNull()
                get { params.params }.isNotEmpty()
                get { eventType }.isEqualTo("RewardDistributed")
                get { clauseIndex }.isEqualTo(0)
            }
        }

        @Test
        fun `decodeLogEvents should return empty list if no logs match`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stringsabi",
                    eventNames = listOf(),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val logs = LOGS_B3TR_ACTION

            val result = processor.publicDecodeLogEvents(logs)

            expectThat(result).isEmpty()
        }
    }

    @Nested
    inner class DecodeLogTransfers {

        @Test
        fun `decodeLogTransfers should return events for valid transfer logs`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stringsabi",
                    eventNames = listOf(),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val log =
                TransferLog(
                    sender = "sender",
                    recipient = "recipient",
                    amount = "100",
                    meta =
                        EventMeta(
                            txID = "tx1",
                            blockID = "block1",
                            blockNumber = 1L,
                            blockTimestamp = 1000L,
                            txOrigin = "origin1",
                            clauseIndex = 0
                        ),
                )

            val result = processor.publicDecodeLogTransfers(listOf(log))

            expectThat(result).isNotEmpty()
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0]) {
                get { id }.isEqualTo("tx1-0-0--1837219028")
                get { blockId }.isEqualTo(log.meta.blockID)
                get { blockNumber }.isEqualTo(log.meta.blockNumber)
                get { blockTimestamp }.isEqualTo(log.meta.blockTimestamp)
                get { txId }.isEqualTo(log.meta.txID)
                get { origin }.isEqualTo(log.meta.txOrigin)
                get { params.params }.isNotEmpty()
                get { eventType }.isEqualTo("VET_TRANSFER")
                get { clauseIndex }.isEqualTo(log.meta.clauseIndex.toLong())
            }
        }

        @Test
        fun `decodeLogTransfers should return empty list for empty logs`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stringsabi",
                    eventNames = listOf(),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val result = processor.publicDecodeLogTransfers(emptyList())

            expectThat(result).isEmpty()
        }
    }

    @Nested
    inner class ProcessBlockEvents {
        @Test
        fun `should extract Transfer events stargate events`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stargate",
                    eventNames = listOf("Transfer"),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val events = processor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(events).isNotEmpty()
        }

        @Test
        fun `should extract BaseVTHORewardsClaimed events stargate events`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stargate",
                    eventNames = listOf("BaseVTHORewardsClaimed"),
                    contractAddresses = emptyList(),
                    includeVetTransfers = false
                )

            val events = processor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(events).isNotEmpty()
            expectThat(events.size).isEqualTo(5)
            expectThat(events[0].eventType).isEqualTo("BaseVTHORewardsClaimed")
        }

        @Test
        fun `should extract event when filtering by contract address`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stargate",
                    eventNames = listOf("BaseVTHORewardsClaimed"),
                    contractAddresses = listOf(STARGATE_NFT_CONTRACT),
                    includeVetTransfers = false
                )

            val events = processor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(events).isNotEmpty()
            expectThat(events.size).isEqualTo(5)
            expectThat(events[0].eventType).isEqualTo("BaseVTHORewardsClaimed")
        }

        @Test
        fun `should return VET_TRANSFER events`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/stargate",
                    eventNames = listOf("TokenMinted"),
                    contractAddresses =
                        listOf(STARGATE_DELEGATION_CONTRACT, STARGATE_NFT_CONTRACT, VTHO_CONTRACT),
                    includeVetTransfers = true
                )

            val events = processor.processEvents(BLOCK_STARGATE_STAKE)
            expectThat(events).isNotEmpty()
            expectThat(events.size).isEqualTo(2)
            expectThat(events[0].eventType).isEqualTo("TokenMinted")
            expectThat(events[1].eventType).isEqualTo("VET_TRANSFER")
        }

        @Test
        fun `should return NFT transfer events`() {
            val processor =
                TestableAbiEventProcessor(
                    basePath = "test-abis/tokens",
                    eventNames = listOf("Transfer", "TransferSingle", "TransferBatch"),
                    contractAddresses = emptyList(),
                    includeVetTransfers = true
                )

            val events = processor.processEvents(BlockFixtures.BLOCK_NFT_TRANSFER)

            expectThat(events).isNotEmpty()
            expectThat(events.size).isEqualTo(1)
            expectThat(events[0].eventType).isEqualTo("Transfer")
        }
    }

    private class TestableAbiEventProcessor(
        basePath: String,
        eventNames: List<String>,
        contractAddresses: List<String>,
        includeVetTransfers: Boolean
    ) : AbiEventProcessor(basePath, eventNames, contractAddresses, includeVetTransfers) {

        fun publicDecodeEvent(
            event: TxEvent,
            tx: Transaction,
            block: Block,
            outputIndex: Int,
            eventIndex: Int
        ) = decodeEvent(event, tx, block, outputIndex, eventIndex)

        fun publicDecodeLogEvents(logs: List<EventLog>) = decodeLogEvents(logs)

        fun publicDecodeLogTransfers(logs: List<TransferLog>) = decodeLogTransfers(logs)

        fun publicExtractVetTransfers(
            output: TxOutputs,
            tx: Transaction,
            block: Block,
            outputIndex: Int
        ) = extractVetTransfers(output, tx, block, outputIndex)
    }
}
