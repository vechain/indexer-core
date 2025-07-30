package org.vechain.indexer.event

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.EventMockFactory.createIndexedEvent
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.business.Condition
import org.vechain.indexer.event.model.business.Event
import org.vechain.indexer.event.model.enums.Operator
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_BASE_REWARD
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_STAKE
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_STAKE_AND_DELEGATE
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_STAKE_DELEGATE
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_UNSTAKE
import org.vechain.indexer.fixtures.ContractAddresses.STARGATE_DELEGATION_CONTRACT
import org.vechain.indexer.fixtures.ContractAddresses.STARGATE_NFT_CONTRACT
import org.vechain.indexer.fixtures.ContractAddresses.VTHO_CONTRACT
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty

@ExtendWith(MockKExtension::class)
class BusinessEventProcessorTest {
    @Nested
    inner class ProcessBlockEvents {
        @Test
        fun `should process STARGATE_CLAIM_REWARDS_BASE events`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_CLAIM_REWARDS_BASE"),
                    businessEventContracts = emptyList(),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(indexedEvents).isNotEmpty()
        }

        @Test
        fun `should not process STARGATE_STAKE_DELEGATE if required events are already used by other business events`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames =
                        listOf("STARGATE_STAKE", "STARGATE_STAKE_DELEGATE", "STARGATE_DELEGATE"),
                    businessEventContracts = emptyList(),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_STAKE_DELEGATE)

            expectThat(indexedEvents).hasSize(2)
            expect {
                that(indexedEvents[0].eventType).isEqualTo("STARGATE_DELEGATE")
                that(indexedEvents[1].eventType).isEqualTo("STARGATE_STAKE")
            }
        }

        @Test
        fun `should not process STARGATE_STAKE if required events are already used by STARGATE_STAKE_DELEGATE`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_STAKE_DELEGATE", "STARGATE_STAKE"),
                    businessEventContracts = emptyList(),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_STAKE_DELEGATE)

            expectThat(indexedEvents).hasSize(1)
            expect { that(indexedEvents[0].eventType).isEqualTo("STARGATE_STAKE_DELEGATE") }
        }

        @Test
        fun `should process events when filtering by contract address`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_CLAIM_REWARDS_BASE"),
                    businessEventContracts = listOf(STARGATE_NFT_CONTRACT, VTHO_CONTRACT),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(indexedEvents).isNotEmpty()
        }

        @Test
        fun `should load STARGATE_STAKE events`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_STAKE"),
                    businessEventContracts =
                        listOf(STARGATE_DELEGATION_CONTRACT, STARGATE_NFT_CONTRACT, VTHO_CONTRACT),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_STAKE)

            expectThat(indexedEvents).isNotEmpty()
        }

        @Test
        fun `should load STARGATE_UNSTAKE events`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_UNSTAKE"),
                    businessEventContracts =
                        listOf(STARGATE_DELEGATION_CONTRACT, STARGATE_NFT_CONTRACT, VTHO_CONTRACT),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_UNSTAKE)

            expectThat(indexedEvents).isNotEmpty()
        }

        @Test
        fun `should process STARGATE_STAKE_AND_DELEGATE events`() {
            val eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_STAKE_DELEGATE"),
                    businessEventContracts =
                        listOf(STARGATE_DELEGATION_CONTRACT, STARGATE_NFT_CONTRACT, VTHO_CONTRACT),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to STARGATE_NFT_CONTRACT,
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT,
                        ),
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_STAKE_AND_DELEGATE)

            expectThat(indexedEvents).isNotEmpty()
        }

        @Test
        fun `should find all BE with available events`() {
            var event1 =
                createIndexedEvent(
                    address = "0x",
                    clauseIndex = 0,
                    params =
                        AbiEventParameters(
                            returnValues =
                                mapOf(
                                    "from" to "0xa52b171d88be72f2550f2ffcd166b4825656a9d7",
                                    "to" to "0x035daf5d3ab419d60d753faa5cb3b8876a97846d",
                                    "tokenId" to 45,
                                ),
                            eventType = "Transfer",
                        ),
                    id = "id1",
                )

            var event2 =
                createIndexedEvent(
                    address = "0x",
                    clauseIndex = 0,
                    params =
                        AbiEventParameters(
                            returnValues =
                                mapOf(
                                    "from" to "0xa52b171d88be72f2550f2ffcd166b4825656a9d7",
                                    "to" to "0x035daf5d3ab419d60d753faa5cb3b8876a97846d",
                                    "tokenId" to 46,
                                ),
                            eventType = "Transfer",
                        ),
                    id = "id2",
                )

            val definition1 =
                BusinessEventDefinition(
                    name = "TRANSFER_1",
                    sameClause = true,
                    events =
                        listOf(
                            Event(
                                name = "Transfer",
                                alias = "test",
                                conditions =
                                    listOf(
                                        Condition("tokenId", false, "45", true, Operator.EQ),
                                    ),
                            ),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val definition2 =
                BusinessEventDefinition(
                    name = "TRANSFER_2",
                    sameClause = true,
                    events =
                        listOf(
                            Event(
                                name = "Transfer",
                                alias = "test",
                                conditions =
                                    listOf(
                                        Condition("tokenId", false, "46", true, Operator.EQ),
                                    ),
                            ),
                            Event(
                                name = "Transfer",
                                alias = "test",
                                conditions =
                                    listOf(
                                        Condition("tokenId", false, "47", true, Operator.EQ),
                                    ),
                            ),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val definition3 =
                BusinessEventDefinition(
                    name = "TRANSFER_3",
                    sameClause = true,
                    events =
                        listOf(
                            Event(
                                name = "Transfer",
                                alias = "test",
                                conditions =
                                    listOf(
                                        Condition("tokenId", false, "46", true, Operator.EQ),
                                    ),
                            ),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val processor =
                createProcessorWithDefinition(listOf(definition1, definition2, definition3))
            val result = processor.testProcessTx(listOf(event1, event2))

            expectThat(result).hasSize(2)
            expectThat(result[0].eventType).isEqualTo("TRANSFER_1")
            expectThat(result[1].eventType).isEqualTo("TRANSFER_3")
        }

        @Test
        fun `should return no business events if conditions do not match`() {
            val event =
                createIndexedEvent(
                    address = "0x",
                    clauseIndex = 0,
                    params =
                        AbiEventParameters(
                            returnValues = mapOf("tokenId" to 99),
                            eventType = "Transfer",
                        ),
                    id = "id1",
                )

            val definition =
                BusinessEventDefinition(
                    name = "NO_MATCH",
                    sameClause = true,
                    events =
                        listOf(
                            Event(
                                name = "Transfer",
                                alias = "test",
                                conditions = listOf(Condition("tokenId", false, "42", true, Operator.EQ)),
                            ),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val processor = createProcessorWithDefinition(listOf(definition))
            val result = processor.testProcessTx(listOf(event))

            expectThat(result).hasSize(0)
        }

        @Test
        fun `should match only the first business event when same event is reused`() {
            val event =
                createIndexedEvent(
                    address = "0x",
                    clauseIndex = 0,
                    params =
                        AbiEventParameters(
                            returnValues = mapOf("tokenId" to 123),
                            eventType = "Transfer",
                        ),
                    id = "id1",
                )

            val def1 =
                BusinessEventDefinition(
                    name = "FIRST_MATCH",
                    sameClause = true,
                    events =
                        listOf(
                            Event("Transfer", "e", listOf(Condition("tokenId", false, "123", true, Operator.EQ))),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val def2 =
                BusinessEventDefinition(
                    name = "SECOND_MATCH",
                    sameClause = true,
                    events =
                        listOf(
                            Event("Transfer", "e", listOf(Condition("tokenId", false, "123", true, Operator.EQ))),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val processor = createProcessorWithDefinition(listOf(def1, def2))
            val result = processor.testProcessTx(listOf(event))

            expectThat(result).hasSize(1)
            expectThat(result[0].eventType).isEqualTo("FIRST_MATCH")
        }

        @Test
        fun `should match only if both events in group match`() {
            val event1 =
                createIndexedEvent(
                    address = "0x",
                    clauseIndex = 0,
                    params = AbiEventParameters(mapOf("tokenId" to 1), "Transfer"),
                    id = "id1",
                )
            val event2 =
                createIndexedEvent(
                    address = "0x",
                    clauseIndex = 0,
                    params = AbiEventParameters(mapOf("tokenId" to 2), "Transfer"),
                    id = "id2",
                )

            val def =
                BusinessEventDefinition(
                    name = "BOTH_MATCH",
                    sameClause = true,
                    events =
                        listOf(
                            Event("Transfer", "e1", listOf(Condition("tokenId", false, "1", true, Operator.EQ))),
                            Event("Transfer", "e2", listOf(Condition("tokenId", false, "2", true, Operator.EQ))),
                        ),
                    rules = listOf(),
                    paramsDefinition = listOf(),
                )

            val processor = createProcessorWithDefinition(listOf(def))
            val result = processor.testProcessTx(listOf(event1, event2))

            expectThat(result).hasSize(1)
            expectThat(result[0].eventType).isEqualTo("BOTH_MATCH")
        }
    }

    fun createProcessorWithDefinition(def: List<BusinessEventDefinition>): TestableBusinessEventProcessor =
        TestableBusinessEventProcessor(def)

    class TestableBusinessEventProcessor(
        private val injectedDefs: List<BusinessEventDefinition>,
    ) : BusinessEventProcessor(
            businessEventBasePath = "unused",
            abiBasePath = "unused",
            businessEventNames = emptyList(),
            businessEventContracts = emptyList(),
            substitutionParams = emptyMap(),
        ) {
        fun testProcessTx(events: List<IndexedEvent>): List<IndexedEvent> {
            val method =
                BusinessEventProcessor::class
                    .java
                    .getDeclaredMethod("processTransactionForBusinessEvents", List::class.java)
            method.isAccessible = true

            val defsField = BusinessEventProcessor::class.java.getDeclaredField("businessEvents")
            defsField.isAccessible = true
            defsField.set(this, injectedDefs)

            return method.invoke(this, events) as List<IndexedEvent>
        }
    }
}
