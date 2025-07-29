package org.vechain.indexer.event

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
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
    }
}
