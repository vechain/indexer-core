package org.vechain.indexer.event

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_BASE_REWARD
import org.vechain.indexer.fixtures.ContractAddresses.STARGATE_DELEGATION_CONTRACT
import org.vechain.indexer.fixtures.ContractAddresses.STARGATE_NFT_CONTRACT
import org.vechain.indexer.fixtures.ContractAddresses.VTHO_CONTRACT
import strikt.api.expectThat
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
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT
                        )
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(indexedEvents).isNotEmpty()
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
                            "STARGATE_DELEGATION_CONTRACT" to STARGATE_DELEGATION_CONTRACT
                        )
                )
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(indexedEvents).isNotEmpty()
        }
    }
}
