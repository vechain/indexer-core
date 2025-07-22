package org.vechain.indexer.event

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.fixtures.BlockFixtures.BLOCK_STARGATE_BASE_REWARD
import strikt.api.expectThat
import strikt.assertions.isNotEmpty

@ExtendWith(MockKExtension::class)
class BusinessEventProcessorTest {

    @Nested
    inner class ProcessBlockEvents {

        private lateinit var eventProcessor: BusinessEventProcessor

        @BeforeEach
        fun setUp() {
            eventProcessor =
                BusinessEventProcessor(
                    businessEventBasePath = "business-events/stargate",
                    abiBasePath = "test-abis/stargate",
                    businessEventNames = listOf("STARGATE_CLAIM_REWARDS_BASE"),
                    businessEventContracts = emptyList(),
                    substitutionParams =
                        mapOf(
                            "STARGATE_NFT_CONTRACT" to "0x1856c533ac2d94340aaa8544d35a5c1d4a21dee7",
                            "STARGATE_DELEGATION_CONTRACT" to
                                "0x4cb1c9ef05b529c093371264fab2c93cc6cddb0e"
                        )
                )
        }

        @Test
        fun `should process STARGATE_CLAIM_REWARDS_BASE events`() {
            val indexedEvents = eventProcessor.processEvents(BLOCK_STARGATE_BASE_REWARD)

            expectThat(indexedEvents).isNotEmpty()
        }
    }
}
