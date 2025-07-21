package org.vechain.indexer.utils

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.vechain.indexer.event.model.business.BusinessEventDefinition
import org.vechain.indexer.event.model.business.Event
import org.vechain.indexer.event.utils.BusinessEventUtils.containsVetTransferEvent
import org.vechain.indexer.event.utils.BusinessEventUtils.extractAbiEventNames
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEmpty
import strikt.assertions.isFalse
import strikt.assertions.isTrue

internal class BusinessEventUtils {
    @Nested
    inner class ContainesVetTransferEvent {
        @Test
        fun `should return false if empty list`() {
            // Test logic for empty list
            val result = containsVetTransferEvent(emptyList())

            expectThat(result).isFalse()
        }

        @Test
        fun `should return true if contains vet transfer event`() {
            val vetBusinessEvent =
                BusinessEventDefinition(
                    name = "VET_TRANSFER",
                )

            // Test logic for list containing VET_TRANSFER
            val result = containsVetTransferEvent(listOf(vetBusinessEvent))

            expectThat(result).isTrue()
        }

        @Test
        fun `is case sensitive`() {
            val vetBusinessEvent =
                BusinessEventDefinition(
                    name = "vet_transfer",
                )

            // Test logic for list containing VET_TRANSFER in different case
            val result = containsVetTransferEvent(listOf(vetBusinessEvent))

            expectThat(result).isFalse()
        }

        @Test
        fun `should return false if does not contain vet transfer event`() {
            val otherBusinessEvent =
                BusinessEventDefinition(
                    name = "OTHER_EVENT",
                )

            // Test logic for list not containing VET_TRANSFER
            val result = containsVetTransferEvent(listOf(otherBusinessEvent))

            expectThat(result).isFalse()
        }
    }

    @Nested
    inner class ExtractAbiEventNames {
        @Test
        fun `should return empty list if no business events`() {
            // Test logic for empty business events
            val result = extractAbiEventNames(emptyList())

            expectThat(result).isEmpty()
        }

        @Test
        fun `should extract unique event names from business events`() {
            val businessEvents =
                listOf(
                    BusinessEventDefinition(
                        name = "EVENT_A",
                        events = listOf(Event(name = "event1"))
                    ),
                    BusinessEventDefinition(
                        name = "EVENT_B",
                        events = listOf(Event(name = "event2"))
                    ),
                    BusinessEventDefinition(
                        name = "EVENT_C",
                        events = listOf(Event(name = "event1"))
                    ),
                )

            // Test logic for extracting unique event names
            val result = extractAbiEventNames(businessEvents)

            expectThat(result).containsExactly("event1", "event2")
        }

        @Test
        fun `should filter out empty and VET_TRANSFER event names`() {
            val businessEvents =
                listOf(
                    BusinessEventDefinition(
                        name = "EVENT_A",
                        events = listOf(Event(name = "event1"))
                    ),
                    BusinessEventDefinition(
                        name = "VET_TRANSFER",
                        events = listOf(Event(name = "VET_TRANSFER"))
                    ),
                    BusinessEventDefinition(name = "EVENT_B", events = listOf(Event(name = ""))),
                )

            // Test logic for filtering out empty and VET_TRANSFER event names
            val result = extractAbiEventNames(businessEvents)

            expectThat(result).containsExactly("event1")
        }

        @Test
        fun `is case sensitive`() {
            val businessEvents =
                listOf(
                    BusinessEventDefinition(
                        name = "EVENT_A",
                        events = listOf(Event(name = "event1"))
                    ),
                    BusinessEventDefinition(
                        name = "VET_TRANSFER",
                        events = listOf(Event(name = "vet_transfer"))
                    ),
                )

            // Test logic for case sensitivity in event names
            val result = extractAbiEventNames(businessEvents)

            expectThat(result).containsExactly("event1", "vet_transfer")
        }
    }
}
