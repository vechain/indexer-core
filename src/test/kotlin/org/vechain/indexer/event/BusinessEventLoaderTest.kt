package org.vechain.indexer.event

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BusinessEventLoaderTest {

    @Test
    fun `loadBusinessEvents returns all business events if no eventNames provided `() {
        val businessEvents =
            BusinessEventLoader.loadBusinessEvents(
                basePath = "business-events/b3tr",
                eventNames = emptyList()
            )

        // Should return all business events defined in the files
        assert(businessEvents.size == 1) {
            "Expected 2 business events, but got ${businessEvents.size}"
        }
        assert(businessEvents.any { it.name == "B3TR_ActionReward" }) {
            "B3TR_ActionReward event not found"
        }
    }

    @Test
    fun `loadBusinessEvents preforms substitution`() {
        val businessEvents =
            BusinessEventLoader.loadBusinessEvents(
                basePath = "business-events/substitution",
                eventNames = emptyList(),
                envParams = mapOf("REPLACE_ME" to "Blah")
            )

        // The first event name should be substituted
        assert(businessEvents.size == 1) {
            "Expected 1 business event, but got ${businessEvents.size}"
        }
        assert(businessEvents[0].events.size == 1) {
            "Expected 1 event in the business event, but got ${businessEvents[0].events.size}"
        }
        assert(businessEvents[0].events[0].name == "Blah") {
            "Expected event name to be 'Blah', but got '${businessEvents[0].events[0].name}'"
        }
    }

    @Test
    fun `loadBusinessEvents filters by event names`() {
        val businessEvents =
            BusinessEventLoader.loadBusinessEvents(
                basePath = "business-events/b3tr",
                eventNames = listOf("B3TR_ActionReward")
            )

        // Should return only the specified business event
        assert(businessEvents.size == 1) {
            "Expected 1 business event, but got ${businessEvents.size}"
        }
        assert(businessEvents[0].name == "B3TR_ActionReward") {
            "Expected B3TR_ActionReward event, but got ${businessEvents[0].name}"
        }
    }

    @Test
    fun `loadBusinessEvents throws exception for non-existent event name`() {
        try {
            BusinessEventLoader.loadBusinessEvents(
                basePath = "business-events/b3tr",
                eventNames = listOf("NonExistentEvent")
            )
            assert(false) { "Expected IllegalArgumentException for non-existent event name" }
        } catch (e: IllegalArgumentException) {
            assert(e.message?.contains("NonExistentEvent") == true) {
                "Unexpected exception message: ${e.message}"
            }
        }
    }

    @Test
    fun `loadBusinessEvents should return only distinct events`() {
        val businessEvents =
            BusinessEventLoader.loadBusinessEvents(
                basePath = "business-events/duplicate",
                eventNames = emptyList()
            )

        // Should return only distinct business events
        assert(businessEvents.size == 1) {
            "Expected 1 distinct business event, but got ${businessEvents.size}"
        }
        assert(businessEvents[0].name == "B3TR_ActionReward") {
            "Expected B3TR_ActionReward event, but got ${businessEvents[0].name}"
        }
    }

    @Test
    fun `loadBusinessEvents should return only distinct events 2`() {
        val businessEvents =
            BusinessEventLoader.loadBusinessEvents(
                basePath = "business-events/b3tr",
                eventNames = listOf("B3TR_ActionReward", "B3TR_ActionReward")
            )

        // Should return only distinct business events
        assert(businessEvents.size == 1) {
            "Expected 1 distinct business event, but got ${businessEvents.size}"
        }
        assert(businessEvents[0].name == "B3TR_ActionReward") {
            "Expected B3TR_ActionReward event, but got ${businessEvents[0].name}"
        }
    }
}
