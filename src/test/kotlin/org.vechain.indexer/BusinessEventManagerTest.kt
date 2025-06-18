package org.vechain.indexer

import io.mockk.unmockkAll
import org.junit.jupiter.api.*
import org.vechain.indexer.event.BusinessEventManager
import strikt.api.expectThat
import strikt.assertions.containsKeys
import strikt.assertions.isEqualTo

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BusinessEventManagerTest {
    private lateinit var businessEventManager: BusinessEventManager

    @AfterAll
    fun tearDown() {
        unmockkAll()
    }

    @Nested
    inner class LoadBusinessEventsTests {
        @Test
        fun `should load business events when a valid file stream passed in`() {
            businessEventManager = BusinessEventManager("business-events", emptyMap())

            val loadedEvents = businessEventManager.getAllBusinessEvents()
            expectThat(loadedEvents).containsKeys("SampleEvent1", "SampleEvent2")
        }
    }

    @Nested
    inner class GetAbisTests {
        @Test
        fun `should return loaded ABIs`() {
            businessEventManager = BusinessEventManager("business-events", emptyMap())

            val events = businessEventManager.getAllBusinessEvents()
            expectThat(events).containsKeys("SampleEvent1", "SampleEvent2")
        }
    }

    @Nested
    inner class GetEventsByNamesTests {
        @Test
        fun `should return matching event for business event name`() {
            businessEventManager = BusinessEventManager("business-events", emptyMap())

            val events = businessEventManager.getBusinessEventsByNames(listOf("SampleEvent1"))
            expectThat(events.size).isEqualTo(1)
            expectThat(events).containsKeys("SampleEvent1")
        }

        @Test
        fun `should return empty list if no matching events`() {
            businessEventManager = BusinessEventManager("business-events", emptyMap())

            val events = businessEventManager.getBusinessEventsByNames(listOf("randomEvent"))
            expectThat(events).isEqualTo(emptyMap())
        }
    }

    @Nested
    inner class GetBusinessGenericEventNamesTests {
        @Test
        fun `should return matching generic events for given business event definition`() {
            businessEventManager = BusinessEventManager("business-events", emptyMap())

            val events = businessEventManager.getBusinessGenericEventNames(listOf("SampleEvent2"))
            expectThat(events.size).isEqualTo(1)
            expectThat(events).isEqualTo(listOf("Test"))
        }

        @Test
        fun `should return empty list if no matching events`() {
            businessEventManager = BusinessEventManager("business-events", emptyMap())

            val events = businessEventManager.getBusinessGenericEventNames(listOf("randomEvent"))
            expectThat(events).isEqualTo(emptyList())
        }
    }
}
