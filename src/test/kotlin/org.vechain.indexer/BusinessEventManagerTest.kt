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

    @BeforeAll
    fun setUp() {
        businessEventManager = BusinessEventManager()
    }

    @AfterAll
    fun tearDown() {
        unmockkAll()
    }

    @Nested
    inner class LoadBusinessEventsTests {
        @Test
        fun `should load ABIs from a valid directory`() {
            val testBusinessEventPath = "business-events"

            // Load the ABIs directly from files
            businessEventManager.loadBusinessEvents(testBusinessEventPath)

            val loadedEvents = businessEventManager.getAllBusinessEvents()
            expectThat(loadedEvents).containsKeys("SampleEvent1", "SampleEvent2")
        }

        @Test
        fun `should throw exception for invalid directory`() {
            val invalidPath = "invalid-events"

            val exception =
                Assertions.assertThrows(IllegalArgumentException::class.java) {
                    businessEventManager.loadBusinessEvents(invalidPath)
                }
            expectThat(exception.message).isEqualTo("Invalid business events directory: $invalidPath")
        }
    }

    @Nested
    inner class GetAbisTests {
        @Test
        fun `should return loaded ABIs`() {
            val testBusinessEventPath = "business-events"
            businessEventManager.loadBusinessEvents(testBusinessEventPath)

            val events = businessEventManager.getAllBusinessEvents()
            expectThat(events).containsKeys("SampleEvent1", "SampleEvent2")
        }
    }

    @Nested
    inner class GetEventsByNamesTests {
        @Test
        fun `should return matching event for business event name`() {
            val testBusinessEventPath = "business-events"
            businessEventManager.loadBusinessEvents(testBusinessEventPath)

            val events = businessEventManager.getBusinessEventsByNames(listOf("SampleEvent1"))
            expectThat(events.size).isEqualTo(1)
            expectThat(events).containsKeys("SampleEvent1")
        }

        @Test
        fun `should return empty list if no matching events`() {
            val testBusinessEventPath = "business-events"
            businessEventManager.loadBusinessEvents(testBusinessEventPath)
            val events = businessEventManager.getBusinessEventsByNames(listOf("randomEvent"))
            expectThat(events).isEqualTo(emptyMap())
        }
    }

    @Nested
    inner class GetBusinessGenericEventNamesTests {
        @Test
        fun `should return matching generic events for given business event definition`() {
            val testBusinessEventPath = "business-events"
            businessEventManager.loadBusinessEvents(testBusinessEventPath)

            val events = businessEventManager.getBusinessGenericEventNames(listOf("SampleEvent2"))
            expectThat(events.size).isEqualTo(1)
            expectThat(events).isEqualTo(listOf("Test"))
        }

        @Test
        fun `should return empty list if no matching events`() {
            val testBusinessEventPath = "business-events"
            businessEventManager.loadBusinessEvents(testBusinessEventPath)
            val events = businessEventManager.getBusinessGenericEventNames(listOf("randomEvent"))
            expectThat(events).isEqualTo(emptyList())
        }
    }
}
