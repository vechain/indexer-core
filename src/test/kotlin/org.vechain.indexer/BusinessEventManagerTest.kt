package org.vechain.indexer

import io.mockk.unmockkAll
import org.junit.jupiter.api.*
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.helpers.FileLoaderHelper
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
        fun `should load business events when a valid file stream passed in`() {
            val testBusinessEventPath = "business-events"
            val fileStreams = FileLoaderHelper.loadJsonFilesFromPath(testBusinessEventPath)
            businessEventManager.loadBusinessEvents(fileStreams)

            val loadedEvents = businessEventManager.getAllBusinessEvents()
            expectThat(loadedEvents).containsKeys("SampleEvent1", "SampleEvent2")
        }
    }

    @Nested
    inner class GetAbisTests {
        @Test
        fun `should return loaded ABIs`() {
            val fileStreams = FileLoaderHelper.loadJsonFilesFromPath("business-events")
            businessEventManager.loadBusinessEvents(fileStreams)

            val events = businessEventManager.getAllBusinessEvents()
            expectThat(events).containsKeys("SampleEvent1", "SampleEvent2")
        }
    }

    @Nested
    inner class GetEventsByNamesTests {
        @Test
        fun `should return matching event for business event name`() {
            val fileStreams = FileLoaderHelper.loadJsonFilesFromPath("business-events")
            businessEventManager.loadBusinessEvents(fileStreams)

            val events = businessEventManager.getBusinessEventsByNames(listOf("SampleEvent1"))
            expectThat(events.size).isEqualTo(1)
            expectThat(events).containsKeys("SampleEvent1")
        }

        @Test
        fun `should return empty list if no matching events`() {
            val fileStreams = FileLoaderHelper.loadJsonFilesFromPath("business-events")
            businessEventManager.loadBusinessEvents(fileStreams)

            val events = businessEventManager.getBusinessEventsByNames(listOf("randomEvent"))
            expectThat(events).isEqualTo(emptyMap())
        }
    }

    @Nested
    inner class GetBusinessGenericEventNamesTests {
        @Test
        fun `should return matching generic events for given business event definition`() {
            val fileStreams = FileLoaderHelper.loadJsonFilesFromPath("business-events")
            businessEventManager.loadBusinessEvents(fileStreams)

            val events = businessEventManager.getBusinessGenericEventNames(listOf("SampleEvent2"))
            expectThat(events.size).isEqualTo(1)
            expectThat(events).isEqualTo(listOf("Test"))
        }

        @Test
        fun `should return empty list if no matching events`() {
            val fileStreams = FileLoaderHelper.loadJsonFilesFromPath("business-events")
            businessEventManager.loadBusinessEvents(fileStreams)

            val events = businessEventManager.getBusinessGenericEventNames(listOf("randomEvent"))
            expectThat(events).isEqualTo(emptyList())
        }
    }
}
