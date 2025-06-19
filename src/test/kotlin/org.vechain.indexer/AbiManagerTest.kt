package org.vechain.indexer

import io.mockk.unmockkAll
import org.junit.jupiter.api.*
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.fixtures.FileFixtures.abiFiles
import strikt.api.expectThat
import strikt.assertions.containsKeys
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AbiManagerTest {
    private lateinit var abiManager: AbiManager

    @AfterAll
    fun tearDown() {
        unmockkAll()
    }

    @Nested
    inner class LoadAbisTests {
        @Test
        fun `should load ABIs when a valid file stream passed in`() {
            abiManager = AbiManager(abiFiles)

            val loadedAbis = abiManager.getAbis()
            expectThat(loadedAbis).containsKeys("SampleABI1", "SampleABI2")
            loadedAbis["SampleABI1"]?.let { expectThat(it.size).isEqualTo(2) }
            loadedAbis["SampleABI2"]?.let { expectThat(it.size).isEqualTo(2) }
        }
    }

    @Nested
    inner class GetAbisTests {
        @Test
        fun `should return loaded ABIs`() {
            abiManager = AbiManager(abiFiles)

            val abis = abiManager.getAbis()
            expectThat(abis).containsKeys("SampleABI1", "SampleABI2")
            abis["SampleABI1"]?.let { expectThat(it.size).isEqualTo(2) }
            abis["SampleABI2"]?.let { expectThat(it.size).isEqualTo(2) }
        }
    }

    @Nested
    inner class GetEventsByNamesTests {
        @Test
        fun `should return matching events for given ABI and event names`() {
            abiManager = AbiManager(abiFiles)

            val events = abiManager.getEventsByNames(listOf("SampleABI1"), listOf("Event1"))
            expectThat(events).hasSize(1)
            expectThat(events[0].name).isEqualTo("Event1")
        }

        @Test
        fun `should return empty list if no matching events`() {
            val events =
                abiManager.getEventsByNames(listOf("NonExistentABI"), listOf("NonExistentEvent"))
            expectThat(events).hasSize(0)
        }
    }
}
