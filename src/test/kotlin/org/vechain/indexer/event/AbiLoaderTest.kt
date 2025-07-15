package org.vechain.indexer.event

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AbiLoaderTest {
    @Test
    fun `loadEvents returns all events if no eventNames provided`() {
        val events =
            AbiLoader.loadEvents(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                eventNames = emptyList(),
            )

        // Should return all events defined in the files
        assert(events.size == 14) { "Expected 14 events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
        assert(events.any { it.name == "AddrChanged" }) { "AddrChanged event not found" }
        assert(events.any { it.name == "AddressChanged" }) { "AddressChanged event not found" }
        assert(events.any { it.name == "ApprovalForAll" }) { "ApprovalForAll event not found" }
        assert(events.any { it.name == "Approved" }) { "Approved event not found" }
        assert(events.any { it.name == "ContenthashChanged" }) {
            "ContenthashChanged event not found"
        }
        assert(events.any { it.name == "DNSRecordChanged" }) { "DNSRecordChanged event not found" }
        assert(events.any { it.name == "DNSRecordDeleted" }) { "DNSRecordDeleted event not found" }
        assert(events.any { it.name == "DNSZonehashChanged" }) {
            "DNSZonehashChanged event not found"
        }
        assert(events.any { it.name == "InterfaceChanged" }) { "InterfaceChanged event not found" }
        assert(events.any { it.name == "NameChanged" }) { "NameChanged event not found" }
        assert(events.any { it.name == "PubkeyChanged" }) { "PubkeyChanged event not found" }
        assert(events.any { it.name == "TextChanged" }) { "TextChanged event not found" }
        assert(events.any { it.name == "VersionChanged" }) { "VersionChanged event not found" }
    }

    @Test
    fun `loadEvents filters by event names`() {
        val events =
            AbiLoader.loadEvents(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                eventNames = listOf("ABIChanged", "AddrChanged")
            )

        // Should return only the specified events
        assert(events.size == 2) { "Expected 2 events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
        assert(events.any { it.name == "AddrChanged" }) { "AddrChanged event not found" }
    }

    @Test
    fun `loadEvents throws exception for non-existent event name`() {
        try {
            AbiLoader.loadEvents(
                abiFiles = listOf("test-abis/stringsAbis.json"),
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
    fun `loadEvents should return only distinct events`() {
        val events =
            AbiLoader.loadEvents(
                abiFiles = listOf("test-abis/stringsAbis.json", "test-abis/stringsAbis.json"),
                eventNames = emptyList()
            )

        // Should return only distinct events
        assert(events.size == 14) { "Expected 14 distinct events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
    }

    @Test
    fun `loadEvents should return only distinct events 2`() {
        val events =
            AbiLoader.loadEvents(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                eventNames = listOf("ABIChanged", "AddrChanged")
            )

        // Should return only distinct events
        assert(events.size == 2) { "Expected 2 distinct events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
        assert(events.any { it.name == "AddrChanged" }) { "AddrChanged event not found" }
    }

    @Test
    fun `loadEvents should throw and exception if file is not found`() {
        try {
            AbiLoader.loadEvents(
                abiFiles = listOf("non-existent-file.json"),
                eventNames = emptyList()
            )
            assert(false) { "Expected IllegalStateException for non-existent file" }
        } catch (e: IllegalStateException) {
            assert(e.message?.contains("non-existent-file.json") == true) {
                "Unexpected exception message: ${e.message}"
            }
        }
    }

    @Test
    fun `loadEvents performs substitution with envParams`() {
        val events =
            AbiLoader.loadEvents(
                abiFiles = listOf("test-abis/substitution-test.json"),
                eventNames = emptyList(),
                substitutionParams = mapOf("REPLACE_ME" to "Blah")
            )

        // Should return events with substituted values
        assert(events.size == 2) { "Expected 2 event, but got ${events.size}" }
        assert(events.any { it.name == "Blah" }) { "Blah event not found" }
    }
}
