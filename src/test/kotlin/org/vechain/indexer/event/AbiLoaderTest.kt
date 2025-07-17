package org.vechain.indexer.event

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AbiLoaderTest {
    @Test
    fun `load returns all events if no eventNames provided`() {
        val events =
            AbiLoader.load(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                names = emptyList(),
                typeFilter = "event"
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
    fun `load filters by event names`() {
        val events =
            AbiLoader.load(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                names = listOf("ABIChanged", "AddrChanged"),
                typeFilter = "event"
            )

        // Should return only the specified events
        assert(events.size == 2) { "Expected 2 events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
        assert(events.any { it.name == "AddrChanged" }) { "AddrChanged event not found" }
    }

    @Test
    fun `load throws exception for non-existent event name`() {
        try {
            AbiLoader.load(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                names = listOf("NonExistentEvent"),
                typeFilter = "event"
            )
            assert(false) { "Expected IllegalArgumentException for non-existent event name" }
        } catch (e: IllegalArgumentException) {
            assert(e.message?.contains("NonExistentEvent") == true) {
                "Unexpected exception message: ${e.message}"
            }
        }
    }

    @Test
    fun `load should return only distinct events`() {
        val events =
            AbiLoader.load(
                abiFiles = listOf("test-abis/stringsAbis.json", "test-abis/stringsAbis.json"),
                names = emptyList(),
                typeFilter = "event"
            )

        // Should return only distinct events
        assert(events.size == 14) { "Expected 14 distinct events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
    }

    @Test
    fun `load should return only distinct events 2`() {
        val events =
            AbiLoader.load(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                names = listOf("ABIChanged", "AddrChanged"),
                typeFilter = "event"
            )

        // Should return only distinct events
        assert(events.size == 2) { "Expected 2 distinct events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
        assert(events.any { it.name == "AddrChanged" }) { "AddrChanged event not found" }
    }

    @Test
    fun `load should throw and exception if file is not found`() {
        try {
            AbiLoader.load(
                abiFiles = listOf("non-existent-file.json"),
                names = emptyList(),
                typeFilter = "event"
            )
            assert(false) { "Expected IllegalStateException for non-existent file" }
        } catch (e: IllegalStateException) {
            assert(e.message?.contains("non-existent-file.json") == true) {
                "Unexpected exception message: ${e.message}"
            }
        }
    }

    @Test
    fun `load performs substitution with envParams`() {
        val events =
            AbiLoader.load(
                abiFiles = listOf("test-abis/substitution-test.json"),
                names = emptyList(),
                typeFilter = "event",
                substitutionParams = mapOf("REPLACE_ME" to "Blah")
            )

        // Should return events with substituted values
        assert(events.size == 2) { "Expected 2 event, but got ${events.size}" }
        assert(events.any { it.name == "Blah" }) { "Blah event not found" }
    }

    @Test
    fun `load will load a different type of ABI`() {
        val functions =
            AbiLoader.load(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                names = listOf("approve", "clearRecords"),
                typeFilter = "function"
            )

        // Should return all functions defined in the files
        assert(functions.size == 2) { "Expected 2 functions, but got ${functions.size}" }
        assert(functions.any { it.name == "approve" }) { "approve function not found" }
        assert(functions.any { it.name == "clearRecords" }) { "clearRecords function not found" }
    }

    @Test
    fun `loadEvents should return events`() {
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
    fun `loadFunctions should return functions`() {
        val functions =
            AbiLoader.loadFunctions(
                abiFiles = listOf("test-abis/stringsAbis.json"),
                functionNames = listOf("approve", "clearRecords")
            )

        // Should return only the specified functions
        assert(functions.size == 2) { "Expected 2 functions, but got ${functions.size}" }
        assert(functions.any { it.name == "approve" }) { "approve function not found" }
        assert(functions.any { it.name == "clearRecords" }) { "clearRecords function not found" }
    }
}
