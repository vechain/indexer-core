package org.vechain.indexer.event

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.abi.InputOutput

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AbiLoaderTest {
    @Test
    fun `load returns all events if no eventNames provided`() {
        val events =
            AbiLoader.load(
                basePath = "test-abis/stringsabi",
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
                basePath = "test-abis/stringsabi",
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
                basePath = "test-abis/stringsabi",
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
                basePath = "test-abis/duplicate",
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
                basePath = "test-abis/stringsabi",
                names = listOf("ABIChanged", "AddrChanged"),
                typeFilter = "event"
            )

        // Should return only distinct events
        assert(events.size == 2) { "Expected 2 distinct events, but got ${events.size}" }
        assert(events.any { it.name == "ABIChanged" }) { "ABIChanged event not found" }
        assert(events.any { it.name == "AddrChanged" }) { "AddrChanged event not found" }
    }

    @Test
    fun `load performs substitution with envParams`() {
        val events =
            AbiLoader.load(
                basePath = "test-abis/substitution",
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
                basePath = "test-abis/stringsabi",
                names = listOf("approve", "clearRecords"),
                typeFilter = "function"
            )

        // Should return all functions defined in the files
        assert(functions.size == 2) { "Expected 2 functions, but got ${functions.size}" }
        assert(functions.any { it.name == "approve" }) { "approve function not found" }
        assert(functions.any { it.name == "clearRecords" }) { "clearRecords function not found" }
    }

    @Test
    fun `should load events with the same name but different types`() {
        val events =
            AbiLoader.load(
                basePath = "test-abis/tokens",
                names = listOf("Transfer"),
                typeFilter = "event"
            )

        // Should return only the specified events
        assert(events.size == 2) { "Expected 2 events, but got ${events.size}" }
        assert(events[0].name == "Transfer") { "First Transfer event failed to load" }
        assert(events[1].name == "Transfer") { "Second Transfer event failed to load" }
    }

    @Test
    fun `loadEvents should return events`() {
        val events =
            AbiLoader.loadEvents(
                basePath = "test-abis/stringsabi",
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
                basePath = "test-abis/stringsabi",
                functionNames = listOf("approve", "clearRecords")
            )

        // Should return only the specified functions
        assert(functions.size == 2) { "Expected 2 functions, but got ${functions.size}" }
        assert(functions.any { it.name == "approve" }) { "approve function not found" }
        assert(functions.any { it.name == "clearRecords" }) { "clearRecords function not found" }
    }

    @Nested
    inner class GenerateUniqueId {
        @Test
        fun `includes indexed and maintains input order`() {
            val abi =
                AbiElement(
                    name = "Transfer",
                    type = "event",
                    inputs =
                        listOf(
                            InputOutput(indexed = true, type = "address", name = "from"),
                            InputOutput(indexed = false, type = "address", name = "to"),
                            InputOutput(indexed = true, type = "uint256", name = "value")
                        )
                )

            val uniqueId = AbiLoader.generateUniqueId(abi)

            assert(uniqueId == "Transfer(indexed address,address,indexed uint256)") {
                "Unexpected uniqueId: $uniqueId"
            }
        }

        @Test
        fun `throws if abi name is null`() {
            val abi =
                AbiElement(
                    name = null,
                    type = "event",
                    inputs = listOf(InputOutput(indexed = true, type = "address", name = "from"))
                )

            try {
                AbiLoader.generateUniqueId(abi)
                assert(false) { "Expected exception for null name" }
            } catch (e: IllegalArgumentException) {
                assert(e.message?.contains("must have a name") == true)
            }
        }

        @Test
        fun `works with empty inputs`() {
            val abi = AbiElement(name = "NoInputsEvent", type = "event", inputs = emptyList())

            val uniqueId = AbiLoader.generateUniqueId(abi)

            assert(uniqueId == "NoInputsEvent()") { "Unexpected uniqueId: $uniqueId" }
        }
    }
}
