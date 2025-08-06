package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import org.junit.jupiter.api.*
import strikt.api.expectThat
import strikt.assertions.isEqualTo

data class FooBar(val foo: String, val baz: Int)

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JsonLoaderTest {

    private val testDir = "test-resources"
    private val envParams = mapOf("BAR" to "barValue")

    @Test
    fun `load returns a valid object`() {
        // Assuming test.json exists in test-resources with content: {"foo": "bar", "baz": 42}
        val result =
            JsonLoader.load("$testDir/test.json", object : TypeReference<FooBar>() {}, envParams)
        expectThat(result).isEqualTo(FooBar("barValue", 42))
    }

    @Test
    fun `load throws exception for invalid JSON`() {
        // Assuming invalid.json exists in test-resources with invalid JSON content
        Assertions.assertThrows(Exception::class.java) {
            JsonLoader.load("$testDir/invalid.json", object : TypeReference<FooBar>() {}, envParams)
        }
    }

    @Test
    fun `load returns empty list for empty JSON array`() {
        // Assuming empty.json exists in test-resources with content: []
        val result =
            JsonLoader.load(
                "$testDir/empty.json",
                object : TypeReference<List<FooBar>>() {},
                envParams
            )
        expectThat(result).isEqualTo(emptyList<FooBar>())
    }

    @Test
    fun `load throws exception for non-existent file`() {
        // Ensure an exception is thrown when the file does not exist
        Assertions.assertThrows(IllegalStateException::class.java) {
            JsonLoader.load(
                "$testDir/nonexistent.json",
                object : TypeReference<FooBar>() {},
                envParams
            )
        }
    }

    @Test
    fun `load multiple files returns combined results`() {
        // Assuming test1.json and test2.json exist in test-resources
        val result =
            JsonLoader.load(
                listOf("$testDir/test.json", "$testDir/test2.json"),
                object : TypeReference<FooBar>() {},
                envParams
            )
        expectThat(result).isEqualTo(listOf(FooBar("barValue", 42), FooBar("foo", 43)))
    }

    @Test
    fun `loadAndFlatten returns flattened list of objects`() {
        // Assuming test1.json and test2.json exist in test-resources
        val result =
            JsonLoader.loadAndFlatten(
                listOf("$testDir/list1.json", "$testDir/list2.json"),
                object : TypeReference<List<FooBar>>() {},
                envParams
            )
        expectThat(result).isEqualTo(listOf(FooBar("barValue", 42), FooBar("foo", 43)))
    }

    @Test
    fun `substitutePlaceholders replaces placeholders with envParams`() {
        val input = """{"foo": "${'$'}{BAR}", "baz": 42}"""
        val result = JsonLoader.substitutePlaceholders(input, envParams)
        expectThat(result).isEqualTo("""{"foo": "barValue", "baz": 42}""")
    }

    @Test
    fun `readAndSubstituteJson returns substituted JSON`() {
        // test.json should exist in test-resources with content: {"foo": "${BAR}", "baz": 42}
        val result = JsonLoader.readAndSubstituteJson("$testDir/test.json", envParams)
        expectThat(result.trim()).isEqualTo("""{"foo": "barValue", "baz": 42}""")
    }

    @Test
    fun `readAndSubstituteJson throws is file not found`() {
        // Ensure an exception is thrown when the file does not exist

        Assertions.assertThrows(IllegalStateException::class.java) {
            JsonLoader.readAndSubstituteJson("$testDir/nonexistent.json", envParams)
        }
    }
}
