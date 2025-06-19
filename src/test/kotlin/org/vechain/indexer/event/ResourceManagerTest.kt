package org.vechain.indexer.event

import java.io.File
import org.junit.jupiter.api.*
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class TestResourceManager(envParams: Map<String, String> = emptyMap()) :
    ResourceManager(envParams) {
    // Expose protected for testing
    fun publicSubstitutePlaceholders(text: String, params: Map<String, String>) =
        substitutePlaceholders(text, params)

    fun publicReadAndSubstituteJson(path: String) = readAndSubstituteJson(path)
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResourceManagerTest {

    private val testDir = "test-resources"
    private val envParams = mapOf("BAR" to "barValue")
    private lateinit var manager: TestResourceManager

    @BeforeAll
    fun setup() {
        // Ensure test resource directory and file exist
        val dir =
            File(
                javaClass.classLoader.getResource(testDir)?.toURI()
                    ?: error("Test resource dir not found")
            )
        if (!dir.exists()) error("Test resource dir not found")
        manager = TestResourceManager(envParams)
    }

    @Test
    fun `substitutePlaceholders replaces placeholders with envParams`() {
        val input = """{"foo": "${'$'}{BAR}", "baz": "qux"}"""
        val result = manager.publicSubstitutePlaceholders(input, envParams)
        expectThat(result).isEqualTo("""{"foo": "barValue", "baz": "qux"}""")
    }

    @Test
    fun `readAndSubstituteJson returns substituted JSON`() {
        // test.json should exist in test-resources with content: {"foo": "${BAR}", "baz": "qux"}
        val result = manager.publicReadAndSubstituteJson("$testDir/test.json")
        expectThat(result?.trim()).isEqualTo("""{"foo": "barValue", "baz": "qux"}""")
    }

    @Test
    fun `readAndSubstituteJson throws is file not found`() {
        // Ensure an exception is thrown when the file does not exist

        Assertions.assertThrows(IllegalStateException::class.java) {
            manager.publicReadAndSubstituteJson("$testDir/nonexistent.json")
        }
    }
}
