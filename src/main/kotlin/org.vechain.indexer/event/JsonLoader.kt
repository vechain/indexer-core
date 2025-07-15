package org.vechain.indexer.event

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.slf4j.Logger
import org.vechain.indexer.event.utils.FileUtils

object JsonLoader {
    val logger: Logger = org.slf4j.LoggerFactory.getLogger(this::class.java)
    val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    fun <E> loadAndFlatten(
        paths: List<String>,
        typeRef: TypeReference<List<E>>,
        substitutionParams: Map<String, String> = emptyMap()
    ): List<E> {
        return paths.flatMap { load(it, typeRef, substitutionParams) }
    }

    /**
     * Loads a list of JSON files, substituting any placeholders with values from the provided map.
     * If a placeholder is not found in the map, it will check environment variables.
     *
     * @param paths List of JSON file paths to load.
     * @param substitutionParams Map of placeholder names to their replacement values.
     * @return List of parsed objects of type T.
     */
    fun <T> load(
        paths: List<String>,
        typeRef: TypeReference<T>,
        substitutionParams: Map<String, String> = emptyMap()
    ): List<T> {
        val results = mutableListOf<T>()
        for (path in paths) {
            results.add(load(path, typeRef, substitutionParams))
        }
        return results
    }

    /**
     * Loads a single JSON file, substituting any placeholders with values from the provided map. If
     * a placeholder is not found in the map, it will check environment variables.
     *
     * @param path Path to the JSON file to load.
     * @param substitutionParams Map of placeholder names to their replacement values.
     * @return Parsed object of type T.
     */
    fun <T> load(
        path: String,
        typeRef: TypeReference<T>,
        substitutionParams: Map<String, String> = emptyMap()
    ): T {
        val substitutedJson = readAndSubstituteJson(path, substitutionParams)
        return try {
            objectMapper.readValue(substitutedJson, typeRef)
        } catch (ex: Exception) {
            logger.error("❌ Error parsing JSON file $path: ${ex.message}")
            throw ex
        }
    }

    fun readAndSubstituteJson(path: String, substitutionParams: Map<String, String>): String {
        val inputStream =
            FileUtils.getResourceAsStream(path)
                ?: throw IllegalStateException("Resource not found: $path")
        val originalJson = inputStream.bufferedReader().use { it.readText() }
        val substitutedJson = substitutePlaceholders(originalJson, substitutionParams)
        if (substitutedJson.contains("\${")) {
            logger.warn("⚠️Unresolved placeholders found in $path")
        }
        return substitutedJson
    }

    fun substitutePlaceholders(text: String, substitutionParams: Map<String, String>): String {
        val regex = Pattern.compile("""\$\{(\w+)}""")
        val matcher = regex.matcher(text)
        val sb = StringBuffer()
        while (matcher.find()) {
            val key = matcher.group(1)
            val replacement = substitutionParams[key] ?: System.getenv(key)
            if (replacement != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement))
            } else {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group(0)))
            }
        }
        matcher.appendTail(sb)
        return sb.toString()
    }
}
