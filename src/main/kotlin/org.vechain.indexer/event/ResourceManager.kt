package org.vechain.indexer.event

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.slf4j.Logger
import org.vechain.indexer.event.utils.FileUtils

abstract class ResourceManager(
    protected val resourceDirectory: String,
    protected val envParams: Map<String, String> = emptyMap()
) {
    protected val logger: Logger = org.slf4j.LoggerFactory.getLogger(this::class.java)
    protected val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    protected fun listJsonFiles(): List<String> =
        FileUtils.listJsonFilesInClasspathDir(resourceDirectory)

    protected fun readAndSubstituteJson(path: String): String? {
        val inputStream = FileUtils.getResourceAsStream(path)
        if (inputStream == null) {
            logger.warn("⚠️Could not find resource for $path")
            return null
        }
        val originalJson = inputStream.bufferedReader().use { it.readText() }
        val substitutedJson = substitutePlaceholders(originalJson, envParams)
        if (substitutedJson.contains("\${")) {
            logger.warn("⚠️Unresolved placeholders found in $path")
        }
        return substitutedJson
    }

    protected fun substitutePlaceholders(text: String, params: Map<String, String>): String {
        val regex = Pattern.compile("""\$\{(\w+)}""")
        val matcher = regex.matcher(text)
        val sb = StringBuffer()
        while (matcher.find()) {
            val key = matcher.group(1)
            val replacement = params[key] ?: System.getenv(key)
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
