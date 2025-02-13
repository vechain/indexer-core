package org.vechain.indexer.helpers

import java.io.File
import java.io.InputStream

object FileLoaderHelper {
    fun loadJsonFilesFromPath(resourcePath: String): Map<String, InputStream> {
        val classLoader = Thread.currentThread().contextClassLoader
        val resourceDir =
            classLoader.getResource(resourcePath)
                ?: throw IllegalArgumentException("Invalid JSON directory: $resourcePath")

        val jsonFiles =
            File(resourceDir.toURI()).listFiles { file -> file.extension == "json" }

        if (jsonFiles.isNullOrEmpty()) {
            println("No JSON files found in directory: $resourcePath")
            return emptyMap()
        }

        return jsonFiles.associate { file ->
            file.nameWithoutExtension to file.inputStream()
        }
    }
}
