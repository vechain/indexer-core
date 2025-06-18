package org.vechain.indexer.event.utils

import java.io.InputStream
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths
import java.util.jar.JarFile

object FileUtils {
    fun getResourceAsStream(path: String): InputStream? {
        return Thread.currentThread().contextClassLoader.getResourceAsStream(path)
    }

    fun getResourceURLs(path: String): List<URL> {
        return Thread.currentThread().contextClassLoader.getResources(path).toList()
    }

    fun listJsonFilesInClasspathDir(dir: String): List<String> {
        val resources = Thread.currentThread().contextClassLoader.getResources(dir)
        val results = mutableListOf<String>()

        while (resources.hasMoreElements()) {
            val url = resources.nextElement()
            if (url.protocol == "file") {
                val folder = Paths.get(url.toURI())
                Files.walk(folder, 1)
                    .filter { Files.isRegularFile(it) && it.toString().endsWith(".json") }
                    .forEach { results.add("$dir/${it.fileName}") }
            } else if (url.protocol == "jar") {
                val path = url.path
                val jarPath = path.substringAfter("file:").substringBefore("!/")
                val prefix = path.substringAfter("!/", "")
                JarFile(jarPath).use { jar ->
                    jar.entries()
                        .asSequence()
                        .filter {
                            !it.isDirectory &&
                                it.name.startsWith(prefix) &&
                                it.name.endsWith(".json")
                        }
                        .forEach { results.add(it.name) }
                }
            }
        }

        return results
    }
}
