package org.vechain.indexer.utils

import java.io.File
import java.net.JarURLConnection
import java.util.jar.JarFile

object FileScanner {

    /**
     * Scans for `.json` resources in the classpath under [basePath], up to the given [maxDepth].
     * Works inside JARs and on the file system.
     *
     * @param basePath classpath-relative base directory (e.g., "abis")
     * @param suffix optional file suffix to filter by. (e.g. "json" to find only JSON files)
     * @param maxDepth how many subdirectory levels to recurse (0 = just basePath)
     */
    fun findFiles(basePath: String, suffix: String? = null, maxDepth: Int = 2): List<String> {
        val classLoader = Thread.currentThread().contextClassLoader
        val resources = classLoader.getResources(basePath)
        val results = mutableListOf<String>()

        while (resources.hasMoreElements()) {
            val url = resources.nextElement()

            when (url.protocol) {
                "file" -> {
                    val baseDir = File(url.toURI())
                    if (baseDir.isDirectory) {
                        baseDir
                            .walkTopDown()
                            .maxDepth(maxDepth + 1)
                            .filter {
                                it.isFile &&
                                    (suffix == null ||
                                        it.name.endsWith(".$suffix", ignoreCase = true))
                            }
                            .map { file ->
                                // Convert absolute file path to classpath-relative path
                                baseDir
                                    .toPath()
                                    .relativize(file.toPath())
                                    .toString()
                                    .replace(File.separatorChar, '/')
                                    .let { "$basePath/$it" }
                            }
                            .toCollection(results)
                    }
                }
                "jar" -> {
                    val jarConnection = url.openConnection() as JarURLConnection
                    val jarFile: JarFile = jarConnection.jarFile
                    val entryPrefix = jarConnection.entryName.removeSuffix("/") + "/"

                    jarFile
                        .entries()
                        .asSequence()
                        .filter { it.name.startsWith(entryPrefix) && it.name.endsWith(".json") }
                        .filter { relativeDepth(entryPrefix, it.name) <= maxDepth }
                        .map { it.name }
                        .toCollection(results)
                }
            }
        }

        return results.sorted()
    }

    private fun relativeDepth(base: String, full: String): Int {
        val baseParts = base.trim('/').split('/')
        val fullParts = full.trim('/').split('/')
        return (fullParts.size - baseParts.size).coerceAtLeast(0)
    }
}
