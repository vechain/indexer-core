package org.vechain.indexer.event.utils

import java.io.InputStream

object FileUtils {
    fun getResourceAsStream(path: String): InputStream? {
        return Thread.currentThread().contextClassLoader.getResourceAsStream(path)
    }
}
