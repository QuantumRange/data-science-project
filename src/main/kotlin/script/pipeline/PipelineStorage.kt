package dev.qr.scripts.pipeline

import java.io.File
import kotlin.collections.component1
import kotlin.collections.component2

data class FileHolder(
    val name: String,
    val files: Map<String, File>
) {
    operator fun get(key: String): File = files[key] ?: error("No such file: $key")
}

interface PipelineStorage {

    /**
     * The directory where all the files lay
     */
    val directory: File

    /**
     * The source folder is read-only, because otherwise there is the risk of data loss!
     */
    val isWrite: Boolean

    suspend fun getObjects(): List<FileHolder> {
        return directory.listFiles()!!
            .groupBy { it.name.substringBefore(".") }
            .mapNotNull { (name, files) ->
                val holder = allocate(name)

                if (holder.files.values.any { !it.exists() }) return@mapNotNull null
                require(files.all { file ->
                    holder.files.containsValue(file)
                }) { "There are files not reserved in the FileHolder but present!" }

                holder
            }
    }

    suspend fun allocate(name: String): FileHolder

}