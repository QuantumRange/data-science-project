package dev.qr.scripts.pipeline.storage

import dev.qr.scripts.pipeline.FileHolder
import dev.qr.scripts.pipeline.PipelineStorage
import java.io.File

object LinkStorage : PipelineStorage {

    override val directory: File = File("/mnt/Fast2T/data/link/")
    override val isWrite: Boolean = false

    override suspend fun allocate(name: String): FileHolder = FileHolder(
        name,
        mapOf(
            "data" to File(TextStorage.directory, "$name.parquet"),
            "id" to File(TextStorage.directory, "$name.parquet.id.parquet"),
            "meta" to File(TextStorage.directory, "$name.parquet.meta.json"),
        )
    )

}