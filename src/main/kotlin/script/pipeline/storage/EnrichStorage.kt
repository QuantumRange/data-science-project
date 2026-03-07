package dev.qr.script.pipeline.storage

import dev.qr.scripts.pipeline.FileHolder
import dev.qr.scripts.pipeline.PipelineStorage
import java.io.File

object EnrichStorage : PipelineStorage {

    override val directory: File = File("/mnt/data-dump/for_enriching/")
    override val isWrite: Boolean = false

    override suspend fun allocate(name: String): FileHolder = FileHolder(
        name,
        mapOf(
            "data" to File(directory, "$name.parquet")
        )
    )

}