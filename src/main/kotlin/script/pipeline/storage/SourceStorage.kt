package script.pipeline.storage

import dev.qr.scripts.pipeline.FileHolder
import dev.qr.scripts.pipeline.PipelineStorage
import java.io.File

object SourceStorage : PipelineStorage {
    
    override val directory: File = File("/mnt/data-dump/crawls/")
    override val isWrite: Boolean = false

    override suspend fun allocate(name: String): FileHolder = FileHolder(
        name,
        mapOf(
            "data" to File(directory, "$name.parquet"),
            "meta" to File(directory, "$name.parquet.meta.json"),
        )
    )

}