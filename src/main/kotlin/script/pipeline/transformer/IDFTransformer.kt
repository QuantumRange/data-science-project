package dev.qr.script.pipeline.transformer

import dev.qr.parquet.ParquetSchema
import dev.qr.scripts.pipeline.FileHolder
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import parquet.ParquetService
import script.pipeline.MapPipeline

object IDFTransformer : MapPipeline(
    LoggerFactory.getLogger(IDFTransformer::class.java),
    32
) {

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ): Unit = coroutineScope {
//        ParquetService.write(
//            target["data"],
//            ParquetSchema.CRAWL_V6,
//            ParquetService.read(source["data"], ParquetSchema.CRAWL_V5)
//        ) { rec ->
//            // TODO
//        }

        source["meta"].copyTo(target["meta"], overwrite = true)
    }

}