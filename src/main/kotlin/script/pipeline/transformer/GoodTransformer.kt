package dev.qr.script.pipeline.transformer

import dev.qr.parquet.ParquetSchema
import dev.qr.scripts.pipeline.FileHolder
import kotlinx.coroutines.coroutineScope
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory
import parquet.ParquetService
import script.pipeline.MapPipeline
import service.StemService

object GoodTransformer : MapPipeline(
    LoggerFactory.getLogger(GoodTransformer::class.java),
    32
) {

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ): Unit = coroutineScope {
        ParquetService.write(
            target["data"],
            ParquetSchema.CRAWL_LLM,
            ParquetService.read(source["data"], ParquetSchema.CRAWL_V4)
        ) {
            put("id", it.get("id")!!)
            put("domain", it.get("domain")!!)
            put("url", it.get("url")!!)

            val text = (it.get("text") as Utf8).toString()
            val language = StemService.detectLanguage(text)

            if (language != null && language.language != "ENGLISH") {
                put("text", text)
            } else {
                put("text", "")
            }
        }
    }

}
