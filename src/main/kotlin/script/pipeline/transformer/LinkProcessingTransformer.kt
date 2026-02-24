package dev.qr.script.pipeline.transformer

import dev.qr.parquet.ParquetSchema
import dev.qr.scripts.pipeline.FileHolder
import io.ktor.http.*
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory
import parquet.ParquetService
import script.pipeline.MapPipeline
import service.StemService
import java.io.File
import kotlin.coroutines.suspendCoroutine

object LinkProcessingTransformer : MapPipeline(
    LoggerFactory.getLogger(LinkProcessingTransformer::class.java),
    32
) {

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ): Unit = coroutineScope {
        ParquetService.write(
            target["data"],
            ParquetSchema.CRAWL_V5,
            ParquetService.read(source["data"], ParquetSchema.CRAWL_V4)
        ) { rec ->
            val text = (rec.get("text")!! as Utf8).toString()

            val langJob = launch {
                if (text.isBlank()) {
                    put("lang", "UNKNOWN")
                    put("lang_confidence", 0.0)
                    put("text", "")
                } else {
                    val result = StemService.detectLanguage(text)
                    val language = result?.language
                    val stemmedText = StemService.processText(text, language)

                    put("lang", result?.language ?: "UNKNOWN")
                    put("lang_confidence", result?.confidence ?: 0.0)
                    put("text", stemmedText)
                }
            }

            val outflowJob = launch {
                val links: List<String> = Json.decodeFromString<List<String>>((rec.get("links")!! as Utf8).toString())
                val outflow = links
                    .asSequence()
                    .mapNotNull { runCatching { Url(it) }.getOrNull() }
                    .map { it.host }
                    .mapNotNull { domainLookup[it] }
                    .groupBy { it }
                    .map { (host, urls) -> host to urls.size }
                    .toMap()
                put("outflow", Json.encodeToString(outflow))
            }

            put("id", rec.get("id")!!)
            put("domain", rec.get("domain")!!)
            put("url", rec.get("url")!!)
            put("timestamp", rec.get("timestamp")!!)
            put("duration", rec.get("duration")!!)
            put("http_version", rec.get("http_version")!!)
            put("status", rec.get("status")!!)
            put("header", rec.get("header")!!)
            put("type", rec.get("type")!!)

            outflowJob.join()
            langJob.join()
        }

        source["meta"].copyTo(target["meta"], overwrite = true)
    }

    private val domainLookup by lazy {
        HashMap<String, Long>(
            File("/dat/proj/kotlin/2026/data-science-project/data/domain.csv")
                .readLines()
                .drop(1)
                .associate {
                    val id = it.substringBefore(",").toLong()
                    val domain = it.substringAfter(",")

                    domain to id
                }
        )
    }

}