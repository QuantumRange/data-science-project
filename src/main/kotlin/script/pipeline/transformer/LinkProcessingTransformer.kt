package dev.qr.scripts.pipeline.transformer

import dev.qr.parquet.ParquetSchema
import dev.qr.scripts.pipeline.FileHolder
import io.ktor.http.*
import kotlinx.serialization.json.Json
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory
import parquet.ParquetService
import script.pipeline.MapPipeline
import service.StemService
import java.io.File

class LinkProcessingTransformer : MapPipeline(
    LoggerFactory.getLogger(LinkProcessingTransformer::class.java),
    10
) {

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ) {
        transform(source["data"], target["data"])

        source["meta"].copyTo(target["meta"], overwrite = true)
        source["id"].copyTo(target["id"], overwrite = true)
    }

    private val domainLookup by lazy {
        HashMap<String, Long>(
            File("data/domain.csv")
                .readLines()
                .drop(1)
                .associate {
                    val id = it.substringBefore(",").toLong()
                    val domain = it.substringAfter(",")

                    domain to id
                }
        )
    }

    @Suppress("DuplicatedCode")
    suspend fun transform(
        source: File,
        target: File
    ) {
        ParquetService.write(
            target,
            ParquetSchema.CRAWL_V5,
            ParquetService.read(source, ParquetSchema.CRAWL_V4)
        ) { record: GenericRecord ->
            val text = (record.get("text")!! as Utf8).toString()

            val result = StemService.detectLanguage(text)
            val language = result?.language
            val stemmedText = StemService.processText(text, language)

            put("lang", result?.language ?: "UNKNOWN")
            put("lang_confidence", result?.confidence ?: 0.0)
            put("text", stemmedText)

            val links: List<String> = Json.decodeFromString<List<String>>((record.get("links")!! as Utf8).toString())
            val outflow = links
                .asSequence()
                .map { Url(it) }
                .map { it.host }
                .mapNotNull { domainLookup[it] }
                .groupBy { it }
                .map { (host, urls) -> host to urls.size }
                .toMap()
            put("outflow", Json.encodeToString(outflow))

            put("id", record.get("id")!!)
            put("domain", record.get("domain")!!)
            put("url", record.get("url")!!)
            put("timestamp", record.get("timestamp")!!)
            put("duration", record.get("duration")!!)
            put("http_version", record.get("http_version")!!)
            put("status", record.get("status")!!)
            put("header", record.get("header")!!)
            put("type", record.get("type")!!)
        }
    }

}