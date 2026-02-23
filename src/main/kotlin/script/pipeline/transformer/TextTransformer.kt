package dev.qr.scripts.pipeline.transformer

import dev.qr.model.BodyKind
import dev.qr.parquet.ParquetSchema
import dev.qr.scripts.pipeline.FileHolder
import dev.qr.services.LinkService
import io.ktor.http.*
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.json.Json
import org.apache.avro.util.Utf8
import org.jsoup.Jsoup
import org.slf4j.LoggerFactory
import parquet.ParquetService
import script.pipeline.MapPipeline
import util.textMeter

object TextTransformer : MapPipeline(
    LoggerFactory.getLogger(TextTransformer::class.java),
    32
) {

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ): Unit = coroutineScope {
        ParquetService.write(
            target["data"],
            ParquetSchema.CRAWL_V4,
            ParquetService.read(source["data"], ParquetSchema.CRAWL_V3)
        ) {
            val urlStr = (it.get("url")!! as Utf8).toString()
            val url = Url(urlStr)

            val typeIndex = it.get("type")!! as Int
            val type = BodyKind.entries[typeIndex]

            put("id", it.get("id")!!)
            put("domain", it.get("domain")!!)
            put("url", urlStr)

            put("timestamp", it.get("timestamp")!!)
            put("duration", it.get("duration")!!)
            put("http_version", it.get("http_version")!!)
            put("status", it.get("status")!!)
            put("header", it.get("header")!!)
            put("type", typeIndex)

            val content = (it.get("content")!! as Utf8).toString()

            var text: String
            var links: List<String>

            if (type == BodyKind.HTML) {
                val doc = Jsoup.parse(content, urlStr)

                text = doc.text()
                links = LinkService.extractHtmlLinks(url, content)
            } else {
                text = ""
                links = LinkService.extractLinks(url, type, content)
            }

            put("text", text)
            put("links", Json.encodeToString(links))
            textMeter.mark()
        }

        source["meta"].copyTo(target["meta"], overwrite = true)
        source["id"].copyTo(target["id"], overwrite = true)
    }

}
