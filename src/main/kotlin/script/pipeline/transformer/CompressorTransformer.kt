package script.pipeline.transformer

import dev.qr.parquet.ParquetSchema
import parquet.ParquetService
import dev.qr.scripts.pipeline.FileHolder
import io.ktor.util.moveToByteArray
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory
import script.pipeline.MapPipeline
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.HashMap
import java.util.zip.GZIPInputStream

object CompressorTransformer : MapPipeline(
    LoggerFactory.getLogger(CompressorTransformer::class.java),
    32
) {

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ) {
        ParquetService.write(
            target["data"],
            ParquetSchema.CRAWL_V2,
            ParquetService.read(source["data"], ParquetSchema.CRAWL_V1)
        ) {
            put("url", it.get("url"))
            put("timestamp", it.get("timestamp"))
            put("duration", it.get("duration"))
            put("version", it.get("version"))
            put("status", it.get("status"))
            put("header", it.get("header"))
            put("content", it.get("content"))
            put("content_type", it.get("content_type"))

            val rawContent = it.get("content") as? ByteBuffer? ?: return@write

            val raw = GZIPInputStream(ByteArrayInputStream(rawContent.moveToByteArray())).use { gzip ->
                gzip.readAllBytes()
            }.decodeToString()

            put("content", raw)

            @Suppress("UNCHECKED_CAST")
            val map = it.get("header") as? HashMap<Utf8, List<Utf8>>? ?: emptyMap()

            put("header", JsonObject(map.map { (key, value) ->
                key.toString() to JsonArray(value.map { v -> JsonPrimitive(v.toString()) })
            }.toMap()))
        }

        source["meta"].copyTo(target["meta"], overwrite = true)
    }

}