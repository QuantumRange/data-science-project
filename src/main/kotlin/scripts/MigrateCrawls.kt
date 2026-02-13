package dev.qr.scripts

import com.fleeksoft.io.ByteArrayInputStream
import dev.qr.parquet.ParquetSchema
import dev.qr.parquet.ParquetService
import dev.qr.util.runMain
import io.ktor.util.moveToByteArray
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.apache.avro.util.Utf8
import java.io.File
import java.lang.String.format
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

fun main(): Unit = runMain {
    val sourceDir = File("/mnt/Fast2T/data/crawls/")
    val compressDir = File("/mnt/Fast2T/data/crawls_v2/")
    val metaDir = File("/mnt/Fast2T/data/crawls_meta/")

    sourceDir.listFiles()!!
        .filter { it.extension == "parquet" }
        .map { file ->
            async {
                val compressFile = File(compressDir, file.name)
                val metaFile = File(metaDir, file.name)

                if (!compressFile.exists()) {
                    migrate(file, compressFile)
                }
            }
        }
        .awaitAll()
}

val semaphore = Semaphore(2)

private suspend fun migrate(
    source: File,
    target: File
): Unit = semaphore.withPermit {
    ParquetService.write(
        target,
        ParquetSchema.CRAWL_V2,
        ParquetService.read(source, ParquetSchema.CRAWL_V1)
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
        val map = it.get("header") as? java.util.HashMap<Utf8, List<Utf8>>? ?: emptyMap()

        put("header", JsonObject(map.map { (key, value) ->
            key.toString() to JsonArray(value.map { v -> JsonPrimitive(v.toString()) })
        }.toMap()))

    }

    val file = File(source.parentFile, "${source.name}.meta.json")

    if (file.exists()) {
        file.copyTo(File(target.parentFile, "${target.name}.meta.json"), true)
    }

    println(
        "Migrating ${source.name} :: ${
            format(
                "%.02f%%",
                target.length() / source.length().toDouble() * 100.0
            )
        }"
    )
}
