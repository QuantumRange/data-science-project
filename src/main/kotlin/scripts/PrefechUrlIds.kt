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
import java.util.HashMap
import java.util.zip.GZIPInputStream

fun main(): Unit = runMain {
    val sourceDir = File("/mnt/Fast2T/data/crawls_v2/")
    val targetFile = File("/mnt/Fast2T/data/ids.csv")

    println(File("data/urls.csv").readLines().size)

//    sourceDir.listFiles()!!
//        .filter { it.extension == "parquet" }
//        .map { file ->
//            async {
//                val compressFile = File(compressDir, file.name)
//                val metaFile = File(metaDir, file.name)
//
//                if (!compressFile.exists()) {
//                    migrate(file, compressFile)
//                }
//            }
//        }
//        .awaitAll()
}
