package dev.qr.scripts

import com.fleeksoft.io.ByteArrayInputStream
import dev.qr.parquet.ParquetSchema
import dev.qr.parquet.ParquetService
import dev.qr.util.RocksUtil
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
import kotlin.io.path.Path
import kotlin.system.exitProcess

fun main(): Unit = runMain {
    println("Preparing db...")

    println("Nop, you wanted to change this code first! It shoulnd delete but append new data from the last largest id, etc.")
    exitProcess(1)

    val path = Path("db/")
    path.toFile().deleteRecursively()
    RocksUtil.open(path)

    println("Loading...")
    RocksUtil.loadFromPostgresBinaryCopy(Path("data/urls.copy.zst"), batchRows = 1_000_000)

    println("Loaded!")
}
