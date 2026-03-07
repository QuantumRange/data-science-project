package dev.qr.script

import dev.qr.extension.runMain
import dev.qr.parquet.ParquetSchema
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.apache.avro.util.Utf8
import org.jsoup.Jsoup
import org.slf4j.LoggerFactory
import parquet.ParquetService
import service.StemService
import java.io.File
import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndIncrement

@OptIn(ExperimentalAtomicApi::class, ExperimentalCoroutinesApi::class)
fun main(): Unit = runMain {
    supervisorScope {
        ParquetService.write(
            File("/mnt/Fast2T/data/detection/cc-lang.parquet"),
            ParquetSchema.CD_DATA,
            ParquetService.read(File("/mnt/Fast2T/data/common_data_parquet/CC-MAIN-20190421140100-20190421162100-00026.parquet"), null)
        ) { record ->
            val content = (record.get("content")!! as Utf8).toString()
            val text = Jsoup.parse(content).text()

            val language = StemService.detectLanguage(text)

            put("language", language?.language ?: "UNKNOWN")
            put("text", text)
        }
    }
}