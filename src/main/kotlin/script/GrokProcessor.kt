package dev.qr.script

import dev.qr.extension.runMain
import dev.qr.parquet.ParquetSchema
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.chunked
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

private object GrokProcessor

private val log = LoggerFactory.getLogger(GrokProcessor::class.java)

@OptIn(ExperimentalAtomicApi::class, ExperimentalCoroutinesApi::class)
fun main(): Unit = runMain {
    supervisorScope {
        val channel = Channel<Pair<StemService.LanguageResult?, String>>()

        val semaphore = Semaphore(64)

        val jobs = File("/home/qr/data/crawl")
            .listFiles()
            .filter { it.length() != 0L }
            .shuffled()
            .map { file ->
                launch {
                    semaphore.withPermit {
                        runCatching {
                            val content = GZIPInputStream(FileInputStream(file)).use { reader ->
                                reader.readAllBytes().decodeToString()
                            }

                            if (content.isBlank()) return@launch

                            runCatching {
                                val text = Jsoup.parse(content).text()

                                val language = StemService.detectLanguage(text)

                                val stemmed = StemService.processText(text, language?.language)

                                channel.send(language to stemmed)
                            }
                        }
                    }
                }
            }

        launch {
            jobs.joinAll()
            channel.close()
        }

        channel
            .receiveAsFlow()
            .chunked(100_000)
            .withIndex()
            .collect { (idx, data) ->
                val file = File("/mnt/Fast2T/data/grok/$idx.parquet")

                ParquetService.write(file, ParquetSchema.CD_DATA, data.asFlow()) { (language, text) ->
                    put("language", language?.language ?: "UNKNOWN")
                    put("text", text)
                }

                println("Processed $idx")
            }

    }
}