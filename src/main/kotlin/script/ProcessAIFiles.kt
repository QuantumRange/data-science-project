@file:OptIn(ExperimentalAtomicApi::class)

package dev.qr.script

import dev.qr.parquet.ParquetSchema
import dev.qr.extension.runMain
import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory
import parquet.ParquetService
import service.StemService
import java.io.File
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndIncrement

private object ProcessAIFiles

private val log = LoggerFactory.getLogger(ProcessAIFiles::class.java)

fun main(): Unit = runMain {
    supervisorScope {
//        val files = listOf(
//            File("/mnt/Fast2T/data/ai/ai_responses.parquet")
//        ) + File("/mnt/Fast2T/data/ai/lmsys-chat-1m-synth/output/")
//            .listFiles()
//            .filter { it.extension == "parquet" }
    val files = File("/mnt/Fast2T/data/ai-training-set/human/wiki_parquet_output/")
        .listFiles()
        .filter { it.extension == "parquet" }

        val jobs = mutableListOf<Job>()
        val i = AtomicInt(0)
        val semaphore = Semaphore(64)

        for (file in files) {
            jobs.add(launch {
//                val outputFile = File("/mnt/Fast2T/data/ai-training-set/raw-ai", "${i.fetchAndIncrement()}.parquet")
                val outputFile = File("/mnt/Fast2T/data/ai-training-set/raw-human", "${i.fetchAndIncrement()}.parquet")

                semaphore.withPermit {
                    val flow = ParquetService.read(file, null)

                    ParquetService.write(
                        outputFile,
                        ParquetSchema.AI_DATA,
                        flow
                    ) { record ->
                        put("id", record.get("id")!!)
                        put("classification", record.get("classification")!!)

                        val text = (record.get("text") as Utf8).toString()

                        val language = StemService.detectLanguage(text)

                        put("language", language?.language ?: "UNKNOWN")
                        put("text", StemService.processText(text, language?.language))
                    }
                    println("Processed $file")
                }
            })
        }

        jobs.joinAll()
    }
}