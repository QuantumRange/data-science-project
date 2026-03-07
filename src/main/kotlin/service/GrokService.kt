package dev.qr.service

import com.google.common.hash.Hashing
import dev.qr.extension.runMain
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import util.progressBar
import java.io.File
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.io.encoding.Base64

fun main() = runMain {
    GrokService.run()
}

object GrokService {

    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun run(): Unit = coroutineScope {
        val client = HttpClient(CIO)
        val targetFolder = File("/home/qr/data/crawl/")
        val urls: List<Url> = GrokService::class.java.getResource("/grokipedia_urls.jsonl")!!.readText()
            .lines()
            .filter { it.isNotBlank() }
            .map { Json.parseToJsonElement(it).jsonObject["url"]!!.jsonPrimitive.content }
            .mapNotNull { runCatching { Url(it) }.getOrNull() }
            .distinct()

        val progressBar = progressBar(log, urls.size)
        val semaphore = Semaphore(32)

        urls
            .map { url ->
                async {
                    val key = url.toString().hash().replace("\\W".toRegex(), "")
                    val file = File(targetFolder, "$key.html")

                    if (file.exists()) {
                        progressBar.silentInc(1)
                        return@async
                    }

                    val response = semaphore.withPermit { runCatching { client.get(url) }.getOrNull() }

                    if (response == null || !response.status.isSuccess()) {
                        log.warn("Failed {}:\n{}", url, response)
                        file.createNewFile()
                        progressBar.inc(1)
                        return@async
                    }

                    val content = response.readRawBytes().decodeToString()

                    GZIPOutputStream(FileOutputStream(file)).use { gzip ->
                        gzip.write(content.toByteArray())
                    }

                    progressBar.inc(1)
                }
            }
            .awaitAll()

        progressBar.job?.cancelAndJoin()
    }

    fun String.hash(): String = Base64.encode(Hashing.goodFastHash(128).hashString(this, Charsets.UTF_8).asBytes())

}