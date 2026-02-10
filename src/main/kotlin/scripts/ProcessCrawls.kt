package dev.qr.scripts

import dev.qr.model.BodyKind
import dev.qr.model.ProtocolVersion
import dev.qr.parquet.ParquetSchema
import dev.qr.parquet.ParquetService
import dev.qr.util.UrlUtil
import dev.qr.util.runMain
import io.ktor.http.HttpProtocolVersion
import io.ktor.http.Url
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import util.LinkUtil
import java.io.File

private val folderLock = Mutex()
private val parallel = Semaphore(2)
private val folderMetadataFile = File("/mnt/Fast2T/data/crawls_v3/meta.json")

@Serializable
data class FolderMetadata(
    val files: Map<String, Meta>
) {
    @Serializable
    data class Meta(
        val idMin: Long,
        val idMax: Long
    )
}

fun main(): Unit = runMain {
    val sourceDir = File("/mnt/Fast2T/data/crawls_v2/")
    val targetDir = File("/mnt/Fast2T/data/crawls_v3/")

    sourceDir.listFiles()!!
        .filter { it.extension == "parquet" }
        .map { sourceFile ->
            async {
                val metaFile = File(sourceDir, "${sourceFile.name}.meta.json")

                val targetFile = File(targetDir, sourceFile.name)
                val idFile = File(targetDir, "${sourceFile.name}.id.parquet")

                if (targetFile.exists()) return@async

                val ids = mutableSetOf<Long>()
                val metaData = Json.parseToJsonElement(metaFile.readText()).jsonObject
                val min = metaData["url"]!!.jsonObject["min"]!!.jsonPrimitive.long
                val max = metaData["url"]!!.jsonObject["max"]!!.jsonPrimitive.long

                parallel.withPermit {
                    val flow = ParquetService.read(sourceFile, ParquetSchema.CRAWL_V2)
                        .processRecords()
                        .onEach { ids.add(it.get("url") as Long) }

                    ParquetService.write(targetFile, ParquetSchema.CRAWL_V3, flow)
                    ParquetService.write(idFile, ParquetSchema.CRAWL_ID, ids.asFlow()) { id -> put("id", id) }
                    println("Processed ${sourceFile.name}...")
                }

                require(min == ids.minOrNull()) { "Min id mismatch $min vs ${ids.minOrNull()}" }
                require(max == ids.maxOrNull()) { "Max id mismatch $min vs ${ids.maxOrNull()}" }

                folderLock.withLock {
                    var data = if (!folderMetadataFile.exists()) {
                        FolderMetadata(mapOf())
                    } else Json.decodeFromString(folderMetadataFile.readText())

                    data = data.copy(files = data.files + (sourceFile.name to FolderMetadata.Meta(min, max)))

                    folderMetadataFile.writeText(Json.encodeToString(data))
                }
            }
        }
        .awaitAll()
}

private val domainLookup = HashMap<String, Long>(
    File("data/domain.csv")
        .readLines()
        .drop(1)
        .associate {
            val id = it.substringBefore(",").toLong()
            val domain = it.substringAfter(",")

            domain to id
        }
)

private val urlLoopup = HashMap<String, Long>(
    File("data/urls.csv")
        .readLines()
        .drop(1)
        .associate {
            val id = it.substringBefore(",").toLong()
            val domain = it.substringAfter(",")

            domain to id
        }
)

private suspend fun getIdForDomain(domain: String): Long = domainLookup[domain]!!
private suspend fun getUrlForId(id: Long): String = TODO()
private suspend fun getIdForUrl(url: String): Long? = TODO()

private data class Links(
    val domains: Map<Long, Int>,
    val links: List<Long>
)

private suspend fun Flow<GenericRecord>.processRecords(): Flow<GenericRecord> = this.map { record ->
    val id = record.get("url") as Long
    val urlStr = getUrlForId(id)
    val duration = (record.get("duration") as? Long?) ?: 0
    val typeIndex = (record.get("content_type") as? Int?) ?: BodyKind.UNKNOWN.ordinal

    val httpVersionStr = (record.get("version") as? Utf8?)?.toString()
    val httpVersion = httpVersionStr?.let { HttpProtocolVersion.parse(it) }
    val httpIndex = httpVersion?.let { version ->
        ProtocolVersion.entries
            .firstOrNull { it.representation == version }
            ?.ordinal
            ?: error("Unknown version $version")
    } ?: ProtocolVersion.UNKNOWN.ordinal

    val urls = LinkUtil.extractLinks(
        Url(urlStr),
        BodyKind.entries[typeIndex],
        (record.get("content") as Utf8).toString(),
    ).mapNotNull { UrlUtil.processUrl(it) }

    val domains = mutableMapOf<Long, Int>()
    val links = mutableListOf<Long>()

    urls.forEach { url ->
        getIdForUrl(url.toString())
            ?.let { links.add(it) }
            ?: run {
                val domainId = getIdForDomain(url.host)

                domains.compute(domainId) { _, count -> count?.plus(1) ?: 1 }
            }
    }

    // set new fields
    record.put("id", id)
    record.put("url", urlStr)
    record.put("duration", duration)
    record.put("http_version", httpIndex)
    record.put("type", typeIndex)
    record.put("links", Json.encodeToString(Links(domains, links)))

    // erase fields
    record.put("version", null)
    record.put("content_type", null)

    record
}

