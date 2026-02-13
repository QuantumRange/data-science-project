package dev.qr.scripts

import com.codahale.metrics.Meter
import com.codahale.metrics.Metric
import dev.qr.model.BodyKind
import dev.qr.model.ProtocolVersion
import dev.qr.parquet.ParquetSchema
import dev.qr.parquet.ParquetService
import dev.qr.util.RocksUtil
import dev.qr.util.UrlUtil
import dev.qr.util.globalContext
import dev.qr.util.runMain
import io.ktor.http.HttpProtocolVersion
import io.ktor.http.Url
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOn
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
import okhttp3.internal.format
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.hadoop.fs.Options.HandleOpt.path
import util.LinkUtil
import java.io.File
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndIncrement
import kotlin.io.path.Path
import kotlin.system.exitProcess

private val folderLock = Mutex()
private val parallel = Semaphore(1)
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

private val crawlMetric = Meter()
private val fileMetric = Meter()

@OptIn(ExperimentalAtomicApi::class)
fun main(): Unit = runMain {
    println("Preparing db...")
    RocksUtil.open(Path("db/"))
    println("Loaded!")

    val sourceDir = File("/mnt/Fast2T/data/crawls_v2/")
    val targetDir = File("/mnt/Fast2T/data/crawls_v3/")

    var files = AtomicInt((targetDir.listFiles()!!.size - 1) / 2)
    val totalFiles = sourceDir.listFiles()!!.size / 2

    sourceDir.listFiles()!!
        .filter { it.extension == "parquet" }
        .sortedBy { it.name }
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
                        .filter { it.get("version") != null }
                        .onEach { ids.add(it.get("url") as Long) }

                    ParquetService.write(
                        targetFile,
                        ParquetSchema.CRAWL_V3,
                        flow,
                        block = GenericRecord::processRecords
                    )
                    ParquetService.write(idFile, ParquetSchema.CRAWL_ID, ids.asFlow()) { id -> put("id", id) }
                    files.fetchAndIncrement()
                }
                fileMetric.mark()
                crawlMetric.mark(ids.size.toLong())

                val perSecond = fileMetric.fiveMinuteRate
                val left = totalFiles - files.load()
                val time = left / perSecond

                println(
                    "Processed ${sourceFile.name} :: ${
                        format("%.2f", crawlMetric.fiveMinuteRate)
                    }url/s ${
                        format("%.2f", fileMetric.fiveMinuteRate * 60.0)
                    }file/m (${
                        format("%.2f", time * 60.0)
                    }m left)"
                )

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

private val domainLookup by lazy {
    HashMap<String, Long>(
        File("data/domain.csv")
            .readLines()
            .drop(1)
            .associate {
                val id = it.substringBefore(",").toLong()
                val domain = it.substringAfter(",")

                domain to id
            }
    )
}

private fun getIdForDomain(domain: String): Long? = domainLookup[domain]

@Serializable
private data class Links(
    val domains: Map<Long, Int>,
    val links: List<Long>
)

//GenericRecord.(data: T) -> Unit

private suspend fun GenericRecord.processRecords(record: GenericRecord) {
    val id = record.get("url") as Long
    val urlStr = RocksUtil.getUrl(id)!!
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

    val content = (record.get("content") as? Utf8?)?.toString()
//    val urls = content?.let {
//        LinkUtil.extractLinks(
//            Url(urlStr),
//            BodyKind.entries[typeIndex],
//            content,
//        ).mapNotNull { UrlUtil.processUrl(it) }
//    } ?: emptyList()

//    val domains = mutableMapOf<Long, Int>()
//    val links = mutableListOf<Long>()

//    RocksUtil.getIds(urls.map { it.toString() }).forEach { links.add(it) }

//    urls
//        .groupBy { it.host }
//        .forEach { (_, urls) ->
//            val domainId = getIdForDomain(urls.first().host)
//            require(domainId != null) { "Unknown host ${urls.first().host}" }
//            domains[domainId] = urls.size
//        }

    // set new fields
    put("id", id)
    put("domain", getIdForDomain(Url(urlStr).host)!!)
    put("url", urlStr)

    put("timestamp", record.get("timestamp"))
    put("duration", duration)
    put("http_version", httpIndex)
    put("status", record.get("status"))
    put("header", record.get("header"))

    put("type", typeIndex)
    put("content", content)
//    put("links", Json.encodeToString(Links(domains, links)))
}

