package scripts.pipeline.transformer

import dev.qr.model.BodyKind
import dev.qr.model.ProtocolVersion
import dev.qr.parquet.ParquetSchema
import dev.qr.scripts.pipeline.FileHolder
import dev.qr.services.RocksDBService
import io.ktor.http.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Mutex
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory
import parquet.ParquetService
import scripts.pipeline.MapPipeline
import java.io.File
import kotlin.io.path.Path

object IdInjectorTransformer : MapPipeline(
    LoggerFactory.getLogger(IdInjectorTransformer::class.java),
    6
) {

    init {
        RocksDBService.open(Path("./db"))

        Runtime.getRuntime().addShutdownHook(Thread {
            RocksDBService.close()
        })
    }

    override suspend fun transform(
        source: FileHolder,
        target: FileHolder
    ) {
        source["meta"].copyTo(target["meta"], overwrite = true)

        val ids = mutableSetOf<Long>()

        val flow = ParquetService.read(source["data"], ParquetSchema.CRAWL_V2)
            .filter { it.get("version") != null }
            .filter { it.get("status") != null }
            .filter { it.get("header") != null }
            .filter { it.get("content_type") != null }
            .filter { it.get("timestamp") != null }
            .onEach { ids.add(it.get("url") as Long) }

        ParquetService.write(target["data"], ParquetSchema.CRAWL_V3, flow) { processRecords(it) }
        ParquetService.write(target["id"], ParquetSchema.CRAWL_ID, ids.asFlow()) { id ->
            put("id", id)
            put("url", RocksDBService.getUrl(id)!!)
        }
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

    private val mutex = Mutex()

    private suspend fun GenericRecord.processRecords(record: GenericRecord) {
        val id = record.get("url") as Long
        val urlStr = RocksDBService.getUrl(id)!!
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

        val content = (record.get("content") as? Utf8?)?.toString() ?: ""

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

        put("timestamp", record.get("timestamp")!!)
        put("duration", duration)
        put("http_version", httpIndex)
        put("status", record.get("status")!!)
        put("header", record.get("header")!!)

        put("type", typeIndex)
        put("content", content)
//    put("links", Json.encodeToString(Links(domains, links)))
    }

}