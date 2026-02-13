package dev.qr.util

import com.codahale.metrics.Meter
import okhttp3.internal.format
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.BloomFilter
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ColumnFamilyOptions
import org.rocksdb.DBOptions
import org.rocksdb.FlushOptions
import org.rocksdb.LRUCache
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions
import java.io.BufferedInputStream
import java.io.Closeable
import java.io.DataInputStream
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

/**
 * @author AI
 */
object RocksUtil : Closeable {

    private const val ESTIMATED_SIZE = 1_400_000_000L

    private const val CF_URL2ID = "url2id"
    private const val CF_ID2URL = "id2url"

    private val PG_COPY_SIG = byteArrayOf(
        'P'.code.toByte(),
        'G'.code.toByte(),
        'C'.code.toByte(),
        'O'.code.toByte(),
        'P'.code.toByte(),
        'Y'.code.toByte(),
        '\n'.code.toByte(),
        0xFF.toByte(),
        '\r'.code.toByte(),
        '\n'.code.toByte(),
        0x00.toByte()
    )

    private lateinit var db: RocksDB
    private lateinit var cfUrl2Id: ColumnFamilyHandle
    private lateinit var cfId2Url: ColumnFamilyHandle
    private lateinit var defaultCf: ColumnFamilyHandle

    private val digestTL = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

    fun open(dbDir: Path, blockCacheBytes: Long = 2L * 1024 * 1024 * 1024) {
        RocksDB.loadLibrary()

        val tableCfg = BlockBasedTableConfig()
            .setBlockCache(LRUCache(blockCacheBytes))
            .setFilterPolicy(BloomFilter(10.0, false))

        val cfOpts = ColumnFamilyOptions()
            .setTableFormatConfig(tableCfg)

        val dbOpts = DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setMaxBackgroundJobs(32)

        val cfs = listOf(
            ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
            ColumnFamilyDescriptor(CF_URL2ID.toByteArray(), cfOpts),
            ColumnFamilyDescriptor(CF_ID2URL.toByteArray(), cfOpts),
        )

        val handles = ArrayList<ColumnFamilyHandle>(cfs.size)
        db = RocksDB.open(dbOpts, dbDir.toString(), cfs, handles)

        defaultCf = handles[0]
        cfUrl2Id = handles[1]
        cfId2Url = handles[2]
    }

    private val idFile = File("data/urls.meta.id")

    fun loadFromPostgresBinaryCopy(
        inputFile: Path,
        batchRows: Int = 100_000
    ) {
        println("Please wait")
        require(::db.isInitialized) { "Call open(dbDir) first." }

        val rawIn: InputStream = Files.newInputStream(inputFile)
        val inStream: InputStream = com.github.luben.zstd.ZstdInputStream(rawIn)

        var maxId = runCatching { idFile.readText().toLong() }.getOrNull() ?: 0L

        BufferedInputStream(inStream, 16 * 1024 * 1024).use { bis ->
            DataInputStream(bis).use { dis ->
                // ---- Header ----
                val sig = ByteArray(11)
                dis.readFully(sig)
                require(sig.contentEquals(PG_COPY_SIG)) { "Not a PostgreSQL binary COPY stream (bad signature)." }

                val flags = dis.readInt()
                val extLen = dis.readInt()
                if (extLen > 0) dis.skipBytes(extLen)

                // This loader expects: no OIDs, and exactly 2 fields (id bigint, path text).
                // If you included OIDs or changed the SELECT list, adjust parsing accordingly. :contentReference[oaicite:4]{index=4}
                require((flags and (1 shl 16)) == 0) { "COPY stream includes OIDs; not supported by this loader." }

                val writeOpts = WriteOptions()
                    .setDisableWAL(true)
                    .setSync(false)
                // Disabling WAL speeds bulk load but risks losing the last batch on crash. :contentReference[oaicite:5]{index=5}

                WriteBatch(1024 * 1024 * 1024).use { batch ->
                    val idKey = ByteArray(8)
                    val idVal = ByteArray(8)
                    val hashKey = ByteArray(32)

                    val metric = Meter()
                    var rows = 0
                    var total = 0L

                    while (true) {
                        val fieldCount = dis.readShort().toInt()
                        if (fieldCount == -1) break // trailer :contentReference[oaicite:6]{index=6}
                        require(fieldCount == 2) { "Unexpected fieldCount=$fieldCount (expected 2)." }

                        // Field 1: id (bigint -> 8 bytes)
                        val len1 = dis.readInt()
                        require(len1 == 8) { "Unexpected id length=$len1 (expected 8)." }
                        val id = dis.readLong()
                        longToBytesBE(id, idKey)
                        longToBytesBE(id, idVal)

                        // Field 2: path (text -> length bytes)
                        val len2 = dis.readInt()
                        val urlBytes = if (len2 == -1) null else ByteArray(len2).also { dis.readFully(it) }

                        if (urlBytes != null) {
                            // hashKey = sha256(urlBytes) without allocating a new digest array
                            val md = digestTL.get()
                            md.reset()
                            md.update(urlBytes)
                            md.digest(hashKey, 0, hashKey.size)

                            maxId = maxOf(maxId, id)

                            // url2id: sha256(url) -> id(8)
                            batch.put(cfUrl2Id, hashKey, idVal)
                            // id2url: id(8) -> url(bytes)
                            batch.put(cfId2Url, idKey, urlBytes)

                            rows++
                            total++
                        }

                        if (rows >= batchRows) {
                            db.write(writeOpts, batch)
                            batch.clear()

                            val rowsPerMinute = if (metric.fiveMinuteRate == 0.0) 0.001 else metric.fiveMinuteRate
                            val rowsLeft = ESTIMATED_SIZE - total

                            idFile.writeText(maxId.toString())
                            metric.mark(rows.toLong())
                            print(
                                "\rUpserted: $total (${
                                    format(
                                        "%.2f",
                                        metric.fiveMinuteRate
                                    )
                                }/m - est. ${format("%.2f", rowsLeft.toDouble() / rowsPerMinute)}s)"
                            )
                            rows = 0
                        }
                    }

                    if (rows > 0) {
                        db.write(writeOpts, batch)
                        batch.clear()
                    }

                    FlushOptions().setWaitForFlush(true).use { fo ->
                        db.flush(fo, cfUrl2Id)
                        db.flush(fo, cfId2Url)
                    }
                }
            }
        }
    }

    fun getIds(urls: Collection<String>): List<Long> {
        val urlKeys = ArrayList<ByteArray>(urls.size)

        for (u in urls) {
            urlKeys.add(sha256(u.toByteArray(Charsets.UTF_8)))
        }

        val cfList = MutableList(urlKeys.size) { cfUrl2Id }

        val values: List<ByteArray?> = db.multiGetAsList(ReadOptions(), cfList, urlKeys)

        return values.indices.mapNotNull { idx ->
            val idBytes = values[idx] ?: return@mapNotNull null
            if (idBytes.size != 8) return@mapNotNull null
            bytesToLongBE(idBytes)
        }
    }

    fun getId(url: String): Long? {
        val urlBytes = url.toByteArray()
        val hashKey = sha256(urlBytes)
        val idBytes = db.get(cfUrl2Id, hashKey) ?: return null
        if (idBytes.size != 8) return null
        return bytesToLongBE(idBytes)
    }

    fun getUrl(id: Long): String? {
        val idKey = ByteArray(8)
        longToBytesBE(id, idKey)
        val urlBytes = db.get(cfId2Url, idKey) ?: return null
        return String(urlBytes, Charsets.UTF_8)
    }

    override fun close() {
        if (::cfUrl2Id.isInitialized) cfUrl2Id.close()
        if (::cfId2Url.isInitialized) cfId2Url.close()
        if (::defaultCf.isInitialized) defaultCf.close()
        if (::db.isInitialized) db.close()
    }

    private fun sha256(input: ByteArray): ByteArray {
        val md = digestTL.get()
        md.reset()
        return md.digest(input)
    }

    private fun longToBytesBE(v: Long, out: ByteArray) {
        out[0] = (v ushr 56).toByte()
        out[1] = (v ushr 48).toByte()
        out[2] = (v ushr 40).toByte()
        out[3] = (v ushr 32).toByte()
        out[4] = (v ushr 24).toByte()
        out[5] = (v ushr 16).toByte()
        out[6] = (v ushr 8).toByte()
        out[7] = (v).toByte()
    }

    private fun bytesToLongBE(b: ByteArray): Long {
        return ((b[0].toLong() and 0xFF) shl 56) or
                ((b[1].toLong() and 0xFF) shl 48) or
                ((b[2].toLong() and 0xFF) shl 40) or
                ((b[3].toLong() and 0xFF) shl 32) or
                ((b[4].toLong() and 0xFF) shl 24) or
                ((b[5].toLong() and 0xFF) shl 16) or
                ((b[6].toLong() and 0xFF) shl 8) or
                (b[7].toLong() and 0xFF)
    }

}