package dev.qr.parquet

import dev.qr.util.globalContext
import dev.qr.util.parallelMap
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.example.Paper.schema
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.intellij.lang.annotations.Language
import java.io.File

object ParquetService {

    private suspend fun write(
        file: File,
        @Language("JSON") schema: String,
        data: Flow<GenericRecord>,
        codec: CompressionCodecName = CompressionCodecName.ZSTD
    ) {
        val schema = Schema.Parser().parse(schema)!!
        val conf = Configuration()

        AvroParquetWriter.builder<GenericRecord>(LocalOutputFile(file.toPath()))
            .withSchema(schema)
            .withConf(conf)
            .withCompressionCodec(codec)
            .build()
            .use { writer ->
                data.collect { writer.write(it) }
            }
    }

    suspend fun <T> write(
        file: File,
        @Language("JSON") schema: String,
        data: Flow<T>,
        codec: CompressionCodecName = CompressionCodecName.ZSTD,
        block: suspend GenericRecord.(data: T) -> Unit
    ) = coroutineScope {
        val parsedSchema = Schema.Parser().parse(schema)

        write(
            file,
            schema,
            data.parallelMap { GenericData.Record(parsedSchema).apply { block(it) } },
            codec
        )
    }

    suspend fun read(
        file: File,
        @Language("JSON") schema: String
    ): Flow<GenericRecord> {
        val schema = Schema.Parser().parse(schema)

        val configuration: Configuration = Configuration().apply {
            AvroReadSupport.setAvroReadSchema(this, schema)
            AvroReadSupport.setRequestedProjection(this, schema)
        }

        return flow {
            AvroParquetReader.builder<GenericRecord>(LocalInputFile(file.toPath()))
                .withConf(configuration)
                .build()
                .use { reader ->
                    while (true) {
                        val record = reader.read() ?: break
                        emit(record)
                    }
                }
        }
    }

}