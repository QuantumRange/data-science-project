package scripts

import dev.qr.scripts.pipeline.storage.TextStorage
import dev.qr.scripts.pipeline.transformer.TextTransformer
import dev.qr.util.runMain
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import scripts.pipeline.storage.CompressedStorage
import scripts.pipeline.storage.IdInjectedStorage
import scripts.pipeline.storage.SourceStorage
import scripts.pipeline.transformer.CompressorTransformer
import scripts.pipeline.transformer.IdInjectorTransformer

private object MigrateCrawls

private val log = LoggerFactory.getLogger(MigrateCrawls::class.java)

fun main(): Unit = runMain {
    log.info("Performing transformation...")

    val pipelineJob = launch {
//        CompressorTransformer.apply(SourceStorage, CompressedStorage)
//        IdInjectorTransformer.apply(CompressedStorage, IdInjectedStorage)
        TextTransformer.apply(IdInjectedStorage, TextStorage)
    }

    Runtime.getRuntime().addShutdownHook(Thread {
        runBlocking {
            log.info("Initializing soft shutdown...")
            pipelineJob.cancelAndJoin()
        }
    })

    pipelineJob.join()
    log.info("Done!")
}
