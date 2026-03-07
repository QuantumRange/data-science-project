package script

import dev.qr.script.pipeline.storage.TextStorage
import dev.qr.script.pipeline.transformer.LinkProcessingTransformer
import dev.qr.scripts.pipeline.PipelineStorage
import dev.qr.script.pipeline.storage.LinkStorage
import dev.qr.extension.runMain
import dev.qr.script.pipeline.storage.EnrichStorage
import dev.qr.script.pipeline.transformer.GoodTransformer
import dev.qr.script.pipeline.transformer.TextTransformer
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

private object MigrateCrawls

private val log = LoggerFactory.getLogger(MigrateCrawls::class.java)

val globalTarget: PipelineStorage = TextStorage

fun main(): Unit = runMain {
    log.info("Performing transformation...")

    val pipelineJob = launch {
//        CompressorTransformer.apply(SourceStorage, CompressedStorage)
//        IdInjectorTransformer.apply(CompressedStorage, IdInjectedStorage)
//        TextTransformer.apply(IdInjectedStorage, TextStorage)
        GoodTransformer.apply(TextStorage, EnrichStorage)
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
