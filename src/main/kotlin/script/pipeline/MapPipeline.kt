package script.pipeline

import dev.qr.scripts.pipeline.FileHolder
import dev.qr.scripts.pipeline.PipelineStorage
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import util.progressBar
import kotlin.system.exitProcess

abstract class MapPipeline(val log: Logger, val maxParallel: Int) : Pipeline {

    suspend fun apply(
        sourceStorage: PipelineStorage,
        targetStorage: PipelineStorage
    ): Unit = coroutineScope {
        val semaphore = Semaphore(maxParallel)
        val progressBar = progressBar(log, sourceStorage.getObjects().size)

        val sourceObjects = sourceStorage.getObjects()
        val existingTargets = targetStorage.getObjects().map { it.name }.toSet()

        sourceObjects
            .map { source ->
                async {
                    // Already processed, nothing todo
                    if (source.name in existingTargets) {
                        progressBar.silentInc(1)
                        return@async
                    }

                    semaphore.withPermit {
                        val target = targetStorage.allocate(source.name)

                        val result = runCatching {
                            transform(source, target)
                        }

                        if (result.isFailure) {
                            log.error("Failed to transform '{}'", source.name, result.exceptionOrNull()!!)
                            exitProcess(-1)
                        }

                        withContext(NonCancellable) {
                            if (sourceStorage.isWrite) {
                                source.files.values.forEach { it.delete() }
                            }
                        }

                        progressBar.inc(1)
                    }
                }
            }
            .awaitAll()

        progressBar.job?.cancelAndJoin()
    }

    abstract suspend fun transform(
        source: FileHolder,
        target: FileHolder
    )

}