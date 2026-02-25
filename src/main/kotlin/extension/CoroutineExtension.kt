package dev.qr.extension

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlin.coroutines.CoroutineContext

private val threads = Runtime.getRuntime().availableProcessors() * 4

@OptIn(DelicateCoroutinesApi::class)
val globalContext = CoroutineScope(
    SupervisorJob()
            + newFixedThreadPoolContext(threads, "wk")
            + Dispatchers.IO.limitedParallelism(threads, "io")
)

fun runMain(
    block: suspend CoroutineScope.() -> Unit
): Unit = runBlocking(
    globalContext.coroutineContext,
    block
)

// https://medium.com/@josehhbraz/parallel-map-in-flow-kotlin-f9735c9dc237
fun <T, R> Flow<T>.parallelMap(
    context: CoroutineContext = Dispatchers.Default,
    transform: suspend (T) -> R
): Flow<R> {
    val scope = CoroutineScope(context + SupervisorJob())
    val semaphore = Semaphore(threads)

    return map { value ->
        semaphore.acquire()
        scope.async {
            try {
                transform(value)
            } finally {
                semaphore.release()
            }
        }
    }.buffer(threads * 16).map { it.await() }.onCompletion { scope.cancel() }
}
