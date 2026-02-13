package dev.qr.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

private val processors = Runtime.getRuntime().availableProcessors()

@OptIn(DelicateCoroutinesApi::class)
val globalContext = CoroutineScope(
    SupervisorJob()
            + newFixedThreadPoolContext(processors * 4, "wk")
            + Dispatchers.IO.limitedParallelism(processors * 2, "io")
)

fun runMain(
    block: suspend CoroutineScope.() -> Unit
): Unit = runBlocking(
    globalContext.coroutineContext,
    block
)

// https://medium.com/@josehhbraz/parallel-map-in-flow-kotlin-f9735c9dc237
fun <T, R> Flow<T>.parallelMap(
    context: CoroutineContext = EmptyCoroutineContext,
    transform: suspend (T) -> R
): Flow<R> {
    val scope = CoroutineScope(context + SupervisorJob())
    return map {
        scope.async { transform(it) }
    }
        .buffer(1024)
        .map { it.await() }
        .flowOn(context)
}