package dev.qr.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking

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