package util

import com.codahale.metrics.Meter
import dev.qr.util.globalContext
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import okhttp3.internal.format
import org.slf4j.Logger
import kotlin.math.ceil

val textMeter = Meter()

class ProgressBarJob(
    val log: Logger,
    var progress: Int,
    var max: Int
) {
    internal var job: Job? = null
    private val meter = Meter()

    suspend fun run(): Unit = supervisorScope {
        while (isActive) {
            val percent = progress.toDouble() / max.toDouble()

            val perSecond = meter.fiveMinuteRate
            val left = max - progress

            // 0.01 / 1 000 000
            // 1273tokens
            // 0.00 00 8275308324768756



            val progressChars = ceil(percent * 30).toInt()
            log.info(
                "{}% [{}] {}/s {}s left | {}/s",
                format("%02.03f", percent * 100.0),
                "#".repeat(progressChars) + " ".repeat(30 - progressChars),
                format("%03.04f", meter.fiveMinuteRate),
                format("%03.02f", left / perSecond),
                format("%05.03f", textMeter.fiveMinuteRate),
            )

            delay(15_000L)
        }
    }

    fun inc(amount: Int) {
        meter.mark(amount.toLong())
        progress += amount
    }

    fun silentInc(amount: Int) {
        progress += amount
    }
}

suspend fun progressBar(
    log: Logger,
    max: Int
): ProgressBarJob {
    val job = ProgressBarJob(
        log, 0, max
    )

    job.job = globalContext.launch {
        job.run()
    }

    return job
}
