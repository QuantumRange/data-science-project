package dev.qr.service

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.net.Proxy
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.time.measureTimedValue


object ProxyService {

    val queuedRequests = AtomicLong(0)
    private val log = LoggerFactory.getLogger(ProxyService::class.java)

    internal class Client(
        val proxy: String,
        val client: HttpClient
    ) {

        var failures: Int = 0
        private val timeouts = mutableMapOf<String, Timeout>()

        class Timeout(private val rateLimit: RateLimit) {

            private val requests = mutableListOf<Long>()

            fun pushRequestNow() {
                requests.add(System.currentTimeMillis())
            }

            fun inRateLimit(): Boolean {
                update()

                return requests.count() > rateLimit.limit
            }

            private fun update() {
                requests.removeAll { it + rateLimit.duration.toMillis() < System.currentTimeMillis() }
            }


        }

        fun getTimeout(rateLimit: RateLimit): Timeout = timeouts.getOrPut(rateLimit.key) { Timeout(rateLimit) }

    }

    private val clients = mutableListOf<Client>()

    init {
        log.info("Loading proxies")
        val ips = ProxyService.javaClass.classLoader.getResource("proxies.txt")!!
            .readText()
            .lines()
            .filter { it.isNotEmpty() }

        for (ip in ips) {
            val dat = ip.split(":")

            runCatching {
                clients.add(
                    Client(
                        ip,
                        HttpClient(CIO) {
                            defaultRequest {
                                val credentials =
                                    Base64.getEncoder().encodeToString("${dat[2]}:${dat[3]}".toByteArray())
                                header(HttpHeaders.ProxyAuthorization, "Basic $credentials")
//                                userAgent("Spider/1.0 no data gets used for ai or is being sold, personal project!")
                            }
                            engine {
                                proxy = Proxy(Proxy.Type.HTTP, InetSocketAddress(dat[0], dat[1].toInt()))
                            }
                            install(HttpTimeout) {
                                requestTimeoutMillis = 10_000
                                socketTimeoutMillis = 10_000
                                connectTimeoutMillis = 10_000
                            }
                        }
                    )
                )
            }.onFailure { log.warn("Failed to add proxy $ip", it) }
        }

        log.info("Loaded ${ips.size} proxies")
    }

    private val rateLimitMutex = Mutex()

    private suspend fun requestClient(rateLimit: RateLimit, depth: Int = 0): Client {
        return rateLimitMutex.withLock {
            val list = clients
                .filter { !it.getTimeout(rateLimit).inRateLimit() }
                .sortedBy { it.failures }
                .take(50)

            if (list.isEmpty()) return@withLock null

            val client = list[Random.nextInt(list.size)]

            client.getTimeout(rateLimit).pushRequestNow()

            client
        } ?: run {
            if (depth > 100) throw IllegalStateException("No proxies left ???")

            delay(1_000)

            return requestClient(rateLimit, depth + 1)
        }
    }

    class RateLimit(
        val key: String,
        val limit: Int,
        val duration: Duration
    )

    suspend fun <T> web(
        rateLimit: RateLimit,
        request: suspend (client: HttpClient) -> HttpResponse,
        block: suspend (response: HttpResponse) -> T
    ): T {
        queuedRequests.incrementAndGet()
        while (true) {
            val client = requestClient(rateLimit)

            val (response, duration) = measureTimedValue {
                runCatching { supervisorScope { request(client.client) } }
            }

            if (!response.isSuccess) {
                continue
            }

            val result = runCatching { supervisorScope { block(response.getOrNull()!!) } }

            if (!result.isSuccess) {
                continue
            }

            queuedRequests.decrementAndGet()
            return result.getOrThrow()
        }
    }

}

