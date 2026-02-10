package dev.qr.util

import io.ktor.http.*
import io.ktor.util.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.net.URI

object UrlUtil {

    private val log = LoggerFactory.getLogger(javaClass)

    @Serializable
    private data class ClearUrlData(
        val providers: Map<String, RawPattern>
    )

    @Serializable
    private data class RawPattern(
        val urlPattern: String,
        val completeProvider: Boolean = false,
        val rules: List<String> = emptyList(),
        val rawRules: List<String> = emptyList(),
        val referralMarketing: List<String> = emptyList(),
        val exceptions: List<String> = emptyList(),
        val redirections: List<String> = emptyList()
    )

    private data class Pattern(
        val urlPattern: Regex,
        val completeProvider: Boolean,
        val rules: List<Regex> = emptyList(),
        val rawRules: List<Regex> = emptyList(),
        val referralMarketing: List<Regex> = emptyList(),
        val exceptions: List<Regex> = emptyList(),
        val redirections: List<Regex> = emptyList()
    ) {
        fun match(depth: Int, url: Url): Url {
            if (!urlPattern.containsMatchIn(url.toString())) return url
            if (exceptions.any { it.containsMatchIn(url.toString()) }) return url

            for (redirect in redirections) {
                redirect.find(url.toString())?.let { result ->
                    return processUrl(result.groupValues[1].decodeURLQueryComponent(), depth = depth + 1) ?: url
                }
            }

            val fieldRules = rules + referralMarketing

            val cleanFieldUrl = URLBuilder(url).apply {
                parameters.names()
                    .filter { fieldRules.any { rule -> rule.matches(it) } }
                    .forEach { parameters.remove(it) }
            }.build()

            if (rawRules.isEmpty()) return cleanFieldUrl

            return Url(
                rawRules.fold(
                    cleanFieldUrl.toString()
                ) { url, rawRule ->
                    rawRule.replace(url, "")
                }
            )
        }
    }

    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true
    }

    // https://rules2.clearurls.xyz/data.minify.json
    private val patterns = json.decodeFromString<ClearUrlData>(
        (UrlUtil.javaClass.classLoader
            .getResource("data.minify.json") ?: error("Need clear url data"))
            .readText()
    ).providers.values.toList().map { raw ->
        Pattern(
            Regex(raw.urlPattern),
            raw.completeProvider,
            raw.rules.map { Regex(it) },
            raw.rawRules.map { Regex(it) },
            raw.referralMarketing.map { Regex(it) },
            raw.exceptions.map { Regex(it) },
            raw.redirections.map { Regex(it) }
        )
    }

    fun processUrl(url: String, depth: Int = 0): Url? {
        val url = runCatching { Url(URI(url).normalize()) }.getOrNull() ?: return null

        if (url.host.length > 120) return null

        return processUrl(url, depth)
    }

    fun processUrl(url: Url, depth: Int = 0): Url {
        val url = Url(url.toURI().normalize())

        val normalizedUri = URLBuilder(
            protocol = url.protocol,
            host = url.host.lowercase(),
            pathSegments = url.rawSegments,
            parameters = url.parameters
        ).build()

        val filteredUri = if (depth < 10) {
            patterns.fold(
                normalizedUri
            ) { url, pattern -> pattern.match(depth, url) }
        } else {
            log.warn("Url $normalizedUri redirected more then 10 times!")
            normalizedUri
        }

        val sortedUri = URLBuilder(filteredUri).apply {
            parameters.clear()
            filteredUri.parameters
                .toMap()
                .toList()
                .map { it.copy(second = it.second.sorted()) }
                .sortedBy { it.first }
                .forEach { (key, values) ->
                    parameters.appendAll(key, values)
                }
        }.build()

        return sortedUri
    }

}
