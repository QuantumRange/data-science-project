package util

import com.fleeksoft.ksoup.Ksoup
import com.prof18.rssparser.RssParser
import com.prof18.rssparser.model.RssChannel
import dev.qr.model.BodyKind
import dev.qr.util.UrlUtil
import io.ktor.http.Url
import io.ktor.http.toURI
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import nl.adaptivity.xmlutil.serialization.XML
import dev.qr.model.sitemap.SitemapIndex
import dev.qr.model.sitemap.UrlSet
import java.net.URI

object LinkUtil {

    private val parser = RssParser()
    private val urlRegex = Regex("/^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$/")

    suspend fun extractLinks(url: Url, kind: BodyKind, content: String): List<String> {
        val links = mutableListOf<String>()

        fun String.relative(): String? {
            return runCatching { url.toURI().resolve(this) }.getOrNull()?.toString()
        }

        fun Collection<String>.relative(): List<String> {
            return this.mapNotNull { it.relative() }.distinct()
        }

        val content = content
        val lastSegment = url.rawSegments.lastOrNull()

        when (kind) {
            BodyKind.TEXT -> {
                if (lastSegment == "robots.txt") {
                    runCatching {
                        content.lines()
                            .map { it.substringBefore("#").trim() }
                            .filter { it.isNotBlank() }
                            .map { line ->
                                if (line.startsWith("Allow:") && !line.contains("*"))
                                    line.substringAfter(": ").relative()?.let { links.add(it) }
                                if (line.startsWith("Sitemap:"))
                                    line.substringAfter(": ").relative()?.let { links.add(it) }
                            }
                    }
                } else {
                    links.addAll(
                        urlRegex.findAll(content)
                            .map { it.value }
                            .toList()
                            .relative()
                    )
                }
            }

            BodyKind.XML -> {
                runCatching {
                    links.addAll(
                        XML.decodeFromString<UrlSet>(content).urls
                            .map { url -> url.location }
                            .relative()
                    )
                }
                runCatching {
                    links.addAll(
                        XML.decodeFromString<SitemapIndex>(content).sitemaps
                            .map { url -> url.location }
                            .relative()
                    )
                }
                runCatching {
                    val channel: RssChannel = parser.parse(content)

                    links.addAll(
                        (listOf(channel.link)
                                + channel.items.flatMap { listOf(it.link, it.sourceUrl, it.commentsUrl) })
                            .filterNotNull()
                            .relative()
                    )
                }
            }

            BodyKind.HTML -> {
                links.addAll(extractLinks(url.toString(), content).relative())
                runCatching {
                    links.addAll(
                        urlRegex.findAll(content)
                            .map { it.value }
                            .toList()
                            .relative()
                    )
                }
            }

            BodyKind.PDF -> {
                runCatching {
                    links.addAll(
                        Json.parseToJsonElement(content)
                            .jsonObject["references"]!!
                            .jsonObject["url"]!!
                            .jsonArray
                            .map { url -> url.jsonPrimitive.content }
                            .mapNotNull { url -> runCatching { Url(URI(url).normalize()) }.getOrNull()?.toString() }
                    )
                }
            }

            else -> {}
        }

        return links.distinct()
    }

    private fun extractLinks(base: String, content: String): List<String> = runCatching {
        val doc = Ksoup.parse(content, base)

        val baseUrl = URI(base)

        fun preprocess(url: String): String? {
            val href = url.trim()
            if (href.isEmpty()) return null

            val illegalPrefixes = listOf("javascript:", "data:", "mailto:", "tel:", "sms:")
            val lowerHref = href.lowercase()
            if (illegalPrefixes.any { it == lowerHref }) return null

            val absoluteUrl = runCatching { baseUrl.resolve(href).normalize() }.getOrNull() ?: return null

            val scheme = absoluteUrl.scheme?.lowercase() ?: return null
            if (scheme != "http" && scheme != "https") return null
            if (absoluteUrl.host.isNullOrBlank()) return null

            val absoluteUrlStr = absoluteUrl.toASCIIString()

            return UrlUtil.processUrl(absoluteUrlStr)?.toString()
        }

        doc.getElementsByAttribute("href")
            .mapNotNull { it.attribute("href")?.value }
            .mapNotNull { preprocess(it) }
    }.getOrNull() ?: emptyList()

}