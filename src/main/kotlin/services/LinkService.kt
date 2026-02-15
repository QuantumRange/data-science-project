package dev.qr.services

import com.prof18.rssparser.RssParser
import com.prof18.rssparser.model.RssChannel
import dev.qr.model.BodyKind
import dev.qr.model.sitemap.SitemapIndex
import dev.qr.model.sitemap.UrlSet
import dev.qr.util.tryDecodeXML
import io.ktor.http.Url
import io.ktor.http.toURI
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URI

object LinkService {

    private val parser = RssParser()
    private val urlRegex = Regex("/^(https?://)?([\\da-z.-]+)\\.([a-z.]{2,6})([\\/\\w \\.-]*)*\\/?$/")
    private val rssFeedPattern = Regex(
        """<\s*(?:rss|rdf:RDF|(?:[A-Za-z_][\w.-]*:)?feed)\b""",
        setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)
    )

    fun String.relative(url: Url): String? {
        return runCatching { url.toURI().resolve(this) }.getOrNull()?.toString()
    }

    fun Collection<String>.relative(url: Url): List<String> {
        return this.distinct().mapNotNull { it.relative(url) }.distinct()
    }

    suspend fun extractLinks(url: Url, kind: BodyKind, content: String): List<String> {
        val links = mutableListOf<String>()

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
                                    line.substringAfter(": ").relative(url)?.let { links.add(it) }
                                if (line.startsWith("Sitemap:"))
                                    line.substringAfter(": ").relative(url)?.let { links.add(it) }
                            }
                    }
                } else {
                    links.addAll(
                        urlRegex.findAll(content)
                            .map { it.value }
                            .toList()
                            .relative(url)
                    )
                }
            }

            BodyKind.XML -> {
                val urlSet = tryDecodeXML<UrlSet>(content)

                if (urlSet != null) {
                    links.addAll(
                        urlSet.urls
                            .map { url -> url.location }
                            .relative(url))

                    return links.distinct()
                }

                val sitemapIndex = tryDecodeXML<SitemapIndex>(content)

                if (sitemapIndex != null) {
                    links.addAll(
                        sitemapIndex.sitemaps
                            .map { it.location }
                            .relative(url)
                    )
                    return links.distinct()
                }

                if (!rssFeedPattern.containsMatchIn(content))
                    return links.distinct()

                runCatching {
                    val channel: RssChannel = parser.parse(content)

                    links.addAll(
                        (listOf(channel.link)
                                + channel.items.flatMap { listOf(it.link, it.sourceUrl, it.commentsUrl) })
                            .filterNotNull()
                            .relative(url)
                    )
                }
            }

            BodyKind.HTML -> {
                links.addAll(extractHtmlLinks(url, content))
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

    fun extractHtmlLinks(url: Url, content: String): List<String> {
        val links = mutableListOf<String>()

        links.addAll(extractLinks(url.toString(), content).relative(url))

        runCatching {
            links.addAll(
                urlRegex.findAll(content)
                    .map { it.value }
                    .toList()
                    .relative(url)
            )
        }

        return links.distinct()
    }

    private val hrefPattern = Regex("href=([\"'])(.*?)\\1")

    private fun extractLinks(base: String, content: String): List<String> = runCatching {
        val baseUrl = URI(base)

        fun preprocess(url: String): String? {
            val href = url.trim()
            if (href.isEmpty()) return null

            val illegalPrefixes = listOf("javascript:", "data:", "mailto:", "tel:", "sms:")
            val lowerHref = href.lowercase()
            if (illegalPrefixes.any { it == lowerHref }) return null

            val absoluteUrl = runCatching {
                baseUrl
                    .resolve(href)
                    .normalize()
            }.getOrNull() ?: return null

            val scheme = absoluteUrl.scheme?.lowercase() ?: return null
            if (scheme != "http" && scheme != "https") return null
            if (absoluteUrl.host.isNullOrBlank()) return null

            return absoluteUrl.toASCIIString()
        }

        hrefPattern.findAll(content)
            .map { it.groupValues[2] }
            .mapNotNull { preprocess(it) }
            .toList()
    }.getOrNull() ?: emptyList<String>()

}