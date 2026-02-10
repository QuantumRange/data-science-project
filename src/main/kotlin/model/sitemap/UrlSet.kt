package dev.qr.model.sitemap

import kotlinx.serialization.Serializable
import nl.adaptivity.xmlutil.serialization.XmlSerialName

@Serializable
@XmlSerialName("urlset", namespace = "http://www.sitemaps.org/schemas/sitemap/0.9", prefix = "")
data class UrlSet(
    val urls: List<Url>
)