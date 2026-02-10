package dev.qr.model.sitemap

import kotlinx.serialization.Serializable
import nl.adaptivity.xmlutil.serialization.XmlElement
import nl.adaptivity.xmlutil.serialization.XmlSerialName

@Serializable
@XmlSerialName("publication", namespace = "http://www.google.com/schemas/sitemap-news/0.9", prefix = "news")
data class Publication(
    @XmlElement
    val name: String,

    @XmlElement
    val language: String,
)