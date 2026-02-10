package dev.qr.model.sitemap

import kotlinx.datetime.LocalDate
import kotlinx.serialization.Serializable
import nl.adaptivity.xmlutil.serialization.XmlElement
import nl.adaptivity.xmlutil.serialization.XmlSerialName

@Serializable
@XmlSerialName("news", namespace = "http://www.google.com/schemas/sitemap-news/0.9", prefix = "news")
data class News(
    val publication: Publication,

    @XmlElement
    @XmlSerialName("publication_date")
    val publicationDate: LocalDate,

    @XmlElement
    val title: String,
)