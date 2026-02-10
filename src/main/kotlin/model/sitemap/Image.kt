package dev.qr.model.sitemap

import kotlinx.serialization.Serializable
import nl.adaptivity.xmlutil.serialization.XmlElement
import nl.adaptivity.xmlutil.serialization.XmlSerialName

@Serializable
@XmlSerialName("image", namespace = "http://www.google.com/schemas/sitemap-image/1.1", prefix = "image")
data class Image(
    @XmlElement
    @XmlSerialName("loc")
    val location: String
)