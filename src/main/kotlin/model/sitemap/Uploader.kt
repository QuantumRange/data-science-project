package dev.qr.model.sitemap

import kotlinx.serialization.Serializable
import nl.adaptivity.xmlutil.serialization.XmlValue

@Serializable
data class Uploader(
    val info: String,

    @XmlValue
    val name: String,
)
