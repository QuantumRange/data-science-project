package dev.qr.util

import kotlinx.serialization.decodeFromString
import nl.adaptivity.xmlutil.serialization.XML

inline fun <reified T> tryDecodeXML(str: String): T? {
    return runCatching {
        XML.decodeFromString<T>(str)
    }.getOrNull()
}