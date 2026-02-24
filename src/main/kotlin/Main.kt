package dev.qr

import dev.qr.services.ModrinthService
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import java.io.File
import java.security.MessageDigest

suspend fun main() {
    val startAt = 0
    val pages = startAt + 1085 // When calling infinitely, as of 2025-02-25, 1084 is the maximum number of pages, so this is hard-coded now
    for (pageNum in startAt..<pages) {
        println("Current: $pageNum")
        val jsonElement = ModrinthService.load(mapOf("limit" to "100", "offset" to "${pageNum * 100}"))
        ModrinthService.saveToFile(jsonElement.toString())
        delay(200) // Technically the API provides notices on rate limits, but this is easier
    }
}

private val json = Json { prettyPrint = true }

fun println(element: JsonElement) {
    println(json.encodeToString(element))
}
