package dev.qr

import dev.qr.service.GrokService
import service.StemService
import java.text.NumberFormat

suspend fun main() {
//    val text = ""
//
//    val result = StemService.detectLanguage(text) ?: return
//
//    val processedText = StemService.processText(text, result.language)
//
//    println("< $text")
//    println("[${result.language}] ${NumberFormat.getPercentInstance().format(result.confidence)}")
//    println("> $processedText")
GrokService.load()
}