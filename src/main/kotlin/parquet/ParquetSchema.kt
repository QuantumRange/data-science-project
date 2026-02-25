package dev.qr.parquet

object ParquetSchema {

    fun load(name: String): String = ParquetSchema::class.java.getResource("/schemas/$name.json")!!.readText()

    val CRAWL_V1 = load("crawl_v1")
    val CRAWL_V2 = load("crawl_v2")
    val CRAWL_V3 = load("crawl_v3")
    val CRAWL_V4 = load("crawl_v4")
    val CRAWL_V5 = load("crawl_v5")
//    val CRAWL_V6 = load("crawl_v6")
    val AI_DATA = load("ai_data")

}