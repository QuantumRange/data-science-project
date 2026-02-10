package dev.qr.parquet

import org.intellij.lang.annotations.Language

object ParquetSchema {

    @Language("JSON")
    const val CRAWL_V1 = """
    {
      "type": "record",
      "name": "Crawl",
      "namespace": "example",
      "fields": [
        { "name": "url", "type": "long" },
    
        {
          "name": "timestamp",
          "type": { "type": "long", "logicalType": "timestamp-millis" },
          "doc": "OffsetDateTime normalized to UTC; epoch millis."
        },
    
        {
          "name": "duration",
          "type": ["null", "long"],
          "default": null,
          "doc": "java.time.Duration encoded as total milliseconds."
        },
    
        { "name": "version", "type": ["null", "string"], "default": null },
        { "name": "status",  "type": ["null", "int"],    "default": null },
    
        {
          "name": "header",
          "type": [
            "null",
            { "type": "map", "values": { "type": "array", "items": "string" } }
          ],
          "default": null
        },
    
        { "name": "content", "type": ["null", "bytes"], "default": null },
    
        { 
          "name": "content_type",
          "type": [
            "null",
            "int"
          ],
          "default": null
        }
      ]
    }
    """

    @Language("JSON")
    const val CRAWL_V2 = """
    {
      "type": "record",
      "name": "Crawl",
      "namespace": "example",
      "fields": [
        { "name": "url", "type": "long" },
    
        {
          "name": "timestamp",
          "type": "long",
          "doc": "OffsetDateTime normalized to UTC; epoch millis."
        },
    
        {
          "name": "duration",
          "type": ["null", "long"],
          "default": null,
          "doc": "java.time.Duration encoded as total milliseconds."
        },
    
        { "name": "version", "type": ["null", "string"], "default": null },
        { "name": "status",  "type": ["null", "int"],    "default": null },
    
        {
          "name": "header",
          "type": ["null", "string"],
          "default": null
        },
    
        { "name": "content", "type": ["null", "string"], "default": null },
    
        { 
          "name": "content_type",
          "type": [
            "null",
            "int"
          ],
          "default": null
        }
      ]
    }
    """

    @Language("JSON")
    const val CRAWL_V3 = """
    {
      "type": "record",
      "name": "Crawl",
      "namespace": "example",
      "fields": [
        { "name": "id", "type": "long" },
        { "name": "url", "type": "string" },
        
        { "name": "timestamp", "type": "long" },
        { "name": "duration", "type": "long" },
        { "name": "http_version", "type": "int" },
        { "name": "status",  "type": ["null", "int"],    "default": null },
        { "name": "header", "type": ["null", "string"], "default": null },
        
        { "name": "type", "type": "int", "default": null },
        { "name": "content", "type": ["null", "string"], "default": null },
        { "name": "links", "type": "string", "default": null }
      ]
    }
    """

    @Language("JSON")
    const val CRAWL_ID = """
    {
      "type": "record",
      "name": "CrawlIds",
      "namespace": "example",
      "fields": [
        { "name": "id", "type": "long" }
      ]
    }
    """

}