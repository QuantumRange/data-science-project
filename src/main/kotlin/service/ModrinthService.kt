package dev.qr.services

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.UserAgent
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.readRawBytes
import io.ktor.http.URLProtocol
import io.ktor.http.isSuccess
import io.ktor.http.parameters
import io.ktor.http.path
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import java.io.File
import java.security.MessageDigest

object ModrinthService {
    // Just about the most basic web request possible
    suspend fun load(params: Map<String, String>): JsonElement{
        val client = HttpClient(CIO) {
            install(UserAgent) {
                agent = "cau_kiel_data_science_project"
            }
        }
        val response: HttpResponse = client.get{
            url {
                protocol = URLProtocol.HTTPS
                host = "api.modrinth.com"
                path("v2", "search")
                params.forEach { (key, value) ->
                    parameters.append(key, value)
                }
            }
        }
        if (!response.status.isSuccess()) {
            throw Exception("Error: ${response.status.description}")

        }
        val jsonResult = Json.parseToJsonElement(response.readRawBytes().decodeToString())
        client.close()
        return jsonResult;
    }

    fun saveToFile(data: String){
        // Hash the output and use it as the filename - means if we already have this, it won't get saved again, avoiding duplicates
        val dataHash = MessageDigest.getInstance("MD5").digest(data.toByteArray()).toHexString()
        File("./src/main/resources/data/$dataHash.json").writeText(data)
    }
}