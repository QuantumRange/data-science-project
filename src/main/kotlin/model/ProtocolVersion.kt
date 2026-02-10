package dev.qr.model

import io.ktor.http.HttpProtocolVersion

enum class ProtocolVersion(val representation: HttpProtocolVersion) {
    HTTP_3_0(HttpProtocolVersion.HTTP_3_0),
    HTTP_2_0(HttpProtocolVersion.HTTP_2_0),
    HTTP_1_1(HttpProtocolVersion.HTTP_1_1),
    HTTP_1_0(HttpProtocolVersion.HTTP_1_0),
    SPDY_3(HttpProtocolVersion.SPDY_3),
    QUIC(HttpProtocolVersion.QUIC),
    UNKNOWN(HttpProtocolVersion("UNKNOWN", 0, 0)),
}