package dev.qr.model.sitemap

import kotlinx.datetime.format.DateTimeComponents
import kotlinx.datetime.serializers.FormattedInstantSerializer

object IsoDateSerializer : FormattedInstantSerializer(
    name = "sitemap",
    format = DateTimeComponents.Formats.ISO_DATE_TIME_OFFSET,
)
