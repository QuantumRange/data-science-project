package script.pipeline

import org.slf4j.LoggerFactory

sealed interface Pipeline

private val log = LoggerFactory.getLogger(Pipeline::class.java)
