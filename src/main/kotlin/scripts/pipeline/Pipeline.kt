package scripts.pipeline

import dev.qr.scripts.pipeline.PipelineStorage
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory

sealed interface Pipeline

private val log = LoggerFactory.getLogger(Pipeline::class.java)
