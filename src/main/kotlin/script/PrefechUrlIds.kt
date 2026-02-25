package dev.qr.scripts

import dev.qr.services.RocksDBService
import dev.qr.extension.runMain
import kotlin.io.path.Path

fun main(): Unit = runMain {
    println("Preparing db...")

    val path = Path("db/")
    RocksDBService.open(path)

    println("Loading...")
    RocksDBService.loadFromPostgresBinaryCopy(Path("data/urls_3.copy.zst"), batchRows = 1_000_000)

    println("Loaded!")
}
