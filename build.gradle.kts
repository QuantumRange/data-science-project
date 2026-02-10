plugins {
    kotlin("jvm") version "2.3.0"
    kotlin("plugin.serialization") version "2.3.0"
}

group = "dev.qr"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://jitpack.io")
    maven("https://central.sonatype.com/repository/maven-snapshots/")
}

dependencies {
    // parquet
    implementation("org.apache.parquet:parquet-avro:1.17.0")
    implementation("org.apache.parquet:parquet-hadoop:1.17.0")
    implementation("org.apache.hadoop:hadoop-client:3.4.2")

    // parquet brotli codec
    implementation("com.github.rdblue:brotli-codec:0.1.1")

    // kotlin basics
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1")

    // html
    implementation("io.ktor:ktor-client-core:3.3.3")
    implementation("com.fleeksoft.ksoup:ksoup:0.2.5")
    implementation("in.wilsonl.minifyhtml:minify-html:0.18.1")

    // xml
    implementation("io.github.pdvrieze.xmlutil:core-jdk:0.91.3")
    implementation("io.github.pdvrieze.xmlutil:serialization-jvm:0.91.3")
    implementation("com.prof18.rssparser:rssparser:6.1.1")

    // misc
    implementation("ch.qos.logback:logback-classic:1.5.28")
    implementation("io.dropwizard.metrics:metrics-core:4.2.28")

    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(22)
}

tasks.test {
    useJUnitPlatform()
}