plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version("0.7.0")
}

rootProject.name = "kafka-kt"

include(
    "generator-kt",
    "clients-kt",
    "server-common-kt",
    "log4j-appender-kt",
    "trogdor-kt",
    "streams-kt",
)
