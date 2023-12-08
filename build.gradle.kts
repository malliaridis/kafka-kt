plugins {
    kotlin("jvm")
}

group = "com.malliaridis.kafka"
version = "0.1.0-alpha01"

allprojects {
    repositories {
        google()
        mavenCentral()
        mavenLocal()
        maven("https://maven.pkg.jetbrains.space/public/p/compose/dev")
    }
}

kotlin {
    jvmToolchain(11)
}
