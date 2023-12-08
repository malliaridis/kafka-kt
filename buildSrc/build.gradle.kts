import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

dependencies {
    // TODO Move to Deps
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.9.0")
}

//kotlin {
//    sourceSets.getByName("main").kotlin.srcDir("buildSrc/src/main/kotlin")
//}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "11"
    }
}
