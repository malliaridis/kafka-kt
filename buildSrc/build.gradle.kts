import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

dependencies {
    // TODO Move to Deps
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.8.20")
}

//kotlin {
//    sourceSets.getByName("main").kotlin.srcDir("buildSrc/src/main/kotlin")
//}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "1.8"
    }
}
