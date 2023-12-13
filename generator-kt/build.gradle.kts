plugins {
    kotlin("jvm")
}

dependencies {
    implementation(libs.sourceforge.argparse4j)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jdk8.datatypes)
    implementation(libs.jackson.jaxrs.jsonProvider)

    testImplementation(kotlin("test"))
}

// TODO Disable javadoc / dokka
//javadoc {
//    enabled = false
//}
