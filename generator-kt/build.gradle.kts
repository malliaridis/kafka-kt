plugins {
    kotlin("jvm")
}

dependencies {
    implementation(Deps.Libs.argparse4j)
    implementation(Deps.Libs.jacksonDatabind)
    implementation(Deps.Libs.jacksonJDK8Datatypes)
    implementation(Deps.Libs.jacksonJaxrsJsonProvider)

    testImplementation(kotlin("test"))
}

// TODO Disable javadoc / dokka
//javadoc {
//    enabled = false
//}
