// TODO Move buildVersionFileName to project.properties?
val buildVersionFileName = "kafka-version.properties"

plugins {
    java // TODO Remove java plugin once fully migrated
    kotlin("jvm")
    `java-library`
}

val generator: Configuration by configurations.creating

dependencies {
    implementation(Deps.Libs.zstd)
    implementation(Deps.Libs.lz4)
    implementation(Deps.Libs.snappy)
    implementation(Deps.Libs.slf4jApi)

    compileOnly(Deps.Libs.jacksonDatabind) // for SASL/OAUTHBEARER bearer token parsing
    compileOnly(Deps.Libs.jacksonJDK8Datatypes)
    compileOnly(Deps.Libs.jose4j) // for SASL/OAUTHBEARER JWT validation; only used by broker

    testImplementation(kotlin("test"))
    testImplementation(Deps.Libs.bcpkix)
    testImplementation(Deps.Libs.jacksonJaxrsJsonProvider)
    testImplementation(Deps.Libs.jose4j)
    testImplementation(Deps.Libs.junitJupiter)
    testImplementation(Deps.Libs.log4j)
    testImplementation(Deps.Libs.mockitoInline)

    testRuntimeOnly(Deps.Libs.slf4jlog4j)
    testRuntimeOnly(Deps.Libs.jacksonDatabind)
    testRuntimeOnly(Deps.Libs.jacksonJDK8Datatypes)

    generator(project(":generator-kt"))
}

tasks.register("createVersionFile") {
    val receiptFile = File("$buildDir/kafka/$buildVersionFileName")
    val commitId = determineCommitId(project, rootDir)
    inputs.property("commitId", commitId)
    inputs.property("version", version)
    outputs.file(receiptFile)

    doLast {
        val data = mapOf(
            "commitId" to commitId,
            "version" to version,
        )

        receiptFile.parentFile.mkdirs()

        val content = data.entries
            .map { "${it.key}=${it.value}" }
            .sorted()
            .joinToString("\n")

        receiptFile.writeText(
            text = content,
            charset = Charsets.ISO_8859_1,
        )
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Jar> {
    archiveBaseName.set("kafka-clients")
    dependsOn("createVersionFile")
    from("$buildDir") {
        include("kafka/$buildVersionFileName")
    }
}

tasks.clean {
    doFirst {
        delete("$buildDir/kafka/")
    }
}

tasks.register<JavaExec>("processMessages") {
    mainClass.set("org.apache.kafka.message.MessageGenerator")
    classpath = generator
    args = listOf(
        "-p", "org.apache.kafka.common.message",
        "-o", "src/generated/kotlin/org/apache/kafka/common/message",
        "-i", "src/main/resources/common/message",
        "-t", "ApiMessageTypeGenerator",
        "-m", "MessageDataGenerator", "JsonConverterGenerator",
    )
    inputs.dir("src/main/resources/common/message")
        .withPropertyName("messages")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.cacheIf { true }
    outputs.dir("src/generated/kotlin/org/apache/kafka/common/message")
}

tasks.register<JavaExec>("processTestMessages") {
    mainClass.set("org.apache.kafka.message.MessageGenerator")
    classpath = generator
    args = listOf(
        "-p", "org.apache.kafka.common.message",
        "-o", "src/generated-test/kotlin/org/apache/kafka/common/message",
        "-i", "src/test/resources/common/message",
        "-m", "MessageDataGenerator", "JsonConverterGenerator",
    )
    inputs.dir("src/test/resources/common/message")
        .withPropertyName("testMessages")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.cacheIf { true }
    outputs.dir("src/generated-test/kotlin/org/apache/kafka/common/message")
}

java.sourceSets["main"].java {
    srcDir("src/generated/java")
}

kotlin.sourceSets["main"].kotlin {
    srcDir("src/generated/kotlin")
}

java.sourceSets["test"].java {
    srcDir("src/generated-test/java")
}

kotlin.sourceSets["test"].kotlin {
    srcDir("src/generated-test/kotlin")
}

tasks.compileKotlin {
    dependsOn("processMessages")
    dependsOn("processKotlinMessages")
}

tasks.compileTestKotlin {
    dependsOn("processTestMessages")
    dependsOn("processKotlinTestMessages")
}

tasks.javadoc {
    include("**/org/apache/kafka/clients/admin/*")
    include("**/org/apache/kafka/clients/consumer/*")
    include("**/org/apache/kafka/clients/producer/*")
    include("**/org/apache/kafka/common/*")
    include("**/org/apache/kafka/common/acl/*")
    include("**/org/apache/kafka/common/annotation/*")
    include("**/org/apache/kafka/common/errors/*")
    include("**/org/apache/kafka/common/header/*")
    include("**/org/apache/kafka/common/metrics/*")
    include("**/org/apache/kafka/common/metrics/stats/*")
    include("**/org/apache/kafka/common/quota/*")
    include("**/org/apache/kafka/common/resource/*")
    include("**/org/apache/kafka/common/serialization/*")
    include("**/org/apache/kafka/common/config/*")
    include("**/org/apache/kafka/common/config/provider/*")
    include("**/org/apache/kafka/common/security/auth/*")
    include("**/org/apache/kafka/common/security/plain/*")
    include("**/org/apache/kafka/common/security/scram/*")
    include("**/org/apache/kafka/common/security/token/delegation/*")
    include("**/org/apache/kafka/common/security/oauthbearer/*")
    include("**/org/apache/kafka/common/security/oauthbearer/secured/*")
    include("**/org/apache/kafka/server/authorizer/*")
    include("**/org/apache/kafka/server/policy/*")
    include("**/org/apache/kafka/server/quota/*")
}
