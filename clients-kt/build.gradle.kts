/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO Move buildVersionFileName to project.properties?
val buildVersionFileName = "kafka-version.properties"

plugins {
    java // TODO Remove java plugin once fully migrated
    kotlin("jvm")
    `java-library`
    `java-test-fixtures`
}

val generator: Configuration by configurations.creating

dependencies {
    implementation(libs.luben.zstd)
    implementation(libs.lz4.java)
    implementation(libs.snappy.java)
    implementation(libs.slf4j.api)

    compileOnly(libs.jackson.databind) // for SASL/OAUTHBEARER bearer token parsing
    compileOnly(libs.jackson.jdk8.datatypes)
    compileOnly(libs.bitbucket.jose4j) // for SASL/OAUTHBEARER JWT validation; only used by broker

    testImplementation(kotlin("test"))
    testImplementation(libs.bouncycastle.bcpkix)
    testImplementation(libs.jackson.jaxrs.jsonProvider)
    testImplementation(libs.bitbucket.jose4j)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.log4j.reload4j)
    testImplementation(libs.mockito.inline)
    testImplementation(libs.mockito.kotlin)

    testRuntimeOnly(libs.slf4j.log4j)
    testRuntimeOnly(libs.jackson.databind)
    testRuntimeOnly(libs.jackson.jdk8.datatypes)

    testFixturesImplementation(kotlin("test"))
    testFixturesImplementation(libs.slf4j.api)

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
    maxParallelForks = Runtime.getRuntime().availableProcessors()
}

tasks.withType<Jar> {
    archiveBaseName.set("kafka-clients-kt")
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
}

tasks.compileTestKotlin {
    dependsOn("processTestMessages")
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
