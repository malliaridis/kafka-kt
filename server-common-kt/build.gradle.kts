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
}

dependencies {
    api(project(":clients-kt"))

    implementation(libs.slf4j.api)
    implementation(libs.yammer.metrics)
    implementation(libs.jopt.simple)
    implementation(libs.jackson.databind)
    implementation(libs.pcollections.pcollections)

    testImplementation(project(":clients-kt"))
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":clients-kt")))

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.kotlin)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.inline) // supports mocking static methods, final classes, etc.
    testImplementation(libs.hamcrest.hamcrest)

    testRuntimeOnly(libs.slf4j.log4j)
}

// TODO Write out duplicate task code
tasks.register("createVersionFile") {
    val receiptFile = file("$buildDir/kafka/$buildVersionFileName")
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
    archiveBaseName.set("kafka-server-common-kt")
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
