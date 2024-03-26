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

plugins {
    java // TODO Remove java plugin once fully migrated
    kotlin("jvm")
    `java-library`
}

dependencies {
    implementation(project(":clients-kt"))
    implementation(project(":log4j-appender-kt"))

    implementation(libs.sourceforge.argparse4j)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jdk8.datatypes)
    implementation(libs.slf4j.api)
    implementation(libs.slf4j.log4j)
    implementation(libs.jackson.jaxrs.jsonProvider)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.inject.hk2)
    implementation(libs.javax.bind.api) // Jersey dependency that was available in the JDK before Java 9
    implementation(libs.javax.activation) // Jersey dependency that was available in the JDK before Java 9
    implementation(libs.jetty.server)
    implementation(libs.jetty.servlet)
    implementation(libs.jetty.servlets)

    testImplementation(project(":clients-kt"))
    testImplementation(testFixtures(project(":clients-kt")))

    testImplementation(kotlin("test"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.inline) // supports mocking static methods, final classes, etc.
    testImplementation(libs.mockito.kotlin)

    testRuntimeOnly(libs.slf4j.log4j)
}

tasks.test {
    useJUnitPlatform()
    maxParallelForks = Runtime.getRuntime().availableProcessors()
}
