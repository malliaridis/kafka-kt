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
    api(project(":clients-kt"))

    implementation(libs.slf4j.api)
    implementation(libs.yammer.metrics)
    implementation(libs.jopt.simple)
    implementation(libs.jackson.databind)
    implementation(libs.pcollections.pcollections)

    testImplementation(project(":clients-kt"))
    testImplementation(kotlin("test"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.inline) // supports mocking static methods, final classes, etc.
    testImplementation(libs.hamcrest.hamcrest)

    testRuntimeOnly(libs.slf4j.log4j)
}
