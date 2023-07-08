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

package org.apache.kafka.message

import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import java.util.concurrent.TimeUnit

class IsNullConditionalTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    @Throws(Exception::class)
    fun testNullCheck() {
        val buffer = CodeBuffer()
        IsNullConditional.forName("foobar")
            .nullableVersions(Versions.parse(input = "2+", defaultVersions = null)!!)
            .possibleVersions(Versions.parse(input = "0+", defaultVersions = null)!!)
            .ifNull { buffer.printf("println(\"null\")%n") }
            .generate(buffer)
        VersionConditionalTest.claimEquals(
            buffer,
            "if (foobar == null) {%n",
            "    println(\"null\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAnotherNullCheck() {
        val buffer = CodeBuffer()
        IsNullConditional.forName("foobar")
            .nullableVersions(Versions.parse(input = "0+", defaultVersions = null)!!)
            .possibleVersions(Versions.parse(input = "2+", defaultVersions = null)!!)
            .ifNull { buffer.printf("println(\"null\")%n") }
            .ifShouldNotBeNull { buffer.printf("println(\"not null\")%n") }
            .generate(buffer)
        VersionConditionalTest.claimEquals(
            buffer,
            "if (foobar == null) {%n",
            "    println(\"null\")%n",
            "} else {%n",
            "    println(\"not null\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testNotNullCheck() {
        val buffer = CodeBuffer()
        IsNullConditional.forName("foobar")
            .nullableVersions(Versions.parse(input = "0+", defaultVersions = null)!!)
            .possibleVersions(Versions.parse(input = "2+", defaultVersions = null)!!)
            .ifShouldNotBeNull { buffer.printf("println(\"not null\")%n") }
            .generate(buffer)
        VersionConditionalTest.claimEquals(
            buffer,
            "if (foobar != null) {%n",
            "    println(\"not null\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testNeverNull() {
        val buffer = CodeBuffer()
        IsNullConditional.forName("baz")
            .nullableVersions(Versions.parse(input = "0-2", defaultVersions = null)!!)
            .possibleVersions(Versions.parse(input = "3+", defaultVersions = null)!!)
            .ifNull { buffer.printf("println(\"null\")%n") }
            .ifShouldNotBeNull { buffer.printf("println(\"not null\")%n") }
            .generate(buffer)
        VersionConditionalTest.claimEquals(buffer, "println(\"not null\")%n")
    }

    @Test
    @Throws(Exception::class)
    fun testNeverNullWithBlockScope() {
        val buffer = CodeBuffer()
        IsNullConditional.forName("baz")
            .nullableVersions(Versions.parse(input = "0-2", defaultVersions = null)!!)
            .possibleVersions(Versions.parse(input = "3+", defaultVersions = null)!!)
            .ifNull { buffer.printf("println(\"null\")%n") }
            .ifShouldNotBeNull { buffer.printf("println(\"not null\")%n") }
            .alwaysEmitBlockScope(true)
            .generate(buffer)
        VersionConditionalTest.claimEquals(
            buffer,
            "run {%n",
            "    println(\"not null\")%n",
            "}%n",
        )
    }
}
