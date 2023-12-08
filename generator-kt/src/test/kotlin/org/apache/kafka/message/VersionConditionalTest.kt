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
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class VersionConditionalTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    @Throws(Exception::class)
    fun testAlwaysFalseConditional() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1-2", null)!!,
            possibleVersions = Versions.parse("3+", null)!!,
        )
            .ifMember { buffer.printf("println(\"hello world\")%n") }
            .ifNotMember { buffer.printf("println(\"foobar\")%n") }
            .generate(buffer)
        claimEquals(buffer, "println(\"foobar\")%n")
    }

    @Test
    @Throws(Exception::class)
    fun testAnotherAlwaysFalseConditional() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("3+", null)!!,
            possibleVersions = Versions.parse("1-2", null)!!
        )
            .ifMember { buffer.printf("println(\"hello world\")%n") }
            .ifNotMember { buffer.printf("println(\"foobar\")%n") }
            .generate(buffer)
        claimEquals(buffer, "println(\"foobar\")%n")
    }

    @Test
    @Throws(Exception::class)
    fun testAllowMembershipCheckAlwaysFalseFails() {
        try {
            val buffer = CodeBuffer()
            VersionConditional.forVersions(
                containingVersions = Versions.parse("1-2", null)!!,
                possibleVersions = Versions.parse("3+", null)!!,
            )
                .ifMember { buffer.printf("println(\"hello world\")%n") }
                .ifNotMember { buffer.printf("println(\"foobar\")%n") }
                .allowMembershipCheckAlwaysFalse(false)
                .generate(buffer)
        } catch (e: RuntimeException) {
            assertTrue(e.message!!.contains("no versions in common"))
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAlwaysTrueConditional() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1-5", null)!!,
            possibleVersions = Versions.parse("2-4", null)!!,
        ).ifMember { buffer.printf("println(\"hello world\")%n") }
            .ifNotMember { buffer.printf("println(\"foobar\")%n") }
            .allowMembershipCheckAlwaysFalse(false).generate(buffer)
        claimEquals(buffer, "println(\"hello world\")%n")
    }

    @Test
    @Throws(Exception::class)
    fun testAlwaysTrueConditionalWithAlwaysEmitBlockScope() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1-5", null)!!,
            possibleVersions = Versions.parse("2-4", null)!!,
        ).ifMember { buffer.printf("println(\"hello world\")%n") }
            .ifNotMember { buffer.printf("println(\"foobar\")%n") }
            .alwaysEmitBlockScope(true).generate(buffer)
        claimEquals(buffer, "run<Unit> {%n", "    println(\"hello world\")%n", "}%n")
    }

    @Test
    @Throws(Exception::class)
    fun testLowerRangeCheckWithElse() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1+", null)!!,
            possibleVersions = Versions.parse("0-100", null)!!,
        )
            .ifMember { buffer.printf("println(\"hello world\")%n") }
            .ifNotMember { buffer.printf("println(\"foobar\")%n") }
            .generate(buffer)
        claimEquals(
            buffer,
            "if (version >= 1) {%n",
            "    println(\"hello world\")%n",
            "} else {%n",
            "    println(\"foobar\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLowerRangeCheckWithIfMember() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1+", null)!!,
            possibleVersions = Versions.parse("0-100", null)!!,
        )
            .ifMember { buffer.printf("println(\"hello world\")%n") }
            .generate(buffer)
        claimEquals(
            buffer,
            "if (version >= 1) {%n",
            "    println(\"hello world\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLowerRangeCheckWithIfNotMember() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1+", null)!!,
            possibleVersions = Versions.parse("0-100", null)!!,
        )
            .ifNotMember { buffer.printf("println(\"hello world\")%n") }
            .generate(buffer)
        claimEquals(
            buffer,
            "if (version < 1) {%n",
            "    println(\"hello world\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUpperRangeCheckWithElse() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("0-10", null)!!,
            possibleVersions = Versions.parse("4+", null)!!,
        )
            .ifMember { buffer.printf("println(\"hello world\")%n") }
            .ifNotMember { buffer.printf("println(\"foobar\")%n") }
            .generate(buffer)
        claimEquals(
            buffer,
            "if (version <= 10) {%n",
            "    println(\"hello world\")%n",
            "} else {%n",
            "    println(\"foobar\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUpperRangeCheckWithIfMember() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("0-10", null)!!,
            possibleVersions = Versions.parse("4+", null)!!,
        )
            .ifMember { buffer.printf("println(\"hello world\")%n") }
            .generate(buffer)
        claimEquals(
            buffer,
            "if (version <= 10) {%n",
            "    println(\"hello world\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUpperRangeCheckWithIfNotMember() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("1+", null)!!,
            possibleVersions = Versions.parse("0-100", null)!!,
        )
            .ifNotMember { buffer.printf("println(\"hello world\")%n") }
            .generate(buffer)
        claimEquals(
            buffer,
            "if (version < 1) {%n",
            "    println(\"hello world\")%n",
            "}%n",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testFullRangeCheck() {
        val buffer = CodeBuffer()
        VersionConditional.forVersions(
            containingVersions = Versions.parse("5-10", null)!!,
            possibleVersions = Versions.parse("1+", null)!!,
        ).ifMember { buffer.printf("println(\"hello world\")%n") }
            .allowMembershipCheckAlwaysFalse(false).generate(buffer)
        claimEquals(
            buffer,
            "if ((version >= 5) && (version <= 10)) {%n",
            "    println(\"hello world\")%n",
            "}%n",
        )
    }

    companion object {
        @Throws(Exception::class)
        fun claimEquals(buffer: CodeBuffer, vararg lines: String?) {
            val stringWriter = StringWriter()
            buffer.write(stringWriter)
            val expectedStringBuilder = StringBuilder()

            for (line in lines) expectedStringBuilder.append(String.format(line!!))

            assertEquals(
                expected = stringWriter.toString(),
                actual = expectedStringBuilder.toString(),
            )
        }
    }
}
