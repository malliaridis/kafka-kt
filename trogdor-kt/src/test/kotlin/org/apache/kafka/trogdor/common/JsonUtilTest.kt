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

package org.apache.kafka.trogdor.common

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import org.apache.kafka.test.TestUtils.tempFile
import org.apache.kafka.trogdor.common.JsonUtil.objectFromCommandLineArgument
import org.apache.kafka.trogdor.common.JsonUtil.openBraceComesFirst
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class JsonUtilTest {

    @Test
    fun testOpenBraceComesFirst() {
        assertTrue(openBraceComesFirst("{}"))
        assertTrue(openBraceComesFirst(" \t{\"foo\":\"bar\"}"))
        assertTrue(openBraceComesFirst(" { \"foo\": \"bar\" }"))
        assertFalse(openBraceComesFirst("/my/file/path"))
        assertFalse(openBraceComesFirst("mypath"))
        assertFalse(openBraceComesFirst(" blah{}"))
    }

    @Test
    @Throws(Exception::class)
    fun testObjectFromCommandLineArgument() {
        assertEquals(
            expected = 123,
            actual = objectFromCommandLineArgument(
                argument = """{"bar":123}""",
                clazz = Foo::class.java,
            ).bar,
        )
        assertEquals(
            expected = 1,
            actual = objectFromCommandLineArgument(
                argument = """   {"bar": 1}   """,
                clazz = Foo::class.java,
            ).bar,
        )
        val tempFile = tempFile()
        try {
            Files.write(tempFile.toPath(), """{"bar": 456}""".toByteArray())
            assertEquals(
                expected = 456,
                actual = objectFromCommandLineArgument(
                    argument = tempFile.absolutePath,
                    clazz = Foo::class.java,
                ).bar,
            )
        } finally {
            Files.delete(tempFile.toPath())
        }
    }

    internal class Foo @JsonCreator constructor(
        @field:JsonProperty @param:JsonProperty("bar") val bar: Int,
    )
}
