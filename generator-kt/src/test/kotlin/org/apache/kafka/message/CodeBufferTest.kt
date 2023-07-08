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

import org.junit.Assert.assertThrows
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class CodeBufferTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    @Throws(Exception::class)
    fun testWrite() {
        val buffer = CodeBuffer()
        buffer.printf("fun main() {%n")
        buffer.incrementIndent()
        buffer.printf("println(\"%s\")%n", "hello world")
        buffer.decrementIndent()
        buffer.printf("}%n")
        val stringWriter = StringWriter()
        buffer.write(stringWriter)
        assertEquals(
            expected = stringWriter.toString(),
            actual = String.format("fun main() {%n") +
                    String.format("    println(\"hello world\")%n") +
                    String.format("}%n")
        )
    }

    @Test
    fun testEquals() {
        val buffer1 = CodeBuffer()
        val buffer2 = CodeBuffer()
        assertEquals(
            expected = buffer1,
            actual = buffer2,
        )
        buffer1.printf("hello world")
        assertNotEquals(
            illegal = buffer1,
            actual = buffer2,
        )
        buffer2.printf("hello world")
        assertEquals(
            expected = buffer1,
            actual = buffer2,
        )
        buffer1.printf("foo, bar, and baz")
        buffer2.printf("foo, bar, and baz")
        assertEquals(
            expected = buffer1,
            actual = buffer2,
        )
    }

    @Test
    fun testIndentMustBeNonNegative() {
        val buffer = CodeBuffer()
        buffer.incrementIndent()
        buffer.decrementIndent()
        val e: RuntimeException = assertThrows(RuntimeException::class.java) {
            buffer.decrementIndent()
        }
        assertTrue(e.message?.contains("Indent < 0") == true)
    }
}
