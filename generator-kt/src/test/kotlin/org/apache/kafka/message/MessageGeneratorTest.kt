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

import org.apache.kafka.message.MessageGenerator.capitalizeFirst
import org.apache.kafka.message.MessageGenerator.firstIsCapitalized
import org.apache.kafka.message.MessageGenerator.lowerCaseFirst
import org.apache.kafka.message.MessageGenerator.stripSuffix
import org.apache.kafka.message.MessageGenerator.toSnakeCase
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class MessageGeneratorTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    @Throws(Exception::class)
    fun testCapitalizeFirst() {
        assertEquals(
            expected = "",
            actual = capitalizeFirst(""),
        )
        assertEquals(
            expected = "AbC",
            actual = capitalizeFirst("abC"),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLowerCaseFirst() {
        assertEquals(
            expected = "",
            actual = lowerCaseFirst(""),
        )
        assertEquals(
            expected = "fORTRAN",
            actual = lowerCaseFirst("FORTRAN"),
        )
        assertEquals(
            expected = "java",
            actual = lowerCaseFirst("java"),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testFirstIsCapitalized() {
        assertFalse(firstIsCapitalized(""))
        assertTrue(firstIsCapitalized("FORTRAN"))
        assertFalse(firstIsCapitalized("java"))
    }

    @Test
    @Throws(Exception::class)
    fun testToSnakeCase() {
        assertEquals(
            expected = "",
            actual = toSnakeCase(""),
        )
        assertEquals(
            expected = "foo_bar_baz",
            actual = toSnakeCase("FooBarBaz"),
        )
        assertEquals(
            expected = "foo_bar_baz",
            actual = toSnakeCase("fooBarBaz"),
        )
        assertEquals(
            expected = "fortran",
            actual = toSnakeCase("FORTRAN"),
        )
    }

    @Test
    @Throws(Exception::class)
    fun stripSuffixTest() {
        assertEquals(
            expected = "FooBa",
            actual = stripSuffix(str = "FooBar", suffix = "r"),
        )
        assertEquals(
            expected = "",
            actual = stripSuffix(str = "FooBar", suffix = "FooBar"),
        )
        assertEquals(
            expected = "Foo",
            actual = stripSuffix(str = "FooBar", suffix = "Bar"),
        )
        try {
            stripSuffix("FooBar", "Baz")
            fail("expected exception")
        } catch (e: RuntimeException) {
        }
    }
}
