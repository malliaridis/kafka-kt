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

package org.apache.kafka.server.utils

import java.util.Properties
import joptsimple.OptionParser
import joptsimple.OptionSpec
import org.apache.kafka.server.util.CommandLineUtils.maybeMergeOptions
import org.apache.kafka.server.util.CommandLineUtils.parseKeyValueArgs
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class CommandLineUtilsTest {

    @Test
    fun testParseEmptyArg() {
        val argArray = listOf("my.empty.property=")
        assertFailsWith<IllegalArgumentException> { parseKeyValueArgs(argArray, false) }
    }

    @Test
    fun testParseEmptyArgWithNoDelimiter() {
        val argArray = listOf("my.empty.property")
        assertFailsWith<IllegalArgumentException> { parseKeyValueArgs(argArray, false) }
    }

    @Test
    fun testParseEmptyArgAsValid() {
        val argArray = listOf("my.empty.property=", "my.empty.property1")
        val props = parseKeyValueArgs(argArray)
        assertEquals(
            expected = "",
            actual = props.getProperty("my.empty.property"),
            message = "Value of a key with missing value should be an empty string",
        )
        assertEquals(
            expected = "",
            actual = props.getProperty("my.empty.property1"),
            message = "Value of a key with missing value with no delimiter should be an empty string",
        )
    }

    @Test
    fun testParseSingleArg() {
        val argArray = listOf("my.property=value")
        val props = parseKeyValueArgs(argArray)
        assertEquals(
            expected = "value",
            actual = props.getProperty("my.property"),
            message = "Value of a single property should be 'value'",
        )
    }

    @Test
    fun testParseArgs() {
        val argArray = listOf("first.property=first", "second.property=second")
        val props = parseKeyValueArgs(argArray)
        assertEquals(
            expected = "first",
            actual = props.getProperty("first.property"),
            message = "Value of first property should be 'first'",
        )
        assertEquals(
            expected = "second",
            actual = props.getProperty("second.property"),
            message = "Value of second property should be 'second'",
        )
    }

    @Test
    fun testParseArgsWithMultipleDelimiters() {
        val argArray = listOf("first.property==first", "second.property=second=", "third.property=thi=rd")
        val props = parseKeyValueArgs(argArray)
        assertEquals(
            expected = "=first",
            actual = props.getProperty("first.property"),
            message = "Value of first property should be '=first'",
        )
        assertEquals(
            expected = "second=",
            actual = props.getProperty("second.property"),
            message = "Value of second property should be 'second='",
        )
        assertEquals(
            expected = "thi=rd",
            actual = props.getProperty("third.property"),
            message = "Value of second property should be 'thi=rd'",
        )
    }

    var props = Properties()

    var parser = OptionParser(false)

    var stringOpt: OptionSpec<String>? = null

    var intOpt: OptionSpec<Int>? = null

    var stringOptOptionalArg: OptionSpec<String>? = null

    var intOptOptionalArg: OptionSpec<Int>? = null

    var stringOptOptionalArgNoDefault: OptionSpec<String>? = null

    var intOptOptionalArgNoDefault: OptionSpec<Int>? = null

    private fun setUpOptions() {
        stringOpt = parser.accepts("str")
            .withRequiredArg()
            .ofType(String::class.java)
            .defaultsTo("default-string")
        intOpt = parser.accepts("int")
            .withRequiredArg()
            .ofType(Int::class.java)
            .defaultsTo(100)
        stringOptOptionalArg = parser.accepts("str-opt")
            .withOptionalArg()
            .ofType(String::class.java)
            .defaultsTo("default-string-2")
        intOptOptionalArg = parser.accepts("int-opt")
            .withOptionalArg()
            .ofType(Int::class.java)
            .defaultsTo(200)
        stringOptOptionalArgNoDefault = parser.accepts("str-opt-nodef")
            .withOptionalArg()
            .ofType(String::class.java)
        intOptOptionalArgNoDefault = parser.accepts("int-opt-nodef")
            .withOptionalArg()
            .ofType(Int::class.java)
    }

    @Test
    fun testMaybeMergeOptionsOverwriteExisting() {
        setUpOptions()
        props["skey"] = "existing-string"
        props["ikey"] = "300"
        props["sokey"] = "existing-string-2"
        props["iokey"] = "400"
        props["sondkey"] = "existing-string-3"
        props["iondkey"] = "500"
        val options = parser.parse(
            "--str", "some-string",
            "--int", "600",
            "--str-opt", "some-string-2",
            "--int-opt", "700",
            "--str-opt-nodef", "some-string-3",
            "--int-opt-nodef", "800",
        )

        maybeMergeOptions(props, "skey", options, stringOpt)
        maybeMergeOptions(props, "ikey", options, intOpt)
        maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
        maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
        maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
        maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

        assertEquals("some-string", props["skey"])
        assertEquals("600", props["ikey"])
        assertEquals("some-string-2", props["sokey"])
        assertEquals("700", props["iokey"])
        assertEquals("some-string-3", props["sondkey"])
        assertEquals("800", props["iondkey"])
    }

    @Test
    fun testMaybeMergeOptionsDefaultOverwriteExisting() {
        setUpOptions()
        props["sokey"] = "existing-string"
        props["iokey"] = "300"
        props["sondkey"] = "existing-string-2"
        props["iondkey"] = "400"
        val options = parser.parse(
            "--str-opt",
            "--int-opt",
            "--str-opt-nodef",
            "--int-opt-nodef",
        )

        maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
        maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
        maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
        maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

        assertEquals("default-string-2", props["sokey"])
        assertEquals("200", props["iokey"])
        assertNull(props["sondkey"])
        assertNull(props["iondkey"])
    }

    @Test
    fun testMaybeMergeOptionsDefaultValueIfNotExist() {
        setUpOptions()
        val options = parser.parse()
        maybeMergeOptions(props, "skey", options, stringOpt)
        maybeMergeOptions(props, "ikey", options, intOpt)
        maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
        maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
        maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
        maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)
        assertEquals("default-string", props["skey"])
        assertEquals("100", props["ikey"])
        assertEquals("default-string-2", props["sokey"])
        assertEquals("200", props["iokey"])
        assertNull(props["sondkey"])
        assertNull(props["iondkey"])
    }

    @Test
    fun testMaybeMergeOptionsNotOverwriteExisting() {
        setUpOptions()
        props["skey"] = "existing-string"
        props["ikey"] = "300"
        props["sokey"] = "existing-string-2"
        props["iokey"] = "400"
        props["sondkey"] = "existing-string-3"
        props["iondkey"] = "500"

        val options = parser.parse()
        maybeMergeOptions(props, "skey", options, stringOpt)
        maybeMergeOptions(props, "ikey", options, intOpt)
        maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
        maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
        maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
        maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

        assertEquals("existing-string", props["skey"])
        assertEquals("300", props["ikey"])
        assertEquals("existing-string-2", props["sokey"])
        assertEquals("400", props["iokey"])
        assertEquals("existing-string-3", props["sondkey"])
        assertEquals("500", props["iondkey"])
    }
}
