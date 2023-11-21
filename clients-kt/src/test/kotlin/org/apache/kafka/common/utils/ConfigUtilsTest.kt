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

package org.apache.kafka.common.utils

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.utils.ConfigUtils.configMapToRedactedString
import org.apache.kafka.common.utils.ConfigUtils.translateDeprecatedConfigs
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ConfigUtilsTest {

    @Test
    fun testTranslateDeprecated() {
        val config = mapOf(
            "foo.bar" to "baz",
            "foo.bar.deprecated" to "quux",
            "chicken" to "1",
            "rooster" to "2",
            "hen" to "3",
            "heifer" to "moo",
            "blah" to "blah",
            "unexpected.non.string.object" to 42,
        )

        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(
                arrayOf("foo.bar", "foo.bar.deprecated"),
                arrayOf("chicken", "rooster", "hen"),
                arrayOf("cow", "beef", "heifer", "steer"),
            ),
        )
        assertEquals("baz", newConfig["foo.bar"])
        assertNull(newConfig["foobar.deprecated"])
        assertEquals("1", newConfig["chicken"])
        assertNull(newConfig["rooster"])
        assertNull(newConfig["hen"])
        assertEquals("moo", newConfig["cow"])
        assertNull(newConfig["beef"])
        assertNull(newConfig["heifer"])
        assertNull(newConfig["steer"])
        assertNull(config["cow"])
        assertEquals("blah", config["blah"])
        assertEquals("blah", newConfig["blah"])
        assertEquals(42, newConfig["unexpected.non.string.object"])
        assertEquals(42, config["unexpected.non.string.object"])
    }

    @Test
    fun testAllowsNewKey() {
        val config = mapOf("foo.bar" to "baz")
        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(
                arrayOf("foo.bar", "foo.bar.deprecated"),
                arrayOf("chicken", "rooster", "hen"),
                arrayOf("cow", "beef", "heifer", "steer")
            ),
        )
        assertNotNull(newConfig)
        assertEquals("baz", newConfig["foo.bar"])
        assertNull(newConfig["foo.bar.deprecated"])
    }

    @Test
    fun testAllowDeprecatedNulls() {
        val config = mapOf(
            "foo.bar.deprecated" to null,
            "foo.bar" to "baz",
        )
        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(arrayOf("foo.bar", "foo.bar.deprecated")),
        )
        assertNotNull(newConfig)
        assertEquals("baz", newConfig["foo.bar"])
        assertNull(newConfig["foo.bar.deprecated"])
    }

    @Test
    fun testAllowNullOverride() {
        val config = mapOf(
            "foo.bar.deprecated" to "baz",
            "foo.bar" to null,
        )
        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(arrayOf("foo.bar", "foo.bar.deprecated")),
        )
        assertNotNull(newConfig)
        assertNull(newConfig["foo.bar"])
        assertNull(newConfig["foo.bar.deprecated"])
    }

    @Test
    fun testNullMapEntriesWithoutAliasesDoNotThrowNPE() {
        val config = mapOf("other" to null)
        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(arrayOf("foo.bar", "foo.bar.deprecated")),
        )
        assertNotNull(newConfig)
        assertNull(newConfig["other"])
    }

    @Test
    fun testDuplicateSynonyms() {
        val config = mapOf(
            "foo.bar" to "baz",
            "foo.bar.deprecated" to "derp",
        )
        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(
                arrayOf("foo.bar", "foo.bar.deprecated"),
                arrayOf("chicken", "foo.bar.deprecated"),
            ),
        )
        assertNotNull(newConfig)
        assertEquals("baz", newConfig["foo.bar"])
        assertEquals("derp", newConfig["chicken"])
        assertNull(newConfig["foo.bar.deprecated"])
    }

    @Test
    fun testMultipleDeprecations() {
        val config = mapOf(
            "foo.bar.deprecated" to "derp",
            "foo.bar.even.more.deprecated" to "very old configuration",
        )
        val newConfig = translateDeprecatedConfigs(
            configs = config,
            aliasGroups = arrayOf(arrayOf("foo.bar", "foo.bar.deprecated", "foo.bar.even.more.deprecated")),
        )
        assertNotNull(newConfig)
        assertEquals("derp", newConfig["foo.bar"])
        assertNull(newConfig["foo.bar.deprecated"])
        assertNull(newConfig["foo.bar.even.more.deprecated"])
    }

    @Test
    fun testConfigMapToRedactedStringForEmptyMap() {
        assertEquals("{}", configMapToRedactedString(emptyMap(), CONFIG))
    }

    @Test
    fun testConfigMapToRedactedStringWithSecrets() {
        val testMap1: MutableMap<String, Any?> = HashMap()
        testMap1["myString"] = "whatever"
        testMap1["myInt"] = 123
        testMap1["myPassword"] = "foosecret"
        testMap1["myString2"] = null
        testMap1["myUnknown"] = 456
        assertEquals(
            expected =
            "{myInt=123, myPassword=(redacted), myString=\"whatever\", myString2=null, myUnknown=(redacted)}",
            actual = configMapToRedactedString(testMap1, CONFIG)
        )
    }

    companion object {
        private val CONFIG: ConfigDef = ConfigDef().define(
            name = "myPassword",
            type = ConfigDef.Type.PASSWORD,
            importance = Importance.HIGH,
            documentation = "",
        ).define(
            name = "myString",
            type = ConfigDef.Type.STRING,
            importance = Importance.HIGH,
            documentation = "",
        ).define(
            name = "myInt",
            type = ConfigDef.Type.INT,
            importance = Importance.HIGH,
            documentation = "",
        ).define(
            name = "myString2",
            type = ConfigDef.Type.STRING,
            importance = Importance.HIGH,
            documentation = "",
        )
    }
}
