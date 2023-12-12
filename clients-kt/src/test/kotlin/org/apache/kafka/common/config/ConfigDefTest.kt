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

package org.apache.kafka.common.config

import org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.ListSize
import org.apache.kafka.common.config.ConfigDef.NonEmptyStringWithoutControlChars
import org.apache.kafka.common.config.ConfigDef.NonNullValidator
import org.apache.kafka.common.config.ConfigDef.Recommender
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigDef.ValidString
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigDef.Width
import org.apache.kafka.common.config.SslConfigs.addClientSslSupport
import org.apache.kafka.common.config.types.Password
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.LinkedList
import java.util.Properties
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class ConfigDefTest {

    @Test
    fun testBasicTypes() {
        val def = ConfigDef().define(
            name = "a",
            type = Type.INT,
            defaultValue = 5,
            validator = ConfigDef.Range.between(0, 14),
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "b",
            type = Type.LONG,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "c",
            type = Type.STRING,
            defaultValue = "hello",
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "d",
            type = Type.LIST,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "e",
            type = Type.DOUBLE,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "f",
            type = Type.CLASS,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "g",
            type = Type.BOOLEAN,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "h",
            type = Type.BOOLEAN,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "i",
            type = Type.BOOLEAN,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "j",
            type = Type.PASSWORD,
            importance = Importance.HIGH,
            documentation = "docs",
        )

        val props = Properties()
        props["a"] = "1   "
        props["b"] = 2
        props["d"] = " a , b, c"
        props["e"] = 42.5
        props["f"] = String::class.java.getName()
        props["g"] = "true"
        props["h"] = "FalSE"
        props["i"] = "TRUE"
        props["j"] = "password"
        val vals = def.parse(props)
        assertEquals(1, vals["a"])
        assertEquals(2L, vals["b"])
        assertEquals("hello", vals["c"])
        assertEquals(mutableListOf("a", "b", "c"), vals["d"])
        assertEquals(42.5, vals["e"])
        assertEquals(String::class.java, vals["f"])
        assertEquals(true, vals["g"])
        assertEquals(false, vals["h"])
        assertEquals(true, vals["i"])
        assertEquals(Password("password"), vals["j"])
        assertEquals(Password.HIDDEN, vals["j"].toString())
    }

    @Test
    fun testInvalidDefault() {
        assertFailsWith<ConfigException> {
            ConfigDef().define(
                name = "a",
                type = Type.INT,
                defaultValue = "hello",
                importance = Importance.HIGH,
                documentation = "docs",
            )
        }
    }

    @Test
    fun testNullDefault() {
        val def = ConfigDef().define(
            name = "a",
            type = Type.INT,
            defaultValue = null,
            validator = null,
            importance = Importance.LOW,
            documentation = "docs",
        )
        val vals = def.parse(Properties())
        assertNull(vals["a"])
    }

    @Test
    fun testMissingRequired() {
        assertFailsWith<ConfigException> {
            ConfigDef().define(
                name = "a",
                type = Type.INT,
                importance = Importance.HIGH,
                documentation = "docs",
            ).parse(HashMap<String, Any>())
        }
    }

    @Test
    fun testParsingEmptyDefaultValueForStringFieldShouldSucceed() {
        ConfigDef().define(
            name = "a",
            type = Type.STRING,
            defaultValue = "",
            importance = Importance.HIGH,
            documentation = "docs",
        ).parse(HashMap<String, Any>())
    }

    @Test
    fun testDefinedTwice() {
        assertFailsWith<ConfigException> {
            ConfigDef().define(
                name = "a",
                type = Type.STRING,
                importance = Importance.HIGH,
                documentation = "docs",
            ).define(
                name = "a",
                type = Type.INT,
                importance = Importance.HIGH,
                documentation = "docs",
            )
        }
    }

    @Test
    fun testBadInputs() {
        testBadInputs(Type.INT, "hello", "42.5", 42.5, Long.MAX_VALUE, Long.MAX_VALUE.toString(), Any())
        testBadInputs(Type.LONG, "hello", "42.5", Long.MAX_VALUE.toString() + "00", Any())
        testBadInputs(Type.DOUBLE, "hello", Any())
        testBadInputs(Type.STRING, Any())
        testBadInputs(Type.LIST, 53, Any())
        testBadInputs(Type.BOOLEAN, "hello", "truee", "fals")
        testBadInputs(Type.CLASS, "ClassDoesNotExist")
    }

    private fun testBadInputs(type: Type, vararg values: Any) {
        for (value in values) {
            val m = mutableMapOf<String?, Any?>()
            m["name"] = value
            val def = ConfigDef().define(
                name = "name",
                type = type,
                importance = Importance.HIGH,
                documentation = "docs"
            )
            try {
                def.parse(m)
                fail("Expected a config exception on bad input for value $value")
            } catch (_: ConfigException) {
                // this is good
            }
        }
    }

    @Test
    fun testInvalidDefaultRange() {
        assertFailsWith<ConfigException> {
            ConfigDef().define(
                name = "name",
                type = Type.INT,
                defaultValue = -1,
                validator = ConfigDef.Range.between(0, 10),
                importance = Importance.HIGH,
                documentation = "docs"
            )
        }
    }

    @Test
    fun testInvalidDefaultString() {
        assertFailsWith<ConfigException> {
            ConfigDef().define(
                name = "name",
                type = Type.STRING,
                defaultValue = "bad",
                validator = ValidString.`in`("valid", "values"),
                importance = Importance.HIGH,
                documentation = "docs",
            )
        }
    }

    @Test
    fun testNestedClass() {
        // getName(), not getSimpleName() or getCanonicalName(), is the version that should be able to locate the class
        val props = mapOf("name" to NestedClass::class.java.getName())
        ConfigDef().define(
            name = "name",
            type = Type.CLASS,
            importance = Importance.HIGH,
            documentation = "docs",
        ).parse(props)
    }

    @Test
    fun testValidators() {
        testValidators(Type.INT, ConfigDef.Range.between(0, 10), 5, arrayOf(1, 5, 9), arrayOf(-1, 11, null))
        testValidators(
            type = Type.STRING,
            validator = ValidString.`in`("good", "values", "default"),
            defaultVal = "default",
            okValues = arrayOf("good", "values", "default"),
            badValues = arrayOf("bad", "inputs", "DEFAULT", null),
        )
        testValidators(
            type = Type.STRING,
            validator = CaseInsensitiveValidString.`in`("good", "values", "default"),
            defaultVal = "default",
            okValues = arrayOf("gOOd", "VALUES", "default"),
            badValues = arrayOf("Bad", "iNPUts", null),
        )
        testValidators(
            type = Type.LIST,
            validator = ConfigDef.ValidList.`in`("1", "2", "3"),
            defaultVal = "1",
            okValues = arrayOf("1", "2", "3"),
            badValues = arrayOf("4", "5", "6"),
        )
        testValidators(
            type = Type.STRING,
            validator = NonNullValidator(),
            defaultVal = "a",
            okValues = arrayOf("abb"),
            badValues = arrayOf(null),
        )
        testValidators(
            type = Type.STRING,
            validator = ConfigDef.CompositeValidator.of(NonNullValidator(), ValidString.`in`("a", "b")),
            defaultVal = "a",
            okValues = arrayOf("a", "b"),
            badValues = arrayOf(null, -1, "c")
        )
        testValidators(
            type = Type.STRING,
            validator = NonEmptyStringWithoutControlChars(),
            defaultVal = "defaultname",
            okValues = arrayOf(
                "test",
                "name",
                "test/test",
                "test\u1234",
                "\u1324name\\",
                "/+%>&):??<&()?-",
                "+1",
                "\uD83D\uDE01",
                "\uF3B1",
                "     test   \n\r",
                "\n  hello \t",
            ),
            badValues = arrayOf(
                "nontrailing\nnotallowed",
                "as\u0001cii control char",
                "tes\rt",
                "test\btest",
                "1\t2",
                ""
            ),
        )
    }

    @Test
    fun testSslPasswords() {
        val def = ConfigDef()
        addClientSslSupport(def)
        val props = Properties()
        props[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = "key_password"
        props[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = "keystore_password"
        props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = "truststore_password"
        val vals = def.parse(props)
        assertEquals(Password("key_password"), vals[SslConfigs.SSL_KEY_PASSWORD_CONFIG])
        assertEquals(Password.HIDDEN, vals[SslConfigs.SSL_KEY_PASSWORD_CONFIG].toString())
        assertEquals(Password("keystore_password"), vals[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG])
        assertEquals(Password.HIDDEN, vals[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG].toString())
        assertEquals(Password("truststore_password"), vals[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG])
        assertEquals(Password.HIDDEN, vals[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG].toString())
    }

    @Test
    fun testNullDefaultWithValidator() {
        val key = "enum_test"
        val def = ConfigDef()
        def.define(
            name = key,
            type = Type.STRING,
            defaultValue = ConfigDef.NO_DEFAULT_VALUE,
            validator = ValidString.`in`("ONE", "TWO", "THREE"),
            importance = Importance.HIGH,
            documentation = "docs",
        )
        val props = Properties()
        props[key] = "ONE"
        val vals = def.parse(props)
        assertEquals("ONE", vals[key])
    }

    @Test
    fun testGroupInference() {
        val expected1 = listOf("group1", "group2")
        val def1 = ConfigDef().define(
            name = "a",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group1",
            orderInGroup = 1,
            width = Width.SHORT,
            displayName = "a",
        ).define(
            name = "b",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group2",
            orderInGroup = 1,
            width = Width.SHORT,
            displayName = "b",
        ).define(
            name = "c",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group1",
            orderInGroup = 2,
            width = Width.SHORT,
            displayName = "c",
        )
        assertContentEquals(expected1, def1.groups)
        val expected2 = listOf("group2", "group1")
        val def2 = ConfigDef().define(
            name = "a",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group2",
            orderInGroup = 1,
            width = Width.SHORT,
            displayName = "a"
        ).define(
            name = "b",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group2",
            orderInGroup = 2,
            width = Width.SHORT,
            displayName = "b"
        ).define(
            name = "c",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group1",
            orderInGroup = 2,
            width = Width.SHORT,
            displayName = "c"
        )
        assertContentEquals(expected2, def2.groups)
    }

    @Test
    fun testParseForValidate() {
        val expectedParsed = mutableMapOf<String, Any?>()
        expectedParsed["a"] = 1
        expectedParsed["b"] = null
        expectedParsed["c"] = null
        expectedParsed["d"] = 10
        val expected = mutableMapOf<String, ConfigValue>()
        val errorMessageB = "Missing required configuration \"b\" which has no default value."
        val errorMessageC = "Missing required configuration \"c\" which has no default value."
        val configA = ConfigValue(
            name = "a",
            value = 1,
        )
        val configB = ConfigValue(
            name = "b",
            errorMessages = mutableListOf(errorMessageB, errorMessageB),
        )
        val configC = ConfigValue(
            name = "c",
            errorMessages = mutableListOf(errorMessageC),
        )
        val configD = ConfigValue(
            name = "d",
            value = 10,
        )
        expected["a"] = configA
        expected["b"] = configB
        expected["c"] = configC
        expected["d"] = configD
        val def = ConfigDef().define(
            name = "a",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 1,
            width = Width.SHORT,
            displayName = "a",
            dependents = listOf("b", "c"),
            recommender = IntegerRecommender(false),
        )
            .define(
                name = "b",
                type = Type.INT,
                importance = Importance.HIGH,
                documentation = "docs",
                group = "group",
                orderInGroup = 2,
                width = Width.SHORT,
                displayName = "b",
                recommender = IntegerRecommender(true),
            )
            .define(
                name = "c",
                type = Type.INT,
                importance = Importance.HIGH,
                documentation = "docs",
                group = "group",
                orderInGroup = 3,
                width = Width.SHORT,
                displayName = "c",
                recommender = IntegerRecommender(true),
            )
            .define(
                name = "d",
                type = Type.INT,
                importance = Importance.HIGH,
                documentation = "docs",
                group = "group",
                orderInGroup = 4,
                width = Width.SHORT,
                displayName = "d",
                dependents = listOf("b"),
                recommender = IntegerRecommender(false),
            )
        val props: MutableMap<String, String?> = HashMap()
        props["a"] = "1"
        props["d"] = "10"
        val configValues = mutableMapOf<String, ConfigValue>()
        for (name in def.configKeys.keys) {
            configValues[name] = ConfigValue(name)
        }
        val parsed = def.parseForValidate(props, configValues)
        assertEquals(expectedParsed, parsed)
        assertEquals(expected, configValues)
    }

    @Test
    fun testValidate() {
        val expected = mutableMapOf<String, ConfigValue>()
        val errorMessageB = "Missing required configuration \"b\" which has no default value."
        val errorMessageC = "Missing required configuration \"c\" which has no default value."
        val configA = ConfigValue(
            name = "a",
            value = 1,
            recommendedValues = listOf(1, 2, 3),
        )
        val configB = ConfigValue(
            name = "b",
            recommendedValues = listOf(4, 5),
            errorMessages = listOf(errorMessageB, errorMessageB)
        )
        val configC = ConfigValue(
            name = "c",
            recommendedValues = listOf(4, 5),
            errorMessages = listOf(errorMessageC)
        )
        val configD = ConfigValue(
            name = "d",
            value = 10,
            recommendedValues = listOf(1, 2, 3),
        )
        expected["a"] = configA
        expected["b"] = configB
        expected["c"] = configC
        expected["d"] = configD
        val def = ConfigDef().define(
            name = "a",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 1,
            width = Width.SHORT,
            displayName = "a",
            dependents = listOf("b", "c"),
            recommender = IntegerRecommender(false),
        ).define(
            name = "b",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 2,
            width = Width.SHORT,
            displayName = "b",
            recommender = IntegerRecommender(true),
        ).define(
            name = "c",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 3,
            width = Width.SHORT,
            displayName = "c",
            recommender = IntegerRecommender(true),
        ).define(
            name = "d",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 4,
            width = Width.SHORT,
            displayName = "d",
            dependents = listOf("b"),
            recommender = IntegerRecommender(false),
        )
        val props: MutableMap<String, String?> = HashMap()
        props["a"] = "1"
        props["d"] = "10"
        val configs = def.validate(props)
        for (config in configs) {
            val name = config!!.name
            val expectedConfig = expected[name]
            assertEquals(expectedConfig, config)
        }
    }

    @Test
    fun testValidateMissingConfigKey() {
        val expected = mutableMapOf<String, ConfigValue>()
        val errorMessageB = "Missing required configuration \"b\" which has no default value."
        val errorMessageC = "Missing required configuration \"c\" which has no default value."
        val errorMessageD = "d is referred in the dependents, but not defined."
        val configA = ConfigValue(
            name = "a",
            value = 1,
            recommendedValues = listOf(1, 2, 3),
        )
        val configB = ConfigValue(
            name = "b",
            recommendedValues = listOf(4, 5),
            errorMessages = listOf(errorMessageB),
        )
        val configC = ConfigValue(
            name = "c",
            recommendedValues = listOf(4, 5),
            errorMessages = listOf(errorMessageC),
        )
        val configD = ConfigValue(
            name = "d",
            errorMessages = listOf(errorMessageD),
        )
        configD.visible = false
        expected["a"] = configA
        expected["b"] = configB
        expected["c"] = configC
        expected["d"] = configD
        val def = ConfigDef().define(
            name = "a",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 1,
            width = Width.SHORT,
            displayName = "a",
            dependents = listOf("b", "c", "d"),
            recommender = IntegerRecommender(false),
        ).define(
            name = "b",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 2,
            width = Width.SHORT,
            displayName = "b",
            recommender = IntegerRecommender(true)
        ).define(
            name = "c",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
            group = "group",
            orderInGroup = 3,
            width = Width.SHORT,
            displayName = "c",
            recommender = IntegerRecommender(true)
        )
        val props = mutableMapOf<String, String?>()
        props["a"] = "1"
        val configs = def.validate(props)
        for (config in configs) {
            val name = config!!.name
            val expectedConfig = expected[name]
            assertEquals(expectedConfig, config)
        }
    }

    @Test
    fun testValidateCannotParse() {
        val expected = mutableMapOf<String, ConfigValue>()
        val errorMessageB = "Invalid value non_integer for configuration a: Not a number of type INT"
        val configA = ConfigValue(
            name = "a",
            errorMessages = listOf(errorMessageB),
        )
        expected["a"] = configA
        val def = ConfigDef().define(
            name = "a",
            type = Type.INT,
            importance = Importance.HIGH,
            documentation = "docs",
        )
        val props = mutableMapOf<String, String?>()
        props["a"] = "non_integer"
        val configs = def.validate(props)
        for (config in configs) {
            val name = config!!.name
            val expectedConfig = expected[name]
            assertEquals(expectedConfig, config)
        }
    }

    @Test
    fun testCanAddInternalConfig() {
        val configName = "internal.config"
        val configDef = ConfigDef().defineInternal(
            name = configName,
            type = Type.STRING,
            defaultValue = "",
            importance = Importance.LOW,
        )
        val properties = HashMap<String, String?>()
        properties[configName] = "value"
        val results = configDef.validate(properties)
        val configValue = results[0]
        assertEquals("value", configValue!!.value)
        assertEquals(configName, configValue.name)
    }

    @Test
    fun testInternalConfigDoesntShowUpInDocs() {
        val name = "my.config"
        val configDef = ConfigDef().defineInternal(
            name = name,
            type = Type.STRING,
            defaultValue = "",
            importance = Importance.LOW,
        )
        configDef.defineInternal(
            name = "my.other.config",
            type = Type.STRING,
            defaultValue = "",
            validator = null,
            importance = Importance.LOW,
            documentation = null,
        )
        assertFalse(configDef.toHtmlTable().contains("my.config"))
        assertFalse(configDef.toEnrichedRst().contains("my.config"))
        assertFalse(configDef.toRst().contains("my.config"))
        assertFalse(configDef.toHtmlTable().contains("my.other.config"))
        assertFalse(configDef.toEnrichedRst().contains("my.other.config"))
        assertFalse(configDef.toRst().contains("my.other.config"))
    }

    @Test
    fun testDynamicUpdateModeInDocs() {
        val configDef = ConfigDef().define(
            name = "my.broker.config",
            type = Type.LONG,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "my.cluster.config",
            type = Type.LONG,
            importance = Importance.HIGH,
            documentation = "docs",
        ).define(
            name = "my.readonly.config",
            type = Type.LONG,
            importance = Importance.HIGH,
            documentation = "docs",
        )
        val updateModes = mutableMapOf<String?, String?>()
        updateModes["my.broker.config"] = "per-broker"
        updateModes["my.cluster.config"] = "cluster-wide"
        val html = configDef.toHtmlTable(updateModes)
        val configsInHtml: MutableSet<String> = HashSet()
        for (line in html.split("\n".toRegex()).dropLastWhile { it.isEmpty() }) {
            if (line.contains("my.broker.config")) {
                assertTrue(line.contains("per-broker"))
                configsInHtml.add("my.broker.config")
            } else if (line.contains("my.cluster.config")) {
                assertTrue(line.contains("cluster-wide"))
                configsInHtml.add("my.cluster.config")
            } else if (line.contains("my.readonly.config")) {
                assertTrue(line.contains("read-only"))
                configsInHtml.add("my.readonly.config")
            }
        }
        assertEquals(configDef.names(), configsInHtml)
    }

    @Test
    fun testNames() {
        val configDef = ConfigDef().define(
            name = "a",
            type = Type.STRING,
            importance = Importance.LOW,
            documentation = "docs",
        ).define(
            name = "b",
            type = Type.STRING,
            importance = Importance.LOW,
            documentation = "docs",
        )
        val names = configDef.names()
        assertEquals(setOf("a", "b"), names)
        // should be unmodifiable
        // Kotlin Migration: Returned set is by default immutable due to Kotlin
//        try {
//            names.add("new")
//            fail()
//        } catch (e: UnsupportedOperationException) {
//            // expected
//        }
    }

    @Test
    fun testMissingDependentConfigs() {
        // Should not be possible to parse a config if a dependent config has not been defined
        val configDef = ConfigDef().define(
            name = "parent",
            type = Type.STRING,
            importance = Importance.HIGH,
            documentation = "parent docs",
            group = "group",
            orderInGroup = 1,
            width = Width.LONG,
            displayName = "Parent",
            dependents = listOf("child"),
        )
        assertFailsWith<ConfigException> { configDef.parse(emptyMap<Any?, Any>()) }
    }

    @Test
    fun testBaseConfigDefDependents() {
        // Creating a ConfigDef based on another should compute the correct number of configs with no parent, even
        // if the base ConfigDef has already computed its parentless configs
        val baseConfigDef = ConfigDef().define(
            name = "a",
            type = Type.STRING,
            importance = Importance.LOW,
            documentation = "docs",
        )
        assertEquals(setOf("a"), baseConfigDef.getConfigsWithNoParent())
        val configDef = ConfigDef(baseConfigDef).define(
            name = "parent",
            type = Type.STRING,
            importance = Importance.HIGH,
            documentation = "parent docs",
            group = "group",
            orderInGroup = 1,
            width = Width.LONG,
            displayName = "Parent",
            dependents = listOf("child"),
        ).define(
            name = "child",
            type = Type.STRING,
            importance = Importance.HIGH,
            documentation = "docs",
        )
        assertEquals(setOf("a", "parent"), configDef.getConfigsWithNoParent())
    }

    private class IntegerRecommender(private val hasParent: Boolean) : Recommender {

        override fun validValues(name: String, parsedConfig: Map<String, Any?>): List<Any> {
            val values: MutableList<Any> = LinkedList()
            if (!hasParent) values.addAll(arrayOf(1, 2, 3))
            else values.addAll(arrayOf(4, 5))

            return values
        }

        override fun visible(name: String, parsedConfig: Map<String, Any?>): Boolean = true
    }

    private fun testValidators(
        type: Type,
        validator: Validator,
        defaultVal: Any,
        okValues: Array<Any>,
        badValues: Array<Any?>,
    ) {
        val def = ConfigDef().define(
            name = "name",
            type = type,
            defaultValue = defaultVal,
            validator = validator,
            importance = Importance.HIGH,
            documentation = "docs",
        )
        for (value in okValues) {
            val m = mutableMapOf<String?, Any?>()
            m["name"] = value
            def.parse(m)
        }
        for (value in badValues) {
            val m = mutableMapOf<String?, Any?>()
            m["name"] = value
            try {
                def.parse(m)
                fail("Expected a config exception due to invalid value $value")
            } catch (_: ConfigException) {
                // this is good
            }
        }
    }

    @Test
    fun toRst() {
        val def = ConfigDef().define(
            name = "opt1",
            type = Type.STRING,
            defaultValue = "a",
            validator = ValidString.`in`("a", "b", "c"),
            importance = Importance.HIGH,
            documentation = "docs1",
        ).define(
            name = "opt2",
            type = Type.INT,
            importance = Importance.MEDIUM,
            documentation = "docs2",
        ).define(
            name = "opt3",
            type = Type.LIST,
            defaultValue = listOf("a", "b"),
            importance = Importance.LOW,
            documentation = "docs3",
        ).define(
            name = "opt4",
            type = Type.BOOLEAN,
            defaultValue = false,
            importance = Importance.LOW,
            documentation = null,
        )
        val expectedRst = """``opt2``
  docs2

  * Type: int
  * Importance: medium

``opt1``
  docs1

  * Type: string
  * Default: a
  * Valid Values: [a, b, c]
  * Importance: high

``opt3``
  docs3

  * Type: list
  * Default: a,b
  * Importance: low

``opt4``

  * Type: boolean
  * Default: false
  * Importance: low

"""
        assertEquals(expectedRst, def.toRst())
    }

    @Test
    fun toEnrichedRst() {
        val def = ConfigDef().define(
            name = "opt1.of.group1",
            type = Type.STRING,
            defaultValue = "a",
            validator = ValidString.`in`("a", "b", "c"),
            importance = Importance.HIGH,
            documentation = "Doc doc.",
            group = "Group One",
            orderInGroup = 0,
            width = Width.NONE,
            displayName = "..",
        ).define(
            name = "opt2.of.group1",
            type = Type.INT,
            defaultValue = ConfigDef.NO_DEFAULT_VALUE,
            importance = Importance.MEDIUM,
            documentation = "Doc doc doc.",
            group = "Group One",
            orderInGroup = 1,
            width = Width.NONE,
            displayName = "..",
            dependents = listOf("some.option1", "some.option2"),
        ).define(
            name = "opt2.of.group2",
            type = Type.BOOLEAN,
            defaultValue = false,
            importance = Importance.HIGH,
            documentation = "Doc doc doc doc.",
            group = "Group Two",
            orderInGroup = 1,
            width = Width.NONE,
            displayName = "..",
        ).define(
            name = "opt1.of.group2",
            type = Type.BOOLEAN,
            defaultValue = false,
            importance = Importance.HIGH,
            documentation = "Doc doc doc doc doc.",
            group = "Group Two",
            orderInGroup = 0,
            width = Width.NONE,
            displayName = "..",
            dependents = listOf("some.option"),
        ).define(
            name = "poor.opt",
            type = Type.STRING,
            defaultValue = "foo",
            importance = Importance.HIGH,
            documentation = "Doc doc doc doc.",
        )
        val expectedRst = """``poor.opt``
  Doc doc doc doc.

  * Type: string
  * Default: foo
  * Importance: high

Group One
^^^^^^^^^

``opt1.of.group1``
  Doc doc.

  * Type: string
  * Default: a
  * Valid Values: [a, b, c]
  * Importance: high

``opt2.of.group1``
  Doc doc doc.

  * Type: int
  * Importance: medium
  * Dependents: ``some.option1``, ``some.option2``

Group Two
^^^^^^^^^

``opt1.of.group2``
  Doc doc doc doc doc.

  * Type: boolean
  * Default: false
  * Importance: high
  * Dependents: ``some.option``

``opt2.of.group2``
  Doc doc doc doc.

  * Type: boolean
  * Default: false
  * Importance: high

"""
        assertEquals(expectedRst, def.toEnrichedRst())
    }

    @Test
    fun testConvertValueToStringBoolean() {
        assertEquals("true", ConfigDef.convertToString(true, Type.BOOLEAN))
        assertNull(ConfigDef.convertToString(null, Type.BOOLEAN))
    }

    @Test
    fun testConvertValueToStringShort() {
        assertEquals("32767", ConfigDef.convertToString(Short.MAX_VALUE, Type.SHORT))
        assertNull(ConfigDef.convertToString(null, Type.SHORT))
    }

    @Test
    fun testConvertValueToStringInt() {
        assertEquals("2147483647", ConfigDef.convertToString(Int.MAX_VALUE, Type.INT))
        assertNull(ConfigDef.convertToString(null, Type.INT))
    }

    @Test
    fun testConvertValueToStringLong() {
        assertEquals("9223372036854775807", ConfigDef.convertToString(Long.MAX_VALUE, Type.LONG))
        assertNull(ConfigDef.convertToString(null, Type.LONG))
    }

    @Test
    fun testConvertValueToStringDouble() {
        assertEquals("3.125", ConfigDef.convertToString(3.125, Type.DOUBLE))
        assertNull(ConfigDef.convertToString(null, Type.DOUBLE))
    }

    @Test
    fun testConvertValueToStringString() {
        assertEquals("foobar", ConfigDef.convertToString("foobar", Type.STRING))
        assertNull(ConfigDef.convertToString(null, Type.STRING))
    }

    @Test
    fun testConvertValueToStringPassword() {
        assertEquals(Password.HIDDEN, ConfigDef.convertToString(Password("foobar"), Type.PASSWORD))
        assertEquals("foobar", ConfigDef.convertToString("foobar", Type.PASSWORD))
        assertNull(ConfigDef.convertToString(null, Type.PASSWORD))
    }

    @Test
    fun testConvertValueToStringList() {
        assertEquals("a,bc,d", ConfigDef.convertToString(mutableListOf("a", "bc", "d"), Type.LIST))
        assertNull(ConfigDef.convertToString(null, Type.LIST))
    }

    @Test
    @Throws(ClassNotFoundException::class)
    fun testConvertValueToStringClass() {
        val actual = ConfigDef.convertToString(ConfigDefTest::class.java, Type.CLASS)
        assertEquals("org.apache.kafka.common.config.ConfigDefTest", actual)
        // Additionally validate that we can look up this class by this name
        assertEquals(ConfigDefTest::class.java, Class.forName(actual))
        assertNull(ConfigDef.convertToString(null, Type.CLASS))
    }

    @Test
    @Throws(ClassNotFoundException::class)
    fun testConvertValueToStringNestedClass() {
        val actual = ConfigDef.convertToString(NestedClass::class.java, Type.CLASS)
        assertEquals("org.apache.kafka.common.config.ConfigDefTest\$NestedClass", actual)
        // Additionally validate that we can look up this class by this name
        assertEquals(NestedClass::class.java, Class.forName(actual))
    }

    @Test
    fun testClassWithAlias() {
        val alias = "PluginAlias"
        val originalClassLoader = Thread.currentThread().getContextClassLoader()
        try {
            // Could try to use the Plugins class from Connect here, but this should simulate enough
            // of the aliasing logic to suffice for this test.
            Thread.currentThread().setContextClassLoader(object : ClassLoader(originalClassLoader) {
                @Throws(ClassNotFoundException::class)
                public override fun loadClass(name: String, resolve: Boolean): Class<*> {
                    return if (alias == name) {
                        NestedClass::class.java
                    } else {
                        super.loadClass(name, resolve)
                    }
                }
            })
            ConfigDef.parseType("Test config", alias, Type.CLASS)
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader)
        }
    }

    private inner class NestedClass

    @Test
    fun testNiceMemoryUnits() {
        assertEquals("", ConfigDef.niceMemoryUnits(0L))
        assertEquals("", ConfigDef.niceMemoryUnits(1023))
        assertEquals(" (1 kibibyte)", ConfigDef.niceMemoryUnits(1024))
        assertEquals("", ConfigDef.niceMemoryUnits(1025))
        assertEquals(" (2 kibibytes)", ConfigDef.niceMemoryUnits(2 * 1024))
        assertEquals(" (1 mebibyte)", ConfigDef.niceMemoryUnits(1024 * 1024))
        assertEquals(" (2 mebibytes)", ConfigDef.niceMemoryUnits(2 * 1024 * 1024))
        assertEquals(" (1 gibibyte)", ConfigDef.niceMemoryUnits(1024 * 1024 * 1024))
        assertEquals(" (2 gibibytes)", ConfigDef.niceMemoryUnits(2L * 1024 * 1024 * 1024))
        assertEquals(" (1 tebibyte)", ConfigDef.niceMemoryUnits(1024L * 1024 * 1024 * 1024))
        assertEquals(" (2 tebibytes)", ConfigDef.niceMemoryUnits(2L * 1024 * 1024 * 1024 * 1024))
        assertEquals(" (1024 tebibytes)", ConfigDef.niceMemoryUnits(1024L * 1024 * 1024 * 1024 * 1024))
        assertEquals(" (2048 tebibytes)", ConfigDef.niceMemoryUnits(2L * 1024 * 1024 * 1024 * 1024 * 1024))
    }

    @Test
    fun testNiceTimeUnits() {
        assertEquals("", ConfigDef.niceTimeUnits(0))
        assertEquals("", ConfigDef.niceTimeUnits(Duration.ofSeconds(1).toMillis() - 1))
        assertEquals(" (1 second)", ConfigDef.niceTimeUnits(Duration.ofSeconds(1).toMillis()))
        assertEquals("", ConfigDef.niceTimeUnits(Duration.ofSeconds(1).toMillis() + 1))
        assertEquals(" (2 seconds)", ConfigDef.niceTimeUnits(Duration.ofSeconds(2).toMillis()))
        assertEquals(" (1 minute)", ConfigDef.niceTimeUnits(Duration.ofMinutes(1).toMillis()))
        assertEquals(" (2 minutes)", ConfigDef.niceTimeUnits(Duration.ofMinutes(2).toMillis()))
        assertEquals(" (1 hour)", ConfigDef.niceTimeUnits(Duration.ofHours(1).toMillis()))
        assertEquals(" (2 hours)", ConfigDef.niceTimeUnits(Duration.ofHours(2).toMillis()))
        assertEquals(" (1 day)", ConfigDef.niceTimeUnits(Duration.ofDays(1).toMillis()))
        assertEquals(" (2 days)", ConfigDef.niceTimeUnits(Duration.ofDays(2).toMillis()))
        assertEquals(" (7 days)", ConfigDef.niceTimeUnits(Duration.ofDays(7).toMillis()))
        assertEquals(" (365 days)", ConfigDef.niceTimeUnits(Duration.ofDays(365).toMillis()))
    }

    @Test
    fun testThrowsExceptionWhenListSizeExceedsLimit() {
        val exception = assertFailsWith<ConfigException> {
            ConfigDef().define(
                name = "lst",
                type = Type.LIST,
                defaultValue = mutableListOf("a", "b"),
                validator = ListSize.atMostOfSize(1),
                importance = Importance.HIGH,
                documentation = "lst doc",
            )
        }
        assertEquals(
            "Invalid value [a, b] for configuration lst: exceeds maximum list size of [1].",
            exception.message
        )
    }

    @Test
    fun testNoExceptionIsThrownWhenListSizeEqualsTheLimit() {
        val lst = listOf("a", "b", "c")
        ConfigDef().define(
            "lst",
            Type.LIST,
            lst,
            ListSize.atMostOfSize(lst.size),
            Importance.HIGH,
            "lst doc"
        )
    }

    @Test
    fun testNoExceptionIsThrownWhenListSizeIsBelowTheLimit() {
        ConfigDef().define(
            "lst",
            Type.LIST,
            mutableListOf("a", "b"),
            ListSize.atMostOfSize(3),
            Importance.HIGH,
            "lst doc"
        )
    }

    @Test
    fun testListSizeValidatorToString() {
        assertEquals("List containing maximum of 5 elements", ListSize.atMostOfSize(5).toString())
    }
}
