package org.apache.kafka.message

import java.util.concurrent.TimeUnit
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import kotlin.test.assertContains
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

class MessageDataGeneratorTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    @Throws(Exception::class)
    fun testNullDefaults() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+" },
                { "name": "field2", "type": "[]TestStruct", "versions": "1+", 
                "nullableVersions": "1+", "default": "null", "fields": [
                  { "name": "field1", "type": "int32", "versions": "0+" }
                ]},
                { "name": "field3", "type": "bytes", "versions": "2+", 
                  "nullableVersions": "2+", "default": "null" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message").generate(testMessageSpec)
    }

    private fun assertStringContains(substring: String, value: String) {
        assertTrue(
            value.contains(substring),
            "Expected string to contain '$substring', but it was $value"
        )
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidNullDefaultForInt() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+", "default": "null" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        assertStringContains(
            "Invalid default for int32",
            assertFailsWith<RuntimeException> {
                MessageDataGenerator("org.apache.kafka.common.message")
                    .generate(testMessageSpec)
            }.message!!,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testValidNullDefaultForInt() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+", "nullableVersions": "0+",
                 "default": "null" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidNullDefaultForPotentiallyNonNullableArray() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "[]int32", "versions": "0+", "nullableVersions": "1+", 
                "default": "null" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        assertStringContains(
            "not all versions of this field are nullable",
            assertFailsWith<RuntimeException> {
                MessageDataGenerator("org.apache.kafka.common.message")
                    .generate(testMessageSpec)
            }.message!!
        )
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidNullDefaultForPotentiallyNonNullableField() {
        val testMessageSpec: MessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int8", "versions": "0+", "nullableVersions": "1+",
                 "default": "null" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        assertStringContains(
            "not all versions of this field are nullable",
            assertFailsWith<RuntimeException> {
                MessageDataGenerator("org.apache.kafka.common.message")
                    .generate(testMessageSpec)
            }.message!!
        )
    }

    @Test
    @Throws(Exception::class)
    fun testNoDefaultForPotentiallyNonNullableArray() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "[]int32", "versions": "0+", "nullableVersions": "1+" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    @Test
    @Ignore("validVersions field with nullable versions are not taken into account yet.")
    @Throws(Exception::class)
    fun testInvalidNullDefaultForValidVersions() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "1-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "[]int32", "versions": "0+", "nullableVersions": "1+",
                 "default": "null" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    @Test
    @Throws(Exception::class)
    fun testNoDefaultForPotentiallyNonNullableField() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int8", "versions": "0+", "nullableVersions": "1+" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    @Test
    @Throws(Exception::class)
    fun testEmptyDefaultForPotentiallyNonNullableArray() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int8", "versions": "0+", "nullableVersions": "1+",
                 "default": "" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    @Test
    @Throws(Exception::class)
    fun testNoDefaultForNonNullableField() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int8", "versions": "0+" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    @Test
    @Throws(Exception::class)
    fun testDefaultForField() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-2",
              "flexibleVersions": "none",
              "fields": [
                { "name": "field1", "type": "int8", "versions": "0+", "default": "8" }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        MessageDataGenerator("org.apache.kafka.common.message")
            .generate(testMessageSpec)
    }

    /**
     * Test attempting to create a field with an invalid name. The name is
     * invalid because it starts with an underscore.
     */
    @Test
    fun testInvalidFieldName() {
        assertStringContains(
            "Invalid field name",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "_badName", "type": "[]int32", "versions": "0+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    // TODO Add field name protection for Kotlin / Java keywords like "in", "for", "is" and so on.

    @Test
    fun testInvalidTagWithoutTaggedVersions() {
        assertStringContains(
            "If a tag is specified, taggedVersions must be specified as well.",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "int32", "versions": "0+", "tag": 0 }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
                fail("Expected the MessageSpec constructor to fail")
            }.message!!
        )
    }

    @Test
    fun testInvalidNegativeTag() {
        assertStringContains(
            "Tags cannot be negative",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "int32", "versions": "0+", 
                            "tag": -1, "taggedVersions": "0+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    @Test
    fun testInvalidFlexibleVersionsRange() {
        assertStringContains(
            "flexibleVersions must be either none, or an open-ended range",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0-2",
                      "fields": [
                        { "name": "field1", "type": "int32", "versions": "0+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    @Test
    fun testInvalidSometimesNullableTaggedField() {
        assertStringContains(
            "Either all tagged versions must be nullable, or none must be",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "string", "versions": "0+", 
                            "tag": 0, "taggedVersions": "0+", "nullableVersions": "1+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    @Test
    fun testInvalidTaggedVersionsNotASubetOfVersions() {
        assertStringContains(
            "taggedVersions must be a subset of versions",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "string", "versions": "0-2", 
                            "tag": 0, "taggedVersions": "1+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    @Test
    fun testInvalidTaggedVersionsWithoutTag() {
        assertStringContains(
            "Please specify a tag, or remove the taggedVersions",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "string", "versions": "0+", 
                            "taggedVersions": "1+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    @Test
    fun testInvalidTaggedVersionsRange() {
        assertStringContains(
            "taggedVersions must be either none, or an open-ended range",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "string", "versions": "0+", 
                            "tag": 0, "taggedVersions": "1-2" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }

    @Test
    fun testDuplicateTags() {
        assertStringContains(
            "duplicate tag",
            assertFailsWith<Throwable> {
                MessageGenerator.JSON_SERDE.readValue(
                    """
                    {
                      "type": "request",
                      "name": "FooBar",
                      "validVersions": "0-2",
                      "flexibleVersions": "0+",
                      "fields": [
                        { "name": "field1", "type": "string", "versions": "0+", 
                            "tag": 0, "taggedVersions": "0+" },
                        { "name": "field2", "type": "int64", "versions": "0+", 
                            "tag": 0, "taggedVersions": "0+" }
                      ]
                    }
                    """.trimIndent(),
                    MessageSpec::class.java,
                )
            }.message!!
        )
    }


    @Test
    @Throws(java.lang.Exception::class)
    fun testInvalidNullDefaultForNullableStruct() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0",
              "flexibleVersions": "none",
              "fields": [
                { "name": "struct1", "type": "MyStruct", "versions": "0+", "nullableVersions": "0+",
                  "default": "not-null", "fields": [
                    { "name": "field1", "type": "string", "versions": "0+" }
                  ]
                }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        assertContains(
            assertFailsWith<RuntimeException> {
                MessageDataGenerator("org.apache.kafka.common.message")
                    .generate(testMessageSpec)
            }.message!!,
            "Invalid default for struct field struct1. The only valid default for a struct field is the empty struct or null",
        )
    }

    @Test
    @Throws(java.lang.Exception::class)
    fun testInvalidNullDefaultForPotentiallyNonNullableStruct() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "FooBar",
              "validVersions": "0-1",
              "flexibleVersions": "none",
              "fields": [
                { "name": "struct1", "type": "MyStruct", "versions": "0+", "nullableVersions": "1+", 
                  "default": "null", "fields": [
                    { "name": "field1", "type": "string", "versions": "0+" }
                  ]
                }
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        assertContains(
            assertFailsWith<RuntimeException> {
                MessageDataGenerator("org.apache.kafka.common.message")
                    .generate(testMessageSpec)
            }.message!!,
            "not all versions of this field are nullable",
        )
    }
}
