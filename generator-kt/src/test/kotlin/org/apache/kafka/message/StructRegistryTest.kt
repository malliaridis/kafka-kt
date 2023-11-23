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
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class StructRegistryTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    @Throws(Exception::class)
    fun testCommonStructs() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "LeaderAndIsrRequest",
              "validVersions": "0-2",
              "flexibleVersions": "0+",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+" },
                { "name": "field2", "type": "[]TestCommonStruct", "versions": "1+" },
                { "name": "field3", "type": "[]TestInlineStruct", "versions": "0+", 
                "fields": [
                  { "name": "inlineField1", "type": "int64", "versions": "0+" }
                ]}
              ],
              "commonStructs": [
                { "name": "TestCommonStruct", "versions": "0+", "fields": [
                  { "name": "commonField1", "type": "int64", "versions": "0+" }
                ]}
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        val structRegistry = StructRegistry()
        structRegistry.register(testMessageSpec)
        assertEquals(
            expected = structRegistry.commonStructNames(),
            actual = setOf("TestCommonStruct"),
        )
        assertFalse(structRegistry.isStructArrayWithKeys(testMessageSpec.fields[1]))
        assertFalse(structRegistry.isStructArrayWithKeys(testMessageSpec.fields[2]))
        assertTrue(structRegistry.commonStructs().hasNext())
        assertEquals(
            expected = structRegistry.commonStructs().next().name,
            actual = "TestCommonStruct",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testReSpecifiedCommonStructError() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "LeaderAndIsrRequest",
              "validVersions": "0-2",
              "flexibleVersions": "0+",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+" },
                { "name": "field2", "type": "[]TestCommonStruct", "versions": "0+", 
                "fields": [
                  { "name": "inlineField1", "type": "int64", "versions": "0+" }
                ]}
              ],
              "commonStructs": [
                { "name": "TestCommonStruct", "versions": "0+", "fields": [
                  { "name": "commonField1", "type": "int64", "versions": "0+" }
                ]}
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        val structRegistry = StructRegistry()
        try {
            structRegistry.register(testMessageSpec)
            fail("Expected StructRegistry#registry to fail")
        } catch (e: RuntimeException) {
            assertTrue(
                e.message!!.contains(
                    "Can't re-specify the common struct TestCommonStruct as an inline struct."
                )
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuplicateCommonStructError() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "LeaderAndIsrRequest",
              "validVersions": "0-2",
              "flexibleVersions": "0+",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+" }
              ],
              "commonStructs": [
                { "name": "TestCommonStruct", "versions": "0+", "fields": [
                  { "name": "commonField1", "type": "int64", "versions": "0+" }
                ]},
                { "name": "TestCommonStruct", "versions": "0+", "fields": [
                  { "name": "commonField1", "type": "int64", "versions": "0+" }
                ]}
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        val structRegistry = StructRegistry()
        try {
            structRegistry.register(testMessageSpec)
            fail("Expected StructRegistry#registry to fail")
        } catch (e: RuntimeException) {
            assertTrue(
                e.message!!.contains("Common struct TestCommonStruct was specified twice.")
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSingleStruct() {
        val testMessageSpec = MessageGenerator.JSON_SERDE.readValue(
            """
            {
              "type": "request",
              "name": "LeaderAndIsrRequest",
              "validVersions": "0-2",
              "flexibleVersions": "0+",
              "fields": [
                { "name": "field1", "type": "int32", "versions": "0+" },
                { "name": "field2", "type": "TestInlineStruct", "versions": "0+", 
                "fields": [
                  { "name": "inlineField1", "type": "int64", "versions": "0+" }
                ]}
              ]
            }
            """.trimIndent(),
            MessageSpec::class.java,
        )
        val structRegistry = StructRegistry()
        structRegistry.register(testMessageSpec)
        val field2 = testMessageSpec.fields[1]
        assertTrue(field2.type.isStruct)
        assertEquals(
            expected = field2.type.toString(),
            actual = "TestInlineStruct",
        )
        assertEquals(
            expected = field2.name,
            actual = "field2",
        )
        assertEquals(
            expected = structRegistry.findStruct(field2).name,
            actual = "TestInlineStruct",
        )
        assertFalse(structRegistry.isStructArrayWithKeys(field2))
    }
}
