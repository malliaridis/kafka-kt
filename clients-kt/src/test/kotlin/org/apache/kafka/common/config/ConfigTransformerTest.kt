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

import org.apache.kafka.common.config.provider.ConfigProvider
import org.junit.jupiter.api.Test
import kotlin.test.BeforeEach
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ConfigTransformerTest {
    
    private var configTransformer: ConfigTransformer? = null
    
    @BeforeEach
    fun setup() {
        configTransformer = ConfigTransformer(mapOf("test" to TestConfigProvider()))
    }

    @Test
    @Throws(Exception::class)
    fun testReplaceVariable() {
        val (data, ttls) = configTransformer!!.transform(mapOf(MY_KEY to "\${test:testPath:testKey}"))
        assertEquals(TEST_RESULT, data[MY_KEY])
        assertTrue(ttls.isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testReplaceVariableWithTTL() {
        val (data, ttls) = configTransformer!!.transform(mapOf(MY_KEY to "\${test:testPath:testKeyWithTTL}"))
        assertEquals(TEST_RESULT_WITH_TTL, data[MY_KEY])
        assertEquals(1L, ttls[TEST_PATH])
    }

    @Test
    @Throws(Exception::class)
    fun testReplaceMultipleVariablesInValue() {
        val (data) = configTransformer!!.transform(
            mapOf(MY_KEY to "hello, \${test:testPath:testKey}; goodbye, \${test:testPath:testKeyWithTTL}!!!")
        )
        assertEquals("hello, testResult; goodbye, testResultWithTTL!!!", data[MY_KEY])
    }

    @Test
    @Throws(Exception::class)
    fun testNoReplacement() {
        val (data) = configTransformer!!.transform(mapOf(MY_KEY to "\${test:testPath:missingKey}"))
        assertEquals("\${test:testPath:missingKey}", data[MY_KEY])
    }

    @Test
    @Throws(Exception::class)
    fun testSingleLevelOfIndirection() {
        val (data) = configTransformer!!.transform(mapOf(MY_KEY to "\${test:testPath:testIndirection}"))
        assertEquals("\${test:testPath:testResult}", data[MY_KEY])
    }

    @Test
    @Throws(Exception::class)
    fun testReplaceVariableNoPath() {
        val (data, ttls) = configTransformer!!.transform(mapOf(MY_KEY to "\${test:testKey}"))
        assertEquals(TEST_RESULT_NO_PATH, data[MY_KEY])
        assertTrue(ttls.isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun testReplaceMultipleVariablesWithoutPathInValue() {
        val (data) = configTransformer!!.transform(mapOf(MY_KEY to "first \${test:testKey}; second \${test:testKey}"))
        assertEquals("first testResultNoPath; second testResultNoPath", data[MY_KEY])
    }

    @Test
    @Throws(Exception::class)
    fun testNullConfigValue() {
        // Kotlin Migration: keys with null values are treated equally to undefined keys and therefore
        // the migration replaces the single entry map with an empty map
        val (data, ttls) = configTransformer!!.transform(emptyMap())
        assertNull(data[MY_KEY])
        assertTrue(ttls.isEmpty())
    }

    class TestConfigProvider : ConfigProvider {

        override fun configure(configs: Map<String, Any?>) = Unit

        override fun get(path: String?): ConfigData {
            return ConfigData(emptyMap())
        }

        override fun get(path: String?, keys: Set<String>): ConfigData {
            val data = mutableMapOf<String, String>()
            var ttl: Long? = null

            if (TEST_PATH == path) {
                if (keys.contains(TEST_KEY)) {
                    data[TEST_KEY] = TEST_RESULT
                }
                if (keys.contains(TEST_KEY_WITH_TTL)) {
                    data[TEST_KEY_WITH_TTL] = TEST_RESULT_WITH_TTL
                    ttl = 1L
                }
                if (keys.contains(TEST_INDIRECTION)) {
                    data[TEST_INDIRECTION] = "\${test:testPath:testResult}"
                }
            } else {
                if (keys.contains(TEST_KEY)) {
                    data[TEST_KEY] = TEST_RESULT_NO_PATH
                }
            }
            return ConfigData(data, ttl)
        }

        override fun close() = Unit
    }

    companion object {
        const val MY_KEY = "myKey"
        const val TEST_INDIRECTION = "testIndirection"
        const val TEST_KEY = "testKey"
        const val TEST_KEY_WITH_TTL = "testKeyWithTTL"
        const val TEST_PATH = "testPath"
        const val TEST_RESULT = "testResult"
        const val TEST_RESULT_WITH_TTL = "testResultWithTTL"
        const val TEST_RESULT_NO_PATH = "testResultNoPath"
    }
}
