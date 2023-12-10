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

package org.apache.kafka.common.config.provider

import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

internal class EnvVarConfigProviderTest {
    
    private lateinit var envVarConfigProvider: EnvVarConfigProvider
    
    @BeforeEach
    fun setup() {
        val testEnvVars = mapOf(
            "test_var1" to "value1",
            "secret_var2" to "value2",
            "new_var3" to "value3",
            "not_so_secret_var4" to "value4",
        )
        envVarConfigProvider = EnvVarConfigProvider(testEnvVars)
        envVarConfigProvider.configure(mapOf("" to ""))
    }

    @Test
    fun testGetAllEnvVarsNotEmpty() {
        val (data) = envVarConfigProvider[""]

        assertNotEquals(0, data.size)
    }

    @Test
    fun testGetMultipleKeysAndCompare() {
        val (data) = envVarConfigProvider[""]

        assertNotEquals(0, data.size)
        assertEquals("value1", data["test_var1"])
        assertEquals("value2", data["secret_var2"])
        assertEquals("value3", data["new_var3"])
        assertEquals("value4", data["not_so_secret_var4"])
    }

    @Test
    fun testGetOneKeyWithNullPath() {
        val (data) = envVarConfigProvider[null, setOf("secret_var2")]

        assertEquals(1, data.size)
        assertEquals("value2", data["secret_var2"])
    }

    @Test
    fun testGetOneKeyWithEmptyPath() {
        val (data) = envVarConfigProvider["", setOf("test_var1")]

        assertEquals(1, data.size)
        assertEquals("value1", data["test_var1"])
    }

    @Test
    fun testGetEnvVarsByKeyList() {
        val keyList: Set<String> = HashSet(mutableListOf("test_var1", "secret_var2"))
        val keys = envVarConfigProvider[null, keyList].data.keys
        assertEquals(keyList, keys)
    }

    @Test
    fun testNotNullPathNonEmptyThrowsException() {
        assertFailsWith<ConfigException> { envVarConfigProvider["test-path", setOf("test_var1")] }
    }

    @Test
    fun testRegExpEnvVarsSingleEntryKeyList() {
        val testConfigMap = mapOf(EnvVarConfigProvider.ALLOWLIST_PATTERN_CONFIG to "secret_.*")
        envVarConfigProvider.configure(testConfigMap)
        val keyList = setOf("secret_var2")
        val keys = envVarConfigProvider[null, setOf("secret_var2")].data.keys

        assertEquals(keyList, keys)
    }

    @Test
    fun testRegExpEnvVarsNoKeyList() {
        val testConfigMap = mapOf(EnvVarConfigProvider.ALLOWLIST_PATTERN_CONFIG to "secret_.*")
        envVarConfigProvider.configure(testConfigMap)
        val keys = envVarConfigProvider[""].data.keys

        assertEquals(setOf("secret_var2"), keys)
    }
}

