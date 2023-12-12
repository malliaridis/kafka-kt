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

import org.junit.jupiter.api.BeforeEach
import java.io.IOException
import java.io.Reader
import java.io.StringReader
import java.util.ServiceLoader
import java.util.stream.StreamSupport
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FileConfigProviderTest {
    
    private var configProvider: FileConfigProvider? = null
    
    @BeforeEach
    fun setup() {
        configProvider = TestFileConfigProvider()
    }

    @Test
    fun testGetAllKeysAtPath() {
        val (data, ttl) = configProvider!!["dummy"]
        val result = mutableMapOf<String, String>()
        result["testKey"] = "testResult"
        result["testKey2"] = "testResult2"
        assertEquals(result, data)
        assertNull(ttl)
    }

    @Test
    fun testGetOneKeyAtPath() {
        val (data, ttl) = configProvider!!["dummy", setOf("testKey")]
        val result = mutableMapOf<String, String>()
        result["testKey"] = "testResult"
        assertEquals(result, data)
        assertNull(ttl)
    }

    @Test
    fun testEmptyPath() {
        val (data, ttl) = configProvider!!["", setOf("testKey")]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testEmptyPathWithKey() {
        val (data, ttl) = configProvider!![""]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testNullPath() {
        val (data, ttl) = configProvider!![null]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testNullPathWithKey() {
        val (data, ttl) = configProvider!![null, setOf("testKey")]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testServiceLoaderDiscovery() {
        val serviceLoader = ServiceLoader.load(ConfigProvider::class.java)
        assertTrue(
            StreamSupport.stream(serviceLoader.spliterator(), false)
                .anyMatch { configProvider: ConfigProvider? -> configProvider is FileConfigProvider })
    }

    class TestFileConfigProvider : FileConfigProvider() {
        @Throws(IOException::class)
        override fun reader(path: String): Reader {
            return StringReader("testKey=testResult\ntestKey2=testResult2")
        }
    }
}
