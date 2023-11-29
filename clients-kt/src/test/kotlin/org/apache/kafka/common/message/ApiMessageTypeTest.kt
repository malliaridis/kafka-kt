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

package org.apache.kafka.common.message

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

@Timeout(120)
class ApiMessageTypeTest {

    @Test
    fun testFromApiKey() {
        for (type in ApiMessageType.values()) {
            val type2 = ApiMessageType.fromApiKey(type.apiKey())
            assertEquals(type2, type)
        }
    }

    @Test
    fun testInvalidFromApiKey() {
        try {
            ApiMessageType.fromApiKey(-1)
            fail("expected to get an UnsupportedVersionException")
        } catch (_: UnsupportedVersionException) {
            // expected
        }
    }

    @Test
    fun testUniqueness() {
        val ids: MutableSet<Short> = hashSetOf()
        val requestNames = mutableSetOf<String>()
        val responseNames = mutableSetOf<String>()
        for (type in ApiMessageType.values()) {
            assertFalse(
                ids.contains(type.apiKey()),
                "found two ApiMessageType objects with id ${type.apiKey()}"
            )
            ids.add(type.apiKey())
            val requestName = type.newRequest().javaClass.getSimpleName()
            assertFalse(
                requestNames.contains(requestName),
                "found two ApiMessageType objects with requestName $requestName"
            )
            requestNames.add(requestName)
            val responseName = type.newResponse().javaClass.getSimpleName()
            assertFalse(
                responseNames.contains(responseName),
                "found two ApiMessageType objects with responseName $responseName"
            )
            responseNames.add(responseName)
        }
        assertEquals(ApiMessageType.values().size, ids.size)
        assertEquals(ApiMessageType.values().size, requestNames.size)
        assertEquals(ApiMessageType.values().size, responseNames.size)
    }

    @Test
    fun testHeaderVersion() {
        assertEquals(1, ApiMessageType.PRODUCE.requestHeaderVersion(0))
        assertEquals(0, ApiMessageType.PRODUCE.responseHeaderVersion(0))
        assertEquals(1, ApiMessageType.PRODUCE.requestHeaderVersion(1))
        assertEquals(0, ApiMessageType.PRODUCE.responseHeaderVersion(1))
        assertEquals(0, ApiMessageType.CONTROLLED_SHUTDOWN.requestHeaderVersion(0))
        assertEquals(0, ApiMessageType.CONTROLLED_SHUTDOWN.responseHeaderVersion(0))
        assertEquals(1, ApiMessageType.CONTROLLED_SHUTDOWN.requestHeaderVersion(1))
        assertEquals(0, ApiMessageType.CONTROLLED_SHUTDOWN.responseHeaderVersion(1))
        assertEquals(1, ApiMessageType.CREATE_TOPICS.requestHeaderVersion(4))
        assertEquals(0, ApiMessageType.CREATE_TOPICS.responseHeaderVersion(4))
        assertEquals(2, ApiMessageType.CREATE_TOPICS.requestHeaderVersion(5))
        assertEquals(1, ApiMessageType.CREATE_TOPICS.responseHeaderVersion(5))
    }

    /**
     * Kafka currently supports direct upgrades from 0.8 to the latest version. As such, it has to support all apis
     * starting from version 0 and we must have schemas from the oldest version to the latest.
     */
    @Test
    fun testAllVersionsHaveSchemas() {
        for (type in ApiMessageType.values()) {
            assertEquals(0, type.lowestSupportedVersion())
            assertEquals(type.requestSchemas().size, type.responseSchemas().size)
            for (schema in type.requestSchemas()) assertNotNull(schema)
            for (schema in type.responseSchemas()) assertNotNull(schema)
            assertEquals(type.highestSupportedVersion() + 1, type.requestSchemas().size)
        }
    }

    @Test
    fun testApiIdsArePositive() {
        for (type in ApiMessageType.values()) assertTrue(type.apiKey() >= 0)
    }
}
