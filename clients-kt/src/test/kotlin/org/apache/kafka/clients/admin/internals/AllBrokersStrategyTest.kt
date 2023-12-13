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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.BrokerKey
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class AllBrokersStrategyTest {
    
    private val logContext = LogContext()
    
    @Test
    fun testBuildRequest() {
        val strategy = AllBrokersStrategy(logContext)
        val builder = strategy.buildRequest(AllBrokersStrategy.LOOKUP_KEYS)
        assertEquals(emptyList(), builder.topics())
    }

    @Test
    fun testBuildRequestWithInvalidLookupKeys() {
        val strategy = AllBrokersStrategy(logContext)
        val key1 = BrokerKey(null)
        val key2 = BrokerKey(1)
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(setOf(key1)) }
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(setOf(key2)) }
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(setOf(key1, key2)) }
        val keys = AllBrokersStrategy.LOOKUP_KEYS.toMutableSet()
        keys.add(key2)
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(keys) }
    }

    @Test
    fun testHandleResponse() {
        val strategy = AllBrokersStrategy(logContext)
        val response = MetadataResponseData()
        response.brokers.add(
            MetadataResponseBroker()
                .setNodeId(1)
                .setHost("host1")
                .setPort(9092)
        )
        response.brokers.add(
            MetadataResponseBroker()
                .setNodeId(2)
                .setHost("host2")
                .setPort(9092)
        )
        val lookupResult = strategy.handleResponse(
            AllBrokersStrategy.LOOKUP_KEYS,
            MetadataResponse(response, ApiKeys.METADATA.latestVersion())
        )
        assertEquals(emptyMap(), lookupResult.failedKeys)
        val expectedMappedKeys = setOf(BrokerKey(1), BrokerKey(2))
        assertEquals(expectedMappedKeys, lookupResult.mappedKeys.keys)
        lookupResult.mappedKeys.forEach { (brokerKey, brokerId) -> assertEquals(brokerId, brokerKey.brokerId) }
    }

    @Test
    fun testHandleResponseWithNoBrokers() {
        val strategy = AllBrokersStrategy(logContext)
        val response = MetadataResponseData()
        val lookupResult = strategy.handleResponse(
            keys = AllBrokersStrategy.LOOKUP_KEYS,
            response = MetadataResponse(response, ApiKeys.METADATA.latestVersion()),
        )
        assertEquals(emptyMap(), lookupResult.failedKeys)
        assertEquals(emptyMap(), lookupResult.mappedKeys)
    }

    @Test
    fun testHandleResponseWithInvalidLookupKeys() {
        val strategy = AllBrokersStrategy(logContext)
        val key1 = BrokerKey(null)
        val key2 = BrokerKey(1)
        val response = MetadataResponse(MetadataResponseData(), ApiKeys.METADATA.latestVersion())
        assertFailsWith<IllegalArgumentException> { strategy.handleResponse(setOf(key1), response) }
        assertFailsWith<IllegalArgumentException> { strategy.handleResponse(setOf(key2), response) }
        assertFailsWith<IllegalArgumentException> { strategy.handleResponse(setOf(key1, key2), response) }
        val keys = AllBrokersStrategy.LOOKUP_KEYS.toMutableSet()
        keys.add(key2)
        assertFailsWith<IllegalArgumentException> { strategy.handleResponse(keys, response) }
    }
}
