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

import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.AllBrokersFuture
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.BrokerKey
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AllBrokersStrategyIntegrationTest {
    
    private val logContext = LogContext()
    
    private val time = MockTime()
    
    private fun buildDriver(result: AllBrokersFuture<Int>): AdminApiDriver<BrokerKey, Int> {
        return AdminApiDriver(
            handler = MockApiHandler(),
            future = result,
            deadlineMs = time.milliseconds() + TIMEOUT_MS,
            retryBackoffMs = RETRY_BACKOFF_MS,
            logContext = logContext,
        )
    }

    @Test
    fun testFatalLookupError() {
        val result = AllBrokersFuture<Int>()
        val driver = buildDriver(result)
        val requestSpecs = driver.poll()
        assertEquals(1, requestSpecs.size)
        val spec = requestSpecs[0]
        assertEquals(AllBrokersStrategy.LOOKUP_KEYS, spec.keys)
        driver.onFailure(time.milliseconds(), spec, UnknownServerException())
        assertTrue(result.all().isDone)
        assertFutureThrows(result.all(), UnknownServerException::class.java)
        assertEquals(emptyList(), driver.poll())
    }

    @Test
    fun testRetryLookupAfterDisconnect() {
        val result = AllBrokersFuture<Int>()
        val driver = buildDriver(result)
        val requestSpecs = driver.poll()
        assertEquals(1, requestSpecs.size)
        val spec = requestSpecs[0]
        assertEquals(AllBrokersStrategy.LOOKUP_KEYS, spec.keys)
        driver.onFailure(time.milliseconds(), spec, DisconnectException())
        val retrySpecs = driver.poll()
        assertEquals(1, retrySpecs.size)
        with(retrySpecs[0]) {
            assertEquals(AllBrokersStrategy.LOOKUP_KEYS, keys)
            assertEquals(time.milliseconds(), nextAllowedTryMs)
            assertEquals(emptyList(), driver.poll())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testMultiBrokerCompletion() {
        val result = AllBrokersFuture<Int>()
        val driver = buildDriver(result)
        val lookupSpecs = driver.poll()
        assertEquals(1, lookupSpecs.size)
        val lookupSpec = lookupSpecs[0]
        val brokerIds = setOf(1, 2)
        driver.onResponse(
            currentTimeMs = time.milliseconds(),
            spec = lookupSpec,
            response = responseWithBrokers(brokerIds),
            node = Node.noNode(),
        )
        assertTrue(result.all().isDone)
        val brokerFutures = result.all().get()
        val requestSpecs = driver.poll()
        assertEquals(2, requestSpecs.size)
        val requestSpec1 = requestSpecs[0]
        val brokerId1 = assertNotNull(requestSpec1.scope.destinationBrokerId())
        assertTrue(brokerIds.contains(brokerId1))
        driver.onResponse(
            currentTimeMs = time.milliseconds(),
            spec = requestSpec1,
            response = responseWithBrokers(emptySet()),
            node = Node.noNode(),
        )
        val future1 = brokerFutures[brokerId1]!!
        assertTrue(future1.isDone)
        val requestSpec2 = requestSpecs[1]
        val brokerId2 = assertNotNull(requestSpec2.scope.destinationBrokerId())
        assertNotEquals(brokerId1, brokerId2)
        assertTrue(brokerIds.contains(brokerId2))
        driver.onResponse(
            currentTimeMs = time.milliseconds(),
            spec = requestSpec2,
            response = responseWithBrokers(emptySet()),
            node = Node.noNode(),
        )
        val future2 = brokerFutures[brokerId2]!!
        assertTrue(future2.isDone)
        assertEquals(emptyList(), driver.poll())
    }

    @Test
    @Throws(Exception::class)
    fun testRetryFulfillmentAfterDisconnect() {
        val result = AllBrokersFuture<Int>()
        val driver = buildDriver(result)
        val lookupSpecs = driver.poll()
        assertEquals(1, lookupSpecs.size)
        val lookupSpec = lookupSpecs[0]
        val brokerId = 1
        driver.onResponse(
            currentTimeMs = time.milliseconds(),
            spec = lookupSpec,
            response = responseWithBrokers(setOf(brokerId)),
            node = Node.noNode(),
        )
        assertTrue(result.all().isDone)
        val brokerFutures = result.all().get()
        val future = brokerFutures[brokerId]!!
        assertFalse(future.isDone)
        val requestSpecs = driver.poll()
        assertEquals(1, requestSpecs.size)
        val requestSpec = requestSpecs[0]
        driver.onFailure(time.milliseconds(), requestSpec, DisconnectException())
        assertFalse(future.isDone)
        val retrySpecs = driver.poll()
        assertEquals(1, retrySpecs.size)
        val retrySpec = retrySpecs[0]
        assertEquals(time.milliseconds() + RETRY_BACKOFF_MS, retrySpec.nextAllowedTryMs)
        assertEquals(brokerId, retrySpec.scope.destinationBrokerId())
        driver.onResponse(
            currentTimeMs = time.milliseconds(),
            spec = retrySpec,
            response = responseWithBrokers(emptySet()),
            node = Node(id = brokerId, host = "host", port = 1234),
        )
        assertTrue(future.isDone)
        assertEquals(brokerId, future.get())
        assertEquals(emptyList(), driver.poll())
    }

    @Test
    @Throws(Exception::class)
    fun testFatalFulfillmentError() {
        val result = AllBrokersFuture<Int>()
        val driver = buildDriver(result)
        val lookupSpecs = driver.poll()
        assertEquals(1, lookupSpecs.size)
        val lookupSpec = lookupSpecs[0]
        val brokerId = 1
        driver.onResponse(
            currentTimeMs = time.milliseconds(),
            spec = lookupSpec,
            response = responseWithBrokers(setOf(brokerId)),
            node = Node.noNode(),
        )
        assertTrue(result.all().isDone)
        val brokerFutures = result.all().get()
        val future = brokerFutures[brokerId]!!
        assertFalse(future.isDone)
        val requestSpecs = driver.poll()
        assertEquals(1, requestSpecs.size)
        val requestSpec = requestSpecs[0]
        driver.onFailure(
            currentTimeMs = time.milliseconds(),
            spec = requestSpec,
            t = UnknownServerException(),
        )
        assertTrue(future.isDone)
        assertFutureThrows(future, UnknownServerException::class.java)
        assertEquals(emptyList(), driver.poll())
    }

    private fun responseWithBrokers(brokerIds: Set<Int>): MetadataResponse {
        val response = MetadataResponseData()
        for (brokerId in brokerIds) {
            response.brokers.add(
                MetadataResponseBroker()
                    .setNodeId(brokerId)
                    .setHost("host$brokerId")
                    .setPort(9092)
            )
        }
        return MetadataResponse(response, ApiKeys.METADATA.latestVersion())
    }

    private inner class MockApiHandler : Batched<BrokerKey, Int>() {

        private val allBrokersStrategy = AllBrokersStrategy(logContext)

        override fun apiName(): String = "mock-api"

        override fun buildBatchedRequest(brokerId: Int, keys: Set<BrokerKey>): AbstractRequest.Builder<*> =
            MetadataRequest.Builder(MetadataRequestData())

        override fun handleResponse(
            broker: Node,
            keys: Set<BrokerKey>,
            response: AbstractResponse,
        ): ApiResult<BrokerKey, Int> = ApiResult.completed(keys.iterator().next(), broker.id)

        override fun lookupStrategy(): AdminApiLookupStrategy<BrokerKey> = allBrokersStrategy
    }

    companion object {

        private const val TIMEOUT_MS: Long = 5000

        private const val RETRY_BACKOFF_MS: Long = 100
    }
}
