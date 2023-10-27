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

import org.apache.kafka.clients.admin.internals.AdminApiDriver.RequestSpec
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched
import org.apache.kafka.clients.admin.internals.AdminApiLookupStrategy.LookupResult
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

internal class AdminApiDriverTest {

    @Test
    fun testCoalescedLookup() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1", "bar" to "c1"))
        val lookupRequests = mapOf(
            setOf("foo", "bar") to mapped("foo", 1, "bar", 2),
        )
        ctx.poll(lookupRequests, emptyMap())
        val fulfillmentResults = mapOf(
            setOf("foo") to completed("foo", 15L),
            setOf("bar") to completed("bar", 30L),
        )
        ctx.poll(emptyMap(), fulfillmentResults)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testCoalescedFulfillment() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1", "bar" to "c2"))
        val lookupRequests = mapOf(
            setOf("foo") to mapped("foo", 1),
            setOf("bar") to mapped("bar", 1),
        )
        ctx.poll(lookupRequests, emptyMap())
        val fulfillmentResults = mapOf(
            setOf("foo", "bar") to completed("foo", 15L, "bar", 30L)
        )
        ctx.poll(emptyMap(), fulfillmentResults)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testKeyLookupFailure() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1", "bar" to "c2"))
        val lookupRequests = mapOf(
            setOf("foo") to failedLookup("foo", UnknownServerException()),
            setOf("bar") to mapped("bar", 1),
        )
        ctx.poll(lookupRequests, emptyMap())
        val fulfillmentResults = mapOf(setOf("bar") to completed("bar", 30L))
        ctx.poll(emptyMap(), fulfillmentResults)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testKeyLookupRetry() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1", "bar" to "c2"))
        val lookupRequests = mapOf(
            setOf("foo") to emptyLookup(),
            setOf("bar") to mapped("bar", 1),
        )
        ctx.poll(lookupRequests, emptyMap())
        val fooRetry = mapOf(setOf("foo") to mapped("foo", 1))
        val barFulfillment = mapOf(setOf("bar") to completed("bar", 30L))
        ctx.poll(fooRetry, barFulfillment)
        val fooFulfillment = mapOf(setOf("foo") to completed("foo", 15L))
        ctx.poll(emptyMap(), fooFulfillment)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testStaticMapping() {
        val ctx = TestContext.staticMapped(mapOf("foo" to 0, "bar" to 1, "baz" to 1))
        val fulfillmentResults = mapOf(
            setOf("foo") to completed("foo", 15L),
            setOf("bar", "baz") to completed("bar", 30L, "baz", 45L),
        )
        ctx.poll(emptyMap(), fulfillmentResults)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testFulfillmentFailure() {
        val ctx = TestContext.staticMapped(mapOf("foo" to 0, "bar" to 1, "baz" to 1))
        val fulfillmentResults = mapOf(
            setOf("foo") to failed("foo", UnknownServerException()),
            setOf("bar", "baz") to completed("bar", 30L, "baz", 45L),
        )
        ctx.poll(emptyMap(), fulfillmentResults)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testFulfillmentRetry() {
        val ctx = TestContext.staticMapped(mapOf("foo" to 0, "bar" to 1, "baz" to 1))
        val fulfillmentResults = mapOf(
            setOf("foo") to completed("foo", 15L),
            setOf("bar", "baz") to completed("bar", 30L),
        )
        ctx.poll(emptyMap(), fulfillmentResults)
        val bazRetry = mapOf(setOf("baz") to completed("baz", 45L))
        ctx.poll(emptyMap(), bazRetry)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testFulfillmentUnmapping() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1", "bar" to "c2"))
        val lookupRequests = mapOf(
            setOf("foo") to mapped("foo", 0),
            setOf("bar") to mapped("bar", 1),
        )
        ctx.poll(lookupRequests, emptyMap())
        val fulfillmentResults = mapOf(
            setOf("foo") to completed("foo", 15L),
            setOf("bar") to unmapped("bar"),
        )
        ctx.poll(emptyMap(), fulfillmentResults)
        val barLookupRetry = mapOf(setOf("bar") to mapped("bar", 1))
        ctx.poll(barLookupRetry, emptyMap())
        val barFulfillRetry = mapOf(setOf("bar") to completed("bar", 30L))
        ctx.poll(emptyMap(), barFulfillRetry)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testRecoalescedLookup() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1", "bar" to "c1"))
        val lookupRequests = mapOf(setOf("foo", "bar") to mapped("foo", 1, "bar", 2))
        ctx.poll(lookupRequests, emptyMap())
        val fulfillment = mapOf(
            setOf("foo") to unmapped("foo"),
            setOf("bar") to unmapped("bar"),
        )
        ctx.poll(emptyMap(), fulfillment)
        val retryLookupRequests = mapOf(setOf("foo", "bar") to mapped("foo", 3, "bar", 3))
        ctx.poll(retryLookupRequests, emptyMap())
        val retryFulfillment = mapOf(setOf("foo", "bar") to completed("foo", 15L, "bar", 30L))
        ctx.poll(emptyMap(), retryFulfillment)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testRetryLookupAfterDisconnect() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1"))
        val initialLeaderId = 1
        val initialLookup = mapOf(setOf("foo") to mapped("foo", initialLeaderId))
        ctx.poll(initialLookup, emptyMap())
        assertMappedKey(ctx, "foo", initialLeaderId)
        ctx.handler.expectRequest(setOf("foo"), completed("foo", 15L))
        val requestSpecs = ctx.driver.poll()
        assertEquals(1, requestSpecs.size)
        val requestSpec = requestSpecs[0]
        assertEquals(initialLeaderId, requestSpec.scope.destinationBrokerId())
        ctx.driver.onFailure(ctx.time.milliseconds(), requestSpec, DisconnectException())
        assertUnmappedKey(ctx, "foo")
        val retryLeaderId = 2
        ctx.lookupStrategy().expectLookup(setOf("foo"), mapped("foo", retryLeaderId))
        val retryLookupSpecs = ctx.driver.poll()
        assertEquals(1, retryLookupSpecs.size)

        with(retryLookupSpecs[0]) {
            assertEquals(ctx.time.milliseconds(), nextAllowedTryMs)
            assertEquals(1, tries)
        }
    }

    @Test
    fun testRetryLookupAndDisableBatchAfterNoBatchedFindCoordinatorsException() {
        val time = MockTime()
        val lc = LogContext()
        val groupIds = setOf("g1", "g2")
        val handler = DeleteConsumerGroupsHandler(lc)
        val future = AdminApiFuture.forKeys<CoordinatorKey, Unit>(
            groupIds.map { CoordinatorKey.byGroupId(it) }.toSet()
        )
        val driver = AdminApiDriver(
            handler = handler,
            future = future,
            deadlineMs = time.milliseconds() + API_TIMEOUT_MS,
            retryBackoffMs = RETRY_BACKOFF_MS.toLong(),
            logContext = LogContext(),
        )
        assertTrue(handler.lookupStrategy.batch)
        val requestSpecs = driver.poll()
        // Expect CoordinatorStrategy to try resolving all coordinators in a single request
        assertEquals(1, requestSpecs.size)
        val requestSpec = requestSpecs[0]
        driver.onFailure(time.milliseconds(), requestSpec, NoBatchedFindCoordinatorsException("message"))
        assertFalse(handler.lookupStrategy.batch)

        // Batching is now disabled, so we now have a request per groupId
        val retryLookupSpecs = driver.poll()
        assertEquals(groupIds.size, retryLookupSpecs.size)
        // These new requests are treated a new requests and not retries
        for (spec in retryLookupSpecs) with(spec) {
            assertEquals(0, nextAllowedTryMs)
            assertEquals(0, tries)
        }
    }

    @Test
    fun testCoalescedStaticAndDynamicFulfillment() {
        val dynamicMapping = mapOf("foo" to "c1")
        val staticMapping = mapOf("bar" to 1)
        val ctx = TestContext(staticMapping, dynamicMapping)

        // Initially we expect a lookup for the dynamic key and a
        // fulfillment request for the static key
        val lookupResult = mapped("foo", 1)
        ctx.lookupStrategy().expectLookup(setOf("foo"), lookupResult)
        ctx.handler.expectRequest(setOf("bar"), completed("bar", 10L))
        val requestSpecs = ctx.driver.poll()
        assertEquals(2, requestSpecs.size)
        val lookupSpec = requestSpecs[0]
        assertEquals(setOf("foo"), lookupSpec.keys)
        ctx.assertLookupResponse(lookupSpec, lookupResult)

        // Receive a disconnect from the fulfillment request so that
        // we have an opportunity to coalesce the keys.
        val fulfillmentSpec = requestSpecs[1]
        assertEquals(setOf("bar"), fulfillmentSpec.keys)
        ctx.driver.onFailure(ctx.time.milliseconds(), fulfillmentSpec, DisconnectException())

        // Now we should get two fulfillment requests. One of them will
        // the coalesced dynamic and static keys for broker 1. The other
        // should contain the single dynamic key for broker 0.
        ctx.handler.reset()
        ctx.handler.expectRequest(setOf("foo", "bar"), completed("foo", 15L, "bar", 30L))
        val coalescedSpecs = ctx.driver.poll()
        assertEquals(1, coalescedSpecs.size)
        val coalescedSpec = coalescedSpecs[0]
        assertEquals(setOf("foo", "bar"), coalescedSpec.keys)

        // Disconnect in order to ensure that only the dynamic key is unmapped.
        // Then complete the remaining requests.
        ctx.driver.onFailure(ctx.time.milliseconds(), coalescedSpec, DisconnectException())
        val fooLookupRetry = mapOf(setOf("foo") to mapped("foo", 3))
        val barFulfillmentRetry = mapOf(setOf("bar") to completed("bar", 30L))
        ctx.poll(fooLookupRetry, barFulfillmentRetry)
        val fooFulfillmentRetry = mapOf(setOf("foo") to completed("foo", 15L))
        ctx.poll(emptyMap(), fooFulfillmentRetry)
        ctx.poll(emptyMap(), emptyMap())
    }

    @Test
    fun testLookupRetryBookkeeping() {
        val ctx = TestContext.dynamicMapped(mapOf("foo" to "c1"))
        val emptyLookup = emptyLookup()
        ctx.lookupStrategy().expectLookup(setOf("foo"), emptyLookup)
        val requestSpecs = ctx.driver.poll()
        assertEquals(1, requestSpecs.size)
        val requestSpec = requestSpecs[0]
        assertEquals(0, requestSpec.tries)
        assertEquals(0L, requestSpec.nextAllowedTryMs)
        ctx.assertLookupResponse(requestSpec, emptyLookup)
        val retrySpecs = ctx.driver.poll()
        assertEquals(1, retrySpecs.size)
        with(retrySpecs[0]) {
            assertEquals(1, tries)
            assertEquals(ctx.time.milliseconds(), nextAllowedTryMs)
        }
    }

    @Test
    fun testFulfillmentRetryBookkeeping() {
        val ctx = TestContext.staticMapped(mapOf("foo" to 0))
        val emptyFulfillment = emptyFulfillment()
        ctx.handler.expectRequest(setOf("foo"), emptyFulfillment)
        val requestSpecs = ctx.driver.poll()
        assertEquals(1, requestSpecs.size)
        val requestSpec = requestSpecs[0]
        assertEquals(0, requestSpec.tries)
        assertEquals(0L, requestSpec.nextAllowedTryMs)
        ctx.assertResponse(requestSpec, emptyFulfillment, Node.noNode())
        val retrySpecs = ctx.driver.poll()
        assertEquals(1, retrySpecs.size)
        with(retrySpecs[0]) {
            assertEquals(1, tries)
            assertEquals(ctx.time.milliseconds() + RETRY_BACKOFF_MS, nextAllowedTryMs)
        }
    }

    private data class MockRequestScope(
        private val destinationBrokerId: Int?,
        private val id: String?,
    ) : ApiRequestScope {
        override fun destinationBrokerId(): Int? = destinationBrokerId
    }

    private class TestContext(
        staticKeys: Map<String, Int>,
        dynamicKeys: Map<String, String>,
    ) {

        val time = MockTime()

        val handler: MockAdminApiHandler<String, Long>

        val driver: AdminApiDriver<String, Long>

        val future: SimpleAdminApiFuture<String, Long>

        init {
            val lookupScopes: MutableMap<String, MockRequestScope> = HashMap()
            staticKeys.forEach { (key, brokerId) ->
                val scope = MockRequestScope(brokerId, null)
                lookupScopes[key] = scope
            }
            dynamicKeys.forEach { (key, context) ->
                val scope = MockRequestScope(null, context)
                lookupScopes[key] = scope
            }
            val lookupStrategy = MockLookupStrategy(lookupScopes)
            handler = MockAdminApiHandler(lookupStrategy)
            future = AdminApiFuture.forKeys(lookupStrategy.lookupScopes.keys)
            driver = AdminApiDriver(
                handler = handler,
                future = future,
                deadlineMs = time.milliseconds() + API_TIMEOUT_MS,
                retryBackoffMs = RETRY_BACKOFF_MS.toLong(),
                logContext = LogContext(),
            )
            staticKeys.forEach { (key, brokerId) -> assertMappedKey(this, key, brokerId) }
            dynamicKeys.keys.forEach { key -> assertUnmappedKey(this, key) }
        }

        fun assertLookupResponse(
            requestSpec: RequestSpec<String>,
            result: LookupResult<String>,
        ) {
            requestSpec.keys.forEach { key -> assertUnmappedKey(this, key) }

            // The response is just a placeholder. The result is all we are interested in
            val response = MetadataResponse(MetadataResponseData(), ApiKeys.METADATA.latestVersion())
            driver.onResponse(time.milliseconds(), requestSpec, response, Node.noNode())
            result.mappedKeys.forEach { (key, brokerId) -> assertMappedKey(this, key, brokerId) }
            result.failedKeys.forEach { (key, exception) -> assertFailedKey(this, key, exception) }
        }

        fun assertResponse(
            requestSpec: RequestSpec<String>,
            result: ApiResult<String, Long>,
            node: Node,
        ) {
            val brokerId = requestSpec.scope.destinationBrokerId()
                ?: throw AssertionError("Fulfillment requests must specify a target brokerId")
            requestSpec.keys.forEach { key -> assertMappedKey(this, key, brokerId) }

            // The response is just a placeholder. The result is all we are interested in
            val response = MetadataResponse(MetadataResponseData(), ApiKeys.METADATA.latestVersion())
            driver.onResponse(time.milliseconds(), requestSpec, response, node)
            result.unmappedKeys.forEach { key -> assertUnmappedKey(this, key) }
            result.failedKeys.forEach { (key, exception) -> assertFailedKey(this, key, exception) }
            result.completedKeys.forEach { (key, value) -> assertCompletedKey(this, key, value) }
        }

        fun lookupStrategy(): MockLookupStrategy<String> = handler.lookupStrategy

        fun poll(
            expectedLookups: Map<Set<String>, LookupResult<String>>,
            expectedRequests: Map<Set<String>, ApiResult<String, Long>>,
        ) {
            if (expectedLookups.isNotEmpty()) {
                val lookupStrategy = lookupStrategy()
                lookupStrategy.reset()
                expectedLookups.forEach { (keys, result) -> lookupStrategy.expectLookup(keys, result) }
            }
            handler.reset()
            expectedRequests.forEach { (keys, result) -> handler.expectRequest(keys, result) }
            val requestSpecs = driver.poll()
            assertEquals(
                expected = expectedLookups.size + expectedRequests.size,
                actual = requestSpecs.size,
                message = "Driver generated an unexpected number of requests",
            )
            for (requestSpec in requestSpecs) {
                val keys = requestSpec.keys
                if (expectedLookups.containsKey(keys)) {
                    val result = expectedLookups[keys]!!
                    assertLookupResponse(requestSpec, result)
                } else if (expectedRequests.containsKey(keys)) {
                    val result = expectedRequests[keys]!!
                    assertResponse(requestSpec, result, Node.noNode())
                } else fail("Unexpected request for keys $keys")
            }
        }

        companion object {

            fun staticMapped(staticKeys: Map<String, Int>): TestContext = TestContext(staticKeys, emptyMap())

            fun dynamicMapped(dynamicKeys: Map<String, String>): TestContext = TestContext(emptyMap(), dynamicKeys)
        }
    }

    private class MockLookupStrategy<K>(val lookupScopes: Map<K, MockRequestScope>) : AdminApiLookupStrategy<K> {

        private val expectedLookups: MutableMap<Set<K>, LookupResult<K>> = HashMap()

        override fun lookupScope(key: K): ApiRequestScope = lookupScopes[key]!!

        fun expectLookup(keys: Set<K>, result: LookupResult<K>) {
            expectedLookups[keys] = result
        }

        override fun buildRequest(keys: Set<K>): AbstractRequest.Builder<*> {
            // The request is just a placeholder in these tests
            assertTrue(expectedLookups.containsKey(keys), "Unexpected lookup request for keys $keys")
            return MetadataRequest.Builder(emptyList(), false)
        }

        override fun handleResponse(keys: Set<K>, response: AbstractResponse): LookupResult<K> {
            return expectedLookups[keys] ?: throw AssertionError("Unexpected fulfillment request for keys $keys")
        }

        fun reset() = expectedLookups.clear()
    }

    private class MockAdminApiHandler<K, V>(val lookupStrategy: MockLookupStrategy<K>) : Batched<K, V>() {

        private val expectedRequests: MutableMap<Set<K>, ApiResult<K, V>> = HashMap()

        override fun apiName(): String = "mock-api"

        override fun lookupStrategy(): AdminApiLookupStrategy<K> = lookupStrategy

        fun expectRequest(keys: Set<K>, result: ApiResult<K, V>) {
            expectedRequests[keys] = result
        }

        override fun buildBatchedRequest(brokerId: Int, keys: Set<K>): AbstractRequest.Builder<*> {
            // The request is just a placeholder in these tests
            assertTrue(expectedRequests.containsKey(keys), "Unexpected fulfillment request for keys $keys")
            return MetadataRequest.Builder(emptyList(), false)
        }

        override fun handleResponse(broker: Node, keys: Set<K>, response: AbstractResponse): ApiResult<K, V> {
            return expectedRequests[keys]
                ?: throw AssertionError("Unexpected fulfillment request for keys $keys")
        }

        fun reset() = expectedRequests.clear()
    }

    companion object {

        private const val API_TIMEOUT_MS = 30000

        private const val RETRY_BACKOFF_MS = 100

        private fun assertMappedKey(
            context: TestContext,
            key: String,
            expectedBrokerId: Int,
        ) {
            val brokerIdOpt = context.driver.keyToBrokerId(key)
            assertEquals(expectedBrokerId, brokerIdOpt)
        }

        private fun assertUnmappedKey(context: TestContext, key: String) {
            val brokerIdOpt = context.driver.keyToBrokerId(key)
            assertEquals(null, brokerIdOpt)
            val future = context.future.all()[key]!!
            assertFalse(future.isDone())
        }

        private fun assertFailedKey(
            context: TestContext,
            key: String?,
            expectedException: Throwable,
        ) {
            val future = context.future.all()[key]!!
            assertTrue(future.isCompletedExceptionally)
            val exception = assertFailsWith<ExecutionException> { future.get() }
            assertEquals(expectedException, exception.cause)
        }

        private fun assertCompletedKey(
            context: TestContext,
            key: String?,
            expected: Long,
        ) {
            val future = context.future.all()[key]!!
            assertTrue(future.isDone())
            try {
                assertEquals(expected, future.get())
            } catch (t: Throwable) {
                throw RuntimeException(t)
            }
        }

        private fun completed(key: String, value: Long): ApiResult<String, Long> =
            ApiResult(mapOf(key to value), emptyMap(), emptyList())

        private fun failed(key: String, exception: Throwable): ApiResult<String, Long> =
            ApiResult(emptyMap(), mapOf(key to exception), emptyList())

        private fun unmapped(vararg keys: String): ApiResult<String, Long> =
            ApiResult(emptyMap(), emptyMap(), keys.toList())

        private fun completed(k1: String, v1: Long, k2: String, v2: Long): ApiResult<String, Long> =
            ApiResult(mapOf(k1 to v1, k2 to v2), emptyMap(), emptyList())

        private fun emptyFulfillment(): ApiResult<String, Long> =
            ApiResult(emptyMap(), emptyMap(), emptyList())

        private fun failedLookup(key: String, exception: Throwable): LookupResult<String> =
            LookupResult(failedKeys = mapOf(key to exception), mappedKeys = emptyMap())

        private fun emptyLookup(): LookupResult<String> =
            LookupResult(failedKeys = emptyMap(), mappedKeys = emptyMap())

        private fun mapped(key: String, brokerId: Int): LookupResult<String> {
            return LookupResult(failedKeys = emptyMap(), mappedKeys = mapOf(key to brokerId))
        }

        private fun mapped(
            k1: String,
            broker1: Int,
            k2: String,
            broker2: Int,
        ): LookupResult<String> =
            LookupResult(failedKeys = emptyMap(), mappedKeys = mapOf(k1 to broker1, k2 to broker2))
    }
}
