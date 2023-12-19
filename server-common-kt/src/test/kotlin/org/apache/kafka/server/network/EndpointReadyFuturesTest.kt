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

package org.apache.kafka.server.network

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class EndpointReadyFuturesTest {

    @Test
    fun testImmediateCompletion() {
        val readyFutures = EndpointReadyFutures.Builder().build(authorizer = null, info = INFO)
        assertEquals(setOf(EXTERNAL, INTERNAL), readyFutures.futures.keys)
        assertComplete(readyFutures, EXTERNAL, INTERNAL)
    }

    @Test
    fun testAddReadinessFuture() {
        val foo = CompletableFuture<Unit>()
        val readyFutures = EndpointReadyFutures.Builder()
            .addReadinessFuture("foo", foo)
            .build(authorizer = null, info = INFO)
        assertEquals(setOf(EXTERNAL, INTERNAL), readyFutures.futures.keys)
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL)
        foo.complete(Unit)
        assertComplete(readyFutures, EXTERNAL, INTERNAL)
    }

    @Test
    fun testAddMultipleReadinessFutures() {
        val foo = CompletableFuture<Unit>()
        val bar = CompletableFuture<Unit>()
        val readyFutures = EndpointReadyFutures.Builder()
                .addReadinessFuture("foo", foo)
                .addReadinessFuture("bar", bar)
                .build(authorizer = null, info = INFO)
        assertEquals(setOf(EXTERNAL, INTERNAL), readyFutures.futures.keys)
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL)
        foo.complete(Unit)
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL)
        bar.complete(Unit)
        assertComplete(readyFutures, EXTERNAL, INTERNAL)
    }

    @Test
    fun testAddReadinessFutures() {
        val bazFutures = mapOf(
            EXTERNAL to CompletableFuture<Unit>(),
            INTERNAL to CompletableFuture<Unit>(),
        )

        val readyFutures: EndpointReadyFutures = EndpointReadyFutures.Builder()
            .addReadinessFutures("baz", bazFutures)
            .build(authorizer = null, info = INFO)
        assertEquals(setOf(EXTERNAL, INTERNAL), readyFutures.futures.keys)
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL)
        bazFutures[EXTERNAL]!!.complete(Unit)
        assertComplete(readyFutures, EXTERNAL)
        assertIncomplete(readyFutures, INTERNAL)
        bazFutures[INTERNAL]!!.complete(Unit)
        assertComplete(readyFutures, EXTERNAL, INTERNAL)
    }

    @Test
    fun testFailedReadinessFuture() {
        val foo = CompletableFuture<Unit>()
        val bar = CompletableFuture<Unit>()
        val readyFutures = EndpointReadyFutures.Builder()
            .addReadinessFuture("foo", foo)
            .addReadinessFuture("bar", bar)
            .build(authorizer = null, info = INFO)
        assertEquals(setOf(EXTERNAL, INTERNAL), readyFutures.futures.keys)
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL)
        foo.complete(null)
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL)
        bar.completeExceptionally(RuntimeException("Failed."))
        assertException(
            readyFutures = readyFutures,
            throwable = RuntimeException("Failed."),
            endpoints = arrayOf(EXTERNAL, INTERNAL),
        )
    }

    companion object {

        private val EXTERNAL = Endpoint(
            listenerName = "EXTERNAL",
            securityProtocol = SecurityProtocol.SSL,
            host = "127.0.0.1",
            port = 9092,
        )

        private val INTERNAL = Endpoint(
            listenerName = "INTERNAL",
            securityProtocol = SecurityProtocol.PLAINTEXT,
            host = "127.0.0.1",
            port = 9093,
        )

        private val INFO = KafkaAuthorizerServerInfo(
            clusterResource = ClusterResource(clusterId = "S6-01LPiQOCBhhFIunQUcQ"),
            brokerId = 1,
            endpoints = listOf(EXTERNAL, INTERNAL),
            interbrokerEndpoint = INTERNAL,
            earlyStartListeners = listOf("INTERNAL"),
        )

        fun assertComplete(
            readyFutures: EndpointReadyFutures,
            vararg endpoints: Endpoint,
        ) {
            for (endpoint in endpoints) {
                val name = endpoint.listenerName!!
                val future = readyFutures.futures[endpoint]
                assertNotNull(future, "Unable to find future for $name")
                assertTrue(future.isDone, "Future for $name is not done.")
                assertFalse(future.isCompletedExceptionally(), "Future for $name is completed exceptionally.")
            }
        }

        fun assertIncomplete(
            readyFutures: EndpointReadyFutures,
            vararg endpoints: Endpoint,
        ) {
            for (endpoint in endpoints) {
                val future = readyFutures.futures[endpoint]
                assertNotNull(future, "Unable to find future for $endpoint")
                assertFalse(future.isDone, "Future for $endpoint is done.")
            }
        }

        fun assertException(
            readyFutures: EndpointReadyFutures,
            throwable: Throwable,
            vararg endpoints: Endpoint,
        ) {
            for (endpoint in endpoints) {
                val future = readyFutures.futures[endpoint]
                assertNotNull(future, "Unable to find future for $endpoint")
                assertTrue(
                    future.isCompletedExceptionally(),
                    "Future for $endpoint is not completed exceptionally."
                )
                val cause = assertFailsWith<CompletionException> { future.getNow(null) }.cause
                assertNotNull(cause, "Unable to find CompletionException cause for $endpoint")
                assertEquals(throwable.javaClass, cause.javaClass)
                assertEquals(throwable.message, cause.message)
            }
        }
    }
}
