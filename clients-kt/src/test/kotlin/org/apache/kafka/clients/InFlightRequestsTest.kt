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

package org.apache.kafka.clients

import org.apache.kafka.clients.NetworkClient.InFlightRequest
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import kotlin.test.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class InFlightRequestsTest {
    
    private lateinit var inFlightRequests: InFlightRequests
    
    private var correlationId = 0
    
    private val dest = "dest"
    
    @BeforeEach
    fun setup() {
        inFlightRequests = InFlightRequests(maxInFlightRequestsPerConnection = 12)
        correlationId = 0
    }

    @Test
    fun testCompleteLastSent() {
        val correlationId1 = addRequest(dest)
        val correlationId2 = addRequest(dest)
        assertEquals(expected = 2, actual = inFlightRequests.count())
        assertEquals(
            expected = correlationId2,
            actual = inFlightRequests.completeLastSent(dest).header.correlationId,
        )
        assertEquals(expected = 1, actual = inFlightRequests.count())
        assertEquals(
            expected = correlationId1,
            actual = inFlightRequests.completeLastSent(dest).header.correlationId,
        )
        assertEquals(expected = 0, actual = inFlightRequests.count())
    }

    @Test
    fun testClearAll() {
        val correlationId1 = addRequest(dest)
        val correlationId2 = addRequest(dest)
        val clearedRequests = inFlightRequests.clearAll(dest).toList()
        assertEquals(expected = 0, actual = inFlightRequests.count())
        assertEquals(expected = 2, actual = clearedRequests.size)
        assertEquals(expected = correlationId1, actual = clearedRequests[0].header.correlationId)
        assertEquals(expected = correlationId2, actual = clearedRequests[1].header.correlationId)
    }

    @Test
    fun testTimedOutNodes() {
        val time: Time = MockTime()
        addRequest(
            destination = "A",
            sendTimeMs = time.milliseconds(),
            requestTimeoutMs = 50
        )
        addRequest(
            destination = "B",
            sendTimeMs = time.milliseconds(),
            requestTimeoutMs = 200
        )
        addRequest(
            destination = "B",
            sendTimeMs = time.milliseconds(),
            requestTimeoutMs = 100
        )
        time.sleep(50)
        assertEquals(
            expected = emptyList<Any>(),
            actual = inFlightRequests.nodesWithTimedOutRequests(time.milliseconds()),
        )
        time.sleep(25)
        assertEquals(
            expected = listOf("A"),
            actual = inFlightRequests.nodesWithTimedOutRequests(time.milliseconds()),
        )
        time.sleep(50)
        assertEquals(
            expected = mutableListOf("A", "B"),
            actual = inFlightRequests.nodesWithTimedOutRequests(time.milliseconds()),
        )
    }

    @Test
    fun testCompleteNext() {
        val correlationId1 = addRequest(dest)
        val correlationId2 = addRequest(dest)
        assertEquals(expected = 2, actual = inFlightRequests.count())
        assertEquals(
            expected = correlationId1,
            actual = inFlightRequests.completeNext(dest).header.correlationId
        )
        assertEquals(expected = 1, actual = inFlightRequests.count())
        assertEquals(
            expected = correlationId2,
            actual = inFlightRequests.completeNext(dest).header.correlationId,
        )
        assertEquals(expected = 0, actual = inFlightRequests.count())
    }

    @Test
    fun testCompleteNextThrowsIfNoInflights() {
        assertFailsWith<IllegalStateException> { inFlightRequests.completeNext(dest) }
    }

    @Test
    fun testCompleteLastSentThrowsIfNoInFlights() {
        assertFailsWith<IllegalStateException> { inFlightRequests.completeLastSent(dest) }
    }

    private fun addRequest(
        destination: String,
        sendTimeMs: Long = 0,
        requestTimeoutMs: Int = 10000,
    ): Int {
        val correlationId = correlationId
        this.correlationId += 1
        val requestHeader = RequestHeader(
            requestApiKey = ApiKeys.METADATA,
            requestVersion = 0.toShort(),
            clientId = "clientId",
            correlationId = correlationId,
        )
        val ifr = InFlightRequest(
            header = requestHeader,
            requestTimeoutMs = requestTimeoutMs.toLong(),
            createdTimeMs = 0,
            destination = destination,
            callback = null,
            expectResponse = false,
            isInternalRequest = false,
            request = null,
            send = null,
            sendTimeMs = sendTimeMs,
        )
        inFlightRequests.add(ifr)
        return correlationId
    }
}
