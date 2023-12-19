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

package org.apache.kafka.server.utils

import java.io.IOException
import java.util.ArrayDeque
import java.util.Queue
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.ClientRequest
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.RequestCompletionHandler
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.InterBrokerSendThread
import org.apache.kafka.server.util.RequestAndCompletionHandler
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InterBrokerSendThreadTest {
    
    private val time: Time = MockTime()

    private val networkClient = mock<KafkaClient>()

    private val completionHandler = StubCompletionHandler()

    private val requestTimeoutMs = 1000

    @Test
    @Throws(InterruptedException::class, IOException::class)
    fun testShutdownThreadShouldNotCauseException() {
        // InterBrokerSendThread#shutdown calls NetworkClient#initiateClose first so NetworkClient#poll
        // can throw DisconnectException when thread is running
        whenever(networkClient.poll(any(), any())).thenThrow(DisconnectException())
        whenever(networkClient.active()).thenReturn(false)
        val exception = AtomicReference<Throwable>()
        val thread = TestInterBrokerSendThread(networkClient) { newValue -> exception.getAndSet(newValue) }
        thread.shutdown()
        thread.pollOnce(100)
        verify(networkClient).poll(any(), any())
        verify(networkClient).initiateClose()
        verify(networkClient).close()
        verify(networkClient).active()
        verifyNoMoreInteractions(networkClient)
        assertNull(exception.get())
    }

    @Test
    fun testDisconnectWithoutShutdownShouldCauseException() {
        val de = DisconnectException()
        whenever(networkClient.poll(any(), any())).thenThrow(de)
        whenever(networkClient.active()).thenReturn(true)
        val throwable = AtomicReference<Throwable>()
        val thread = TestInterBrokerSendThread(networkClient) { newValue -> throwable.getAndSet(newValue) }
        thread.pollOnce(100)
        verify(networkClient).poll(any(), any())
        verify(networkClient).active()
        verifyNoMoreInteractions(networkClient)
        val thrown = throwable.get()
        assertNotNull(thrown)
        assertIs<FatalExitError>(thrown)
    }

    @Test
    fun testShouldNotSendAnythingWhenNoRequests() {
        val sendThread = TestInterBrokerSendThread()

        // poll is always called but there should be no further invocations on NetworkClient
        whenever(networkClient.poll(any(), any())).thenReturn(emptyList())
        sendThread.doWork()
        verify(networkClient).poll(any(), any())
        verifyNoMoreInteractions(networkClient)
        assertFalse(completionHandler.executedWithDisconnectedResponse)
    }

    @Test
    fun testShouldCreateClientRequestAndSendWhenNodeIsReady() {
        val request = StubRequestBuilder<AbstractRequest>()
        val node = Node(id = 1, host = "", port = 8080)
        val handler = RequestAndCompletionHandler(
            creationTimeMs = time.milliseconds(),
            destination = node,
            request = request,
            handler = completionHandler,
        )
        val sendThread = TestInterBrokerSendThread()
        val clientRequest = ClientRequest(
            destination = "dest",
            requestBuilder = request,
            correlationId = 0,
            clientId = "1",
            createdTimeMs = 0,
            expectResponse = true,
            requestTimeoutMs = requestTimeoutMs,
            callback = handler.handler,
        )
        whenever(
            networkClient.newClientRequest(
                nodeId = eq("1"),
                requestBuilder = same(handler.request),
                createdTimeMs = any(),
                expectResponse = eq(true),
                requestTimeoutMs = eq(requestTimeoutMs),
                callback = same(handler.handler),
            )
        ).thenReturn(clientRequest)
        whenever(networkClient.ready(node, time.milliseconds())).thenReturn(true)
        whenever(networkClient.poll(any(), any())).thenReturn(emptyList())
        sendThread.enqueue(handler)
        sendThread.doWork()
        verify(networkClient).newClientRequest(
            nodeId = eq("1"),
            requestBuilder = same(handler.request),
            createdTimeMs = any(),
            expectResponse = eq(true),
            requestTimeoutMs = eq(requestTimeoutMs),
            callback = same(handler.handler),
        )
        verify(networkClient).ready(any(), any())
        verify(networkClient).send(same(clientRequest), any())
        verify(networkClient).poll(any(), any())
        verifyNoMoreInteractions(networkClient)
        assertFalse(completionHandler.executedWithDisconnectedResponse)
    }

    @Test
    fun testShouldCallCompletionHandlerWithDisconnectedResponseWhenNodeNotReady() {
        val request = StubRequestBuilder<AbstractRequest>()
        val node = Node(id = 1, host = "", port = 8080)
        val handler = RequestAndCompletionHandler(
            creationTimeMs = time.milliseconds(),
            destination = node,
            request = request,
            handler = completionHandler,
        )
        val sendThread = TestInterBrokerSendThread()
        val clientRequest = ClientRequest(
            destination = "dest",
            requestBuilder = request,
            correlationId = 0,
            clientId = "1",
            createdTimeMs = 0,
            expectResponse = true,
            requestTimeoutMs = requestTimeoutMs,
            callback = handler.handler,
        )
        whenever(
            networkClient.newClientRequest(
                nodeId = eq("1"),
                requestBuilder = same(handler.request),
                createdTimeMs = any(),
                expectResponse = eq(true),
                requestTimeoutMs = eq(requestTimeoutMs),
                callback = same(handler.handler),
            )
        ).thenReturn(clientRequest)
        whenever(networkClient.ready(node, time.milliseconds())).thenReturn(false)
        whenever(networkClient.connectionDelay(any(), any())).thenReturn(0L)
        whenever(networkClient.poll(any(), any())).thenReturn(emptyList())
        whenever(networkClient.connectionFailed(node)).thenReturn(true)
        whenever(networkClient.authenticationException(node)).thenReturn(AuthenticationException(""))
        sendThread.enqueue(handler)
        sendThread.doWork()
        verify(networkClient).newClientRequest(
            nodeId = eq("1"),
            requestBuilder = same(handler.request),
            createdTimeMs = any(),
            expectResponse = eq(true),
            requestTimeoutMs = eq(requestTimeoutMs),
            callback = same(handler.handler)
        )
        verify(networkClient).ready(any(), any())
        verify(networkClient).connectionDelay(any(), any())
        verify(networkClient).poll(any(), any())
        verify(networkClient).connectionFailed(any())
        verify(networkClient).authenticationException(any())
        verifyNoMoreInteractions(networkClient)
        assertTrue(completionHandler.executedWithDisconnectedResponse)
    }

    @Test
    fun testFailingExpiredRequests() {
        val request = StubRequestBuilder<AbstractRequest>()
        val node = Node(id = 1, host = "", port = 8080)
        val handler = RequestAndCompletionHandler(
            creationTimeMs = time.milliseconds(),
            destination = node,
            request = request,
            handler = completionHandler,
        )
        val sendThread = TestInterBrokerSendThread()
        val clientRequest = ClientRequest(
            destination = "dest",
            requestBuilder = request,
            correlationId = 0,
            clientId = "1",
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
            requestTimeoutMs = requestTimeoutMs,
            callback = handler.handler,
        )
        time.sleep(1500L)
        whenever(
            networkClient.newClientRequest(
                nodeId = eq("1"),
                requestBuilder = same(handler.request),
                createdTimeMs = eq(handler.creationTimeMs),
                expectResponse = eq(true),
                requestTimeoutMs = eq(requestTimeoutMs),
                callback = same(handler.handler),
            )
        ).thenReturn(clientRequest)

        // make the node unready so the request is not cleared
        whenever(networkClient.ready(node, time.milliseconds())).thenReturn(false)
        whenever(networkClient.connectionDelay(any(), any())).thenReturn(0L)
        whenever(networkClient.poll(any(), any())).thenReturn(emptyList())

        // rule out disconnects so the request stays for the expiry check
        whenever(networkClient.connectionFailed(node)).thenReturn(false)
        sendThread.enqueue(handler)
        sendThread.doWork()
        verify(networkClient).newClientRequest(
            nodeId = eq("1"),
            requestBuilder = same(handler.request),
            createdTimeMs = eq(handler.creationTimeMs),
            expectResponse = eq(true),
            requestTimeoutMs = eq(requestTimeoutMs),
            callback = same(handler.handler),
        )
        verify(networkClient).ready(any(), any())
        verify(networkClient).connectionDelay(any(), any())
        verify(networkClient).poll(any(), any())
        verify(networkClient).connectionFailed(any())
        verifyNoMoreInteractions(networkClient)
        assertFalse(sendThread.hasUnsentRequests())
        assertTrue(completionHandler.executedWithDisconnectedResponse)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(InterruptedException::class, IOException::class)
    fun testInterruption(isShuttingDown: Boolean) {
        val interrupted: Exception = InterruptedException()

        // InterBrokerSendThread#shutdown calls NetworkClient#initiateClose first so NetworkClient#poll
        // can throw InterruptedException if a callback request that throws it is handled
        whenever(networkClient.poll(any(), any())).thenAnswer { throw interrupted }
        val exception = AtomicReference<Throwable>()
        val thread: InterBrokerSendThread = TestInterBrokerSendThread(networkClient) {
            if (isShuttingDown) assertIs<InterruptedException>(it)
            else assertIs<FatalExitError>(it)
            exception.getAndSet(it)
        }
        if (isShuttingDown) thread.shutdown()
        thread.pollOnce(100)
        verify(networkClient).poll(any(), any())
        if (isShuttingDown) {
            verify(networkClient).initiateClose()
            verify(networkClient).close()
        }
        verifyNoMoreInteractions(networkClient)
        assertNotNull(exception.get())
    }

    private class StubRequestBuilder<T : AbstractRequest> : AbstractRequest.Builder<T>(ApiKeys.END_TXN) {
        override fun build(version: Short): T = error("Cannot build null request")
    }

    private class StubCompletionHandler : RequestCompletionHandler {

        var executedWithDisconnectedResponse = false

        var response: ClientResponse? = null

        override fun onComplete(response: ClientResponse) {
            executedWithDisconnectedResponse = response.disconnected
            this.response = response
        }
    }

    internal inner class TestInterBrokerSendThread(
        networkClient: KafkaClient = this@InterBrokerSendThreadTest.networkClient,
        private val exceptionCallback: (Throwable) -> Unit = { throwable ->
            throw if (throwable is RuntimeException) throwable
            else RuntimeException(throwable)
        },
    ) : InterBrokerSendThread(
        name = "name",
        networkClient = networkClient,
        requestTimeoutMs = requestTimeoutMs,
        time = time,
    ) {

        private val queue: Queue<RequestAndCompletionHandler> = ArrayDeque()

        fun enqueue(request: RequestAndCompletionHandler) = queue.offer(request)

        override fun generateRequests(): Collection<RequestAndCompletionHandler> {
            return if (queue.isEmpty()) emptyList() else listOf(queue.poll())
        }

        override fun pollOnce(maxTimeoutMs: Long) {
            try {
                super.pollOnce(maxTimeoutMs)
            } catch (t: Throwable) {
                exceptionCallback(t)
            }
        }
    }
}
