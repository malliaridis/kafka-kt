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

package org.apache.kafka.clients.consumer.internals

import java.util.Properties
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class NetworkClientDelegateTest {

    private lateinit var time: MockTime

    private lateinit  var client: MockClient

    @BeforeEach
    fun setup() {
        time = MockTime(autoTickMs = 0)
        client = MockClient(time = time, staticNodes = listOf(mockNode()))
    }

    @Test
    @Throws(Exception::class)
    fun testSuccessfulResponse() {
        newNetworkClientDelegate().use { ncd ->
            val unsentRequest = newUnsentFindCoordinatorRequest()
            prepareFindCoordinatorResponse(Errors.NONE)
            
            ncd.send(unsentRequest)
            ncd.poll(0, time.milliseconds())
            
            assertTrue(unsentRequest.future.isDone)
            assertNotNull(unsentRequest.future.get())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTimeoutBeforeSend() {
        newNetworkClientDelegate().use { ncd ->
            client.setUnreachable(mockNode(), REQUEST_TIMEOUT_MS.toLong())
            val unsentRequest = newUnsentFindCoordinatorRequest()
            
            ncd.send(unsentRequest)
            ncd.poll(0, time.milliseconds())
            
            time.sleep(REQUEST_TIMEOUT_MS.toLong())
            ncd.poll(0, time.milliseconds())
            assertTrue(unsentRequest.future.isDone)
            assertFutureThrows(
                unsentRequest.future,
                TimeoutException::class.java
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTimeoutAfterSend() {
        newNetworkClientDelegate().use { ncd ->
            val unsentRequest = newUnsentFindCoordinatorRequest()
            ncd.send(unsentRequest)
            ncd.poll(0, time.milliseconds())
            time.sleep(REQUEST_TIMEOUT_MS.toLong())
            ncd.poll(0, time.milliseconds())
            assertTrue(unsentRequest.future.isDone)
            assertFutureThrows<DisconnectException>(unsentRequest.future)
        }
    }

    fun newNetworkClientDelegate(): NetworkClientDelegate {
        val logContext = LogContext()
        val properties = Properties()
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.GROUP_ID_CONFIG] = GROUP_ID
        properties[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = REQUEST_TIMEOUT_MS
        return NetworkClientDelegate(
            time = time,
            config = ConsumerConfig(properties),
            logContext = logContext,
            client = client,
        )
    }

    fun newUnsentFindCoordinatorRequest(): UnsentRequest {
        return UnsentRequest(
            requestBuilder = FindCoordinatorRequest.Builder(
                FindCoordinatorRequestData()
                    .setKey(GROUP_ID)
                    .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
            ),
            node = null,
        ).apply { setTimer(time, REQUEST_TIMEOUT_MS.toLong()) }
    }

    fun prepareFindCoordinatorResponse(error: Errors) {
        val findCoordinatorResponse =
            FindCoordinatorResponse.prepareResponse(error, GROUP_ID, mockNode())
        client.prepareResponse(findCoordinatorResponse)
    }

    private fun mockNode(): Node = Node(id = 0, host = "localhost", port = 99)

    companion object {
        
        private const val REQUEST_TIMEOUT_MS = 5000
        
        private const val GROUP_ID = "group"
    }
}

