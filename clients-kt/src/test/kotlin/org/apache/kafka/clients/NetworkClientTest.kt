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

import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.NetworkClientUtils.isReady
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection
import org.apache.kafka.common.message.ProduceResponseData
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.RequestTestUtils.serializeResponseWithHeader
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.DelayedReceive
import org.apache.kafka.test.MockSelector
import org.apache.kafka.test.TestUtils
import org.apache.kafka.test.TestUtils.singletonCluster
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class NetworkClientTest {

    private val defaultRequestTimeoutMs = 1000

    private val time = MockTime()

    private val selector = MockSelector(time)

    private val node = singletonCluster().nodes.first()

    private val reconnectBackoffMsTest = 10 * 1000L

    private val reconnectBackoffMaxMsTest = 10 * 10000L

    private val connectionSetupTimeoutMsTest = 5 * 1000L

    private val connectionSetupTimeoutMaxMsTest = 127 * 1000L

    private val reconnectBackoffExpBase = ClusterConnectionStates.RECONNECT_BACKOFF_EXP_BASE

    private val reconnectBackoffJitter = ClusterConnectionStates.RECONNECT_BACKOFF_JITTER

    private val metadataUpdater = TestMetadataUpdater(listOf(node))

    private val client = createNetworkClient(reconnectBackoffMaxMsTest)

    private val clientWithNoExponentialBackoff = createNetworkClient(reconnectBackoffMsTest)

    private val clientWithStaticNodes = createNetworkClientWithStaticNodes()

    private val clientWithNoVersionDiscovery = createNetworkClientWithNoVersionDiscovery()

    private fun createNetworkClient(reconnectBackoffMaxMs: Long): NetworkClient {
        return NetworkClient(
            selector = selector,
            metadataUpdater = metadataUpdater,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = reconnectBackoffMaxMs,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = true,
            apiVersions = ApiVersions(),
            logContext = LogContext(),
        )
    }

    private fun createNetworkClientWithMultipleNodes(
        reconnectBackoffMaxMs: Long,
        connectionSetupTimeoutMsTest: Long,
        nodeNumber: Int,
    ): NetworkClient {
        val nodes = TestUtils.clusterWith(nodeNumber).nodes
        val metadataUpdater = TestMetadataUpdater(nodes)
        return NetworkClient(
            selector = selector,
            metadataUpdater = metadataUpdater,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = reconnectBackoffMaxMs,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = true,
            apiVersions = ApiVersions(),
            logContext = LogContext(),
        )
    }

    private fun createNetworkClientWithStaticNodes(): NetworkClient {
        return NetworkClient(
            selector = selector,
            metadataUpdater = metadataUpdater,
            clientId = "mock-static",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = 0,
            reconnectBackoffMax = 0,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = true,
            apiVersions = ApiVersions(),
            logContext = LogContext(),
        )
    }

    private fun createNetworkClientWithNoVersionDiscovery(metadata: Metadata): NetworkClient {
        return NetworkClient(
            selector = selector,
            metadata = metadata,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = 0,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = false,
            apiVersions = ApiVersions(),
            logContext = LogContext(),
        )
    }

    private fun createNetworkClientWithNoVersionDiscovery(): NetworkClient {
        return NetworkClient(
            selector = selector,
            metadataUpdater = metadataUpdater,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = reconnectBackoffMaxMsTest,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = false,
            apiVersions = ApiVersions(),
            logContext = LogContext()
        )
    }

    @BeforeEach
    fun setup() {
        selector.reset()
    }

    @Test
    fun testSendToUnreadyNode() {
        val builder = MetadataRequest.Builder(
            topics = listOf("test"),
            allowAutoTopicCreation = true,
        )
        val now = time.milliseconds()
        val request = client.newClientRequest(
            nodeId = "5",
            requestBuilder = builder,
            createdTimeMs = now,
            expectResponse = false,
        )
        assertFailsWith<IllegalStateException> { client.send(request, now) }
    }

    @Test
    fun testSimpleRequestResponse() {
        checkSimpleRequestResponse(client)
    }

    @Test
    fun testSimpleRequestResponseWithStaticNodes() {
        checkSimpleRequestResponse(clientWithStaticNodes)
    }

    @Test
    fun testSimpleRequestResponseWithNoBrokerDiscovery() {
        checkSimpleRequestResponse(clientWithNoVersionDiscovery)
    }

    @Test
    fun testDnsLookupFailure() {
        /* Fail cleanly when the node has a bad hostname */
        assertFalse(
            client.ready(
                node = Node(id = 1234, host = "badhost", port = 1234),
                now = time.milliseconds(),
            )
        )
    }

    @Test
    fun testClose() {
        client.ready(node, time.milliseconds())
        awaitReady(client, node)
        client.poll(timeout = 1, now = time.milliseconds())
        assertTrue(client.isReady(node, time.milliseconds()), "The client should be ready")
        val builder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(TopicProduceDataCollection())
                .setAcks(1.toShort())
                .setTimeoutMs(1000)
        )
        val request = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
        )
        client.send(request, time.milliseconds())
        assertEquals(
            expected = 1,
            actual = client.inFlightRequestCount(node.idString()),
            message = "There should be 1 in-flight request after send",
        )
        assertTrue(client.hasInFlightRequests(node.idString()))
        assertTrue(client.hasInFlightRequests())
        client.close(node.idString())
        assertEquals(
            expected = 0,
            actual = client.inFlightRequestCount(node.idString()),
            message = "There should be no in-flight request after close",
        )
        assertFalse(client.hasInFlightRequests(node.idString()))
        assertFalse(client.hasInFlightRequests())
        assertFalse(
            actual = client.isReady(node, 0),
            message = "Connection should not be ready after close",
        )
    }

    @Test
    fun testUnsupportedVersionDuringInternalMetadataRequest() {
        val topics = listOf("topic_1")

        // disabling auto topic creation for versions less than 4 is not supported
        val builder = MetadataRequest.Builder(topics, false, 3.toShort())
        client.sendInternalMetadataRequest(builder, node.idString(), time.milliseconds())
        assertIs<UnsupportedVersionException>(metadataUpdater.andClearFailure)
    }

    private fun checkSimpleRequestResponse(networkClient: NetworkClient) {
        // has to be before creating any request, as it may send ApiVersionsRequest and its response
        // is mocked with correlation id 0
        awaitReady(networkClient, node)
        val requestVersion = ApiKeys.PRODUCE.latestVersion()
        val builder = ProduceRequest.Builder(
            requestVersion,
            requestVersion,
            ProduceRequestData()
                .setAcks(1.toShort())
                .setTimeoutMs(1000)
        )
        val handler = TestCallbackHandler()
        val request = networkClient.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
            requestTimeoutMs = defaultRequestTimeoutMs,
            callback = handler,
        )
        networkClient.send(request, time.milliseconds())
        networkClient.poll(timeout = 1, now = time.milliseconds())
        assertEquals(expected = 1, actual = networkClient.inFlightRequestCount())
        val produceResponse = ProduceResponse(ProduceResponseData())
        val buffer = serializeResponseWithHeader(
            response = produceResponse,
            version = requestVersion,
            correlationId = request.correlationId,
        )
        selector.completeReceive(NetworkReceive(source = node.idString(), buffer = buffer))
        val responses = networkClient.poll(timeout = 1, now = time.milliseconds())
        assertEquals(expected = 1, actual = responses.size)
        assertTrue(handler.executed, "The handler should have executed.")
        assertTrue(handler.response!!.hasResponse, "Should have a response body.")
        assertEquals(
            expected = request.correlationId,
            actual = handler.response!!.requestHeader.correlationId,
            message = "Should be correlated to the original request",
        )
    }

    private fun delayedApiVersionsResponse(
        correlationId: Int,
        version: Short,
        response: ApiVersionsResponse,
    ) {
        val buffer = serializeResponseWithHeader(response, version, correlationId)
        selector.delayedReceive(
            DelayedReceive(
                source = node.idString(),
                receive = NetworkReceive(source = node.idString(), buffer = buffer),
            )
        )
    }

    private fun setExpectedApiVersionsResponse(response: ApiVersionsResponse) {
        val apiVersionsResponseVersion = response.apiVersion(ApiKeys.API_VERSIONS.id)!!.maxVersion
        delayedApiVersionsResponse(
            correlationId = 0,
            version = apiVersionsResponseVersion,
            response = response,
        )
    }

    private fun awaitReady(client: NetworkClient, node: Node) {
        if (client.discoverBrokerVersions) {
            setExpectedApiVersionsResponse(
                TestUtils.defaultApiVersionsResponse(
                    listenerType = ApiMessageType.ListenerType.ZK_BROKER,
                )
            )
        }
        while (!client.ready(node, time.milliseconds()))
            client.poll(timeout = 1, now = time.milliseconds())
        selector.clear()
    }

    @Test
    fun testInvalidApiVersionsRequest() {
        // initiate the connection
        client.ready(node, time.milliseconds())

        // handle the connection, send the ApiVersionsRequest
        client.poll(timeout = 0, now = time.milliseconds())

        // check that the ApiVersionsRequest has been initiated
        assertTrue(client.hasInFlightRequests(node.idString()))

        // prepare response
        delayedApiVersionsResponse(
            correlationId = 0,
            version = ApiKeys.API_VERSIONS.latestVersion(),
            response = ApiVersionsResponse(
                ApiVersionsResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code)
                    .setThrottleTimeMs(0)
            ),
        )

        // handle completed receives
        client.poll(timeout = 0, now = time.milliseconds())

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()))

        // various assertions
        assertFalse(client.isReady(node, time.milliseconds()))
    }

    @Test
    fun testApiVersionsRequest() {
        // initiate the connection
        client.ready(node, time.milliseconds())

        // handle the connection, send the ApiVersionsRequest
        client.poll(timeout = 0, now = time.milliseconds())

        // check that the ApiVersionsRequest has been initiated
        assertTrue(client.hasInFlightRequests(node.idString()))

        // prepare response
        delayedApiVersionsResponse(
            correlationId = 0,
            version = ApiKeys.API_VERSIONS.latestVersion(),
            response = defaultApiVersionsResponse(),
        )

        // handle completed receives
        client.poll(timeout = 0, now = time.milliseconds())

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()))

        // various assertions
        assertTrue(client.isReady(node, time.milliseconds()))
    }

    @Test
    fun testUnsupportedApiVersionsRequestWithVersionProvidedByTheBroker() {
        // initiate the connection
        client.ready(node, time.milliseconds())

        // handle the connection, initiate first ApiVersionsRequest
        client.poll(timeout = 0, now = time.milliseconds())

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()))

        // completes initiated sends
        client.poll(timeout = 0, now = time.milliseconds())
        assertEquals(expected = 1, actual = selector.completedSends().size)
        var buffer = selector.completedSendBuffers()[0].buffer()
        var header = parseHeader(buffer)
        assertEquals(ApiKeys.API_VERSIONS, header.apiKey)
        assertEquals(expected = 3, actual = header.apiVersion)

        // prepare response
        val apiKeys = ApiVersionCollection()
        apiKeys.add(
            ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(0.toShort())
                .setMaxVersion(2.toShort())
        )
        delayedApiVersionsResponse(
            correlationId = 0,
            version = 0,
            response = ApiVersionsResponse(
                ApiVersionsResponseData()
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
                    .setApiKeys(apiKeys)
            ),
        )

        // handle ApiVersionResponse, initiate second ApiVersionRequest
        client.poll(timeout = 0, now = time.milliseconds())

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()))

        // ApiVersionsResponse has been received
        assertEquals(expected = 1, actual = selector.completedReceives().size)

        // clean up the buffers
        selector.completedSends.clear()
        selector.completedSendBuffers.clear()
        selector.completedReceives.clear()

        // completes initiated sends
        client.poll(timeout = 0, now = time.milliseconds())

        // ApiVersionsRequest has been sent
        assertEquals(expected = 1, actual = selector.completedSends().size)
        buffer = selector.completedSendBuffers()[0].buffer()
        header = parseHeader(buffer)
        assertEquals(expected = ApiKeys.API_VERSIONS, actual = header.apiKey)
        assertEquals(expected = 2, actual = header.apiVersion.toInt())

        // prepare response
        delayedApiVersionsResponse(
            correlationId = 1,
            version = 0,
            response = defaultApiVersionsResponse(),
        )

        // handle completed receives
        client.poll(timeout = 0, now = time.milliseconds())

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()))
        assertEquals(expected = 1, actual = selector.completedReceives().size)

        // the client is ready
        assertTrue(client.isReady(node, time.milliseconds()))
    }

    @Test
    fun testUnsupportedApiVersionsRequestWithoutVersionProvidedByTheBroker() {
        // initiate the connection
        client.ready(node, time.milliseconds())

        // handle the connection, initiate first ApiVersionsRequest
        client.poll(timeout = 0, now = time.milliseconds())

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()))

        // completes initiated sends
        client.poll(timeout = 0, now = time.milliseconds())
        assertEquals(expected = 1, actual = selector.completedSends().size)
        var buffer = selector.completedSendBuffers()[0].buffer()
        var header = parseHeader(buffer)
        assertEquals(expected = ApiKeys.API_VERSIONS, actual = header.apiKey)
        assertEquals(expected = 3, actual = header.apiVersion.toInt())

        // prepare response
        delayedApiVersionsResponse(
            correlationId = 0,
            version = 0,
            response = ApiVersionsResponse(
                ApiVersionsResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
            ),
        )

        // handle ApiVersionResponse, initiate second ApiVersionRequest
        client.poll(timeout = 0, now = time.milliseconds())

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()))

        // ApiVersionsResponse has been received
        assertEquals(expected = 1, actual = selector.completedReceives().size)

        // clean up the buffers
        selector.completedSends.clear()
        selector.completedSendBuffers.clear()
        selector.completedReceives.clear()

        // completes initiated sends
        client.poll(timeout = 0, now = time.milliseconds())

        // ApiVersionsRequest has been sent
        assertEquals(expected = 1, actual = selector.completedSends().size)
        buffer = selector.completedSendBuffers()[0].buffer()
        header = parseHeader(buffer)
        assertEquals(ApiKeys.API_VERSIONS, header.apiKey)
        assertEquals(expected = 0, actual = header.apiVersion.toInt())

        // prepare response
        delayedApiVersionsResponse(
            correlationId = 1,
            version = 0,
            response = defaultApiVersionsResponse()
        )

        // handle completed receives
        client.poll(timeout = 0, now = time.milliseconds())

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()))
        assertEquals(expected = 1, actual = selector.completedReceives().size)

        // the client is ready
        assertTrue(client.isReady(node, time.milliseconds()))
    }

    @Test
    fun testRequestTimeout() {
        testRequestTimeout(defaultRequestTimeoutMs + 5000)
    }
    
    @Test
    fun testDefaultRequestTimeout() {
        testRequestTimeout(defaultRequestTimeoutMs)
    }

    /**
     * This is a helper method that will execute two produce calls. The first call is expected to work and the
     * second produce call is intentionally made to emulate a request timeout. In the case that a timeout occurs
     * during a request, we want to ensure that we [Metadata.requestUpdate] request a metadata update} so that
     * on a subsequent invocation of [poll][NetworkClient.poll], the metadata request will be sent.
     *
     * The [MetadataUpdater] has a specific method to handle
     * [server disconnects][NetworkClient.DefaultMetadataUpdater.handleServerDisconnect]
     * which is where we [request a metadata update][Metadata.requestUpdate]. This test helper method ensures
     * that is invoked by checking [Metadata.updateRequested] after the simulated timeout.
     *
     * @param requestTimeoutMs Timeout in ms
     */
    private fun testRequestTimeout(requestTimeoutMs: Int) {
        val metadata = Metadata(
            refreshBackoffMs = 50,
            metadataExpireMs = 5000,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        )
        val metadataResponse = metadataUpdateWith(
            numNodes = 2,
            topicPartitionCounts = emptyMap(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )

        val client = createNetworkClientWithNoVersionDiscovery(metadata)

        // Send first produce without any timeout.

        // Send first produce without any timeout.
        var clientResponse: ClientResponse = produce(
            client = client,
            requestTimeoutMs = requestTimeoutMs,
            shouldEmulateTimeout = false,
        )
        assertEquals(node.idString(), clientResponse.destination)
        assertFalse(clientResponse.disconnected, "Expected response to succeed and not disconnect")
        assertFalse(clientResponse.timedOut, "Expected response to succeed and not time out")
        assertFalse(metadata.updateRequested(), "Expected NetworkClient to not need to update metadata")

        // Send second request, but emulate a timeout.

        // Send second request, but emulate a timeout.
        clientResponse = produce(
            client = client,
            requestTimeoutMs = requestTimeoutMs,
            shouldEmulateTimeout = true,
        )
        assertEquals(node.idString(), clientResponse.destination)
        assertTrue(clientResponse.disconnected, "Expected response to fail due to disconnection")
        assertTrue(clientResponse.timedOut, "Expected response to fail due to timeout")
        assertTrue(
            actual = metadata.updateRequested(),
            message = "Expected NetworkClient to have called requestUpdate on metadata on timeout",
        )
    }
    
    private fun produce(client: NetworkClient, requestTimeoutMs: Int, shouldEmulateTimeout: Boolean): ClientResponse {
        // has to be before creating any request, as it may send ApiVersionsRequest and its response
        // is mocked with correlation id 0
        awaitReady(client, node)
        val builder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(TopicProduceDataCollection())
                .setAcks(1)
                .setTimeoutMs(1000)
        )
        val handler = TestCallbackHandler()
        val request = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
            requestTimeoutMs = requestTimeoutMs,
            callback = handler,
        )
        client.send(request, time.milliseconds())

        if (shouldEmulateTimeout) {
            // For a delay of slightly more than our timeout threshold to emulate the request timing out.
            time.sleep((requestTimeoutMs + 1).toLong())
        } else {
            val produceResponse = ProduceResponse(ProduceResponseData())
            val buffer = serializeResponseWithHeader(
                response = produceResponse,
                version = ApiKeys.PRODUCE.latestVersion(),
                correlationId = request.correlationId,
            )
            selector.completeReceive(NetworkReceive(source = node.idString(), buffer = buffer))
        }

        val responses = client.poll(0, time.milliseconds())
        assertEquals(1, responses.size)
        return responses[0]
    }

    @Test
    fun testConnectionSetupTimeout() {
        // Use two nodes to ensure that the logic iterate over a set of more than one
        // element. ConcurrentModificationException is not triggered otherwise.
        val cluster = TestUtils.clusterWith(2)
        val node0 = cluster.nodeById(0)!!
        val node1 = cluster.nodeById(1)!!
        client.ready(node0, time.milliseconds())
        selector.serverConnectionBlocked(node0.idString())
        client.ready(node1, time.milliseconds())
        selector.serverConnectionBlocked(node1.idString())
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(
            actual = client.connectionFailed(node),
            message = "The connections should not fail before the socket connection setup timeout elapsed",
        )
        time.sleep((connectionSetupTimeoutMsTest * 1.2).toLong() + 1)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(
            actual = client.connectionFailed(node),
            message = "Expected the connections to fail due to the socket connection setup timeout",
        )
    }

    @Test
    fun testConnectionThrottling() {
        // Instrument the test to return a response with a 100ms throttle delay.
        awaitReady(client, node)
        val requestVersion = ApiKeys.PRODUCE.latestVersion()
        val builder = ProduceRequest.Builder(
            minVersion = requestVersion,
            maxVersion = requestVersion,
            data = ProduceRequestData()
                .setAcks(1.toShort())
                .setTimeoutMs(1000),
        )
        val handler = TestCallbackHandler()
        val request = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
            requestTimeoutMs = defaultRequestTimeoutMs,
            callback = handler,
        )
        client.send(request, time.milliseconds())
        client.poll(1, time.milliseconds())
        val throttleTime = 100
        val produceResponse = ProduceResponse(ProduceResponseData().setThrottleTimeMs(throttleTime))
        val buffer = serializeResponseWithHeader(
            response = produceResponse,
            version = requestVersion,
            correlationId = request.correlationId,
        )
        selector.completeReceive(NetworkReceive(source = node.idString(), buffer = buffer))
        client.poll(timeout = 1, now = time.milliseconds())

        // The connection is not ready due to throttling.
        assertFalse(client.ready(node, time.milliseconds()))
        assertEquals(expected = 100, actual = client.throttleDelayMs(node, time.milliseconds()))

        // After 50ms, the connection is not ready yet.
        time.sleep(50)
        assertFalse(client.ready(node, time.milliseconds()))
        assertEquals(expected = 50, actual = client.throttleDelayMs(node, time.milliseconds()))

        // After another 50ms, the throttling is done and the connection becomes ready again.
        time.sleep(50)
        assertTrue(client.ready(node, time.milliseconds()))
        assertEquals(expected = 0, actual = client.throttleDelayMs(node, time.milliseconds()))
    }

    // Creates expected ApiVersionsResponse from the specified node, where the max protocol version
    // for the specified key is set to the specified version.
    private fun createExpectedApiVersionsResponse(
        key: ApiKeys,
        maxVersion: Short,
    ): ApiVersionsResponse {
        val versionList = ApiVersionCollection()
        for (apiKey in ApiKeys.values()) {
            if (apiKey === key) {
                versionList.add(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(apiKey.id)
                        .setMinVersion(0)
                        .setMaxVersion(maxVersion)
                )
            } else versionList.add(ApiVersionsResponse.toApiVersion(apiKey))
        }
        return ApiVersionsResponse(
            ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code)
                .setThrottleTimeMs(0)
                .setApiKeys(versionList)
        )
    }

    @Test
    fun testThrottlingNotEnabledForConnectionToOlderBroker() {
        // Instrument the test so that the max protocol version for PRODUCE returned from the node is 5 and thus
        // client-side throttling is not enabled. Also, return a response with a 100ms throttle delay.
        setExpectedApiVersionsResponse(
            createExpectedApiVersionsResponse(key = ApiKeys.PRODUCE, maxVersion = 5)
        )
        while (!client.ready(node, time.milliseconds()))
            client.poll(timeout = 1, now = time.milliseconds())
        selector.clear()
        val correlationId = sendEmptyProduceRequest()
        client.poll(timeout = 1, now = time.milliseconds())
        sendThrottledProduceResponse(
            correlationId = correlationId,
            throttleMs = 100,
            version = 5,
        )
        client.poll(timeout = 1, now = time.milliseconds())

        // Since client-side throttling is disabled, the connection is ready even though the
        // response indicated a throttle delay.
        assertTrue(client.ready(node, time.milliseconds()))
        assertEquals(expected = 0, actual = client.throttleDelayMs(node, time.milliseconds()))
    }

    private fun sendEmptyProduceRequest(nodeId: String = node.idString()): Int {
        val builder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(TopicProduceDataCollection())
                .setAcks(1.toShort())
                .setTimeoutMs(1000)
        )
        val handler = TestCallbackHandler()
        val request = client.newClientRequest(
            nodeId = nodeId,
            requestBuilder = builder,
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
            requestTimeoutMs = defaultRequestTimeoutMs,
            callback = handler,
        )
        client.send(request, time.milliseconds())
        return request.correlationId
    }

    private fun sendResponse(response: AbstractResponse, version: Short, correlationId: Int) {
        val buffer = serializeResponseWithHeader(response, version, correlationId)
        selector.completeReceive(NetworkReceive(source = node.idString(), buffer = buffer))
    }

    private fun sendThrottledProduceResponse(correlationId: Int, throttleMs: Int, version: Short) {
        val response = ProduceResponse(ProduceResponseData().setThrottleTimeMs(throttleMs))
        sendResponse(response, version, correlationId)
    }

    @Test
    fun testLeastLoadedNode() {
        client.ready(node, time.milliseconds())
        assertFalse(client.isReady(node, time.milliseconds()))
        assertEquals(expected = node, actual = client.leastLoadedNode(time.milliseconds()))
        awaitReady(client, node)
        client.poll(timeout = 1, now = time.milliseconds())
        assertTrue(
            actual = client.isReady(node, time.milliseconds()),
            message = "The client should be ready",
        )

        // leastloadednode should be our single node
        var leastNode = client.leastLoadedNode(time.milliseconds())
        assertEquals(
            expected = node.id,
            actual = leastNode!!.id,
            message = "There should be one leastloadednode"
        )

        // sleep for longer than reconnect backoff
        time.sleep(reconnectBackoffMsTest)

        // CLOSE node
        selector.serverDisconnect(node.idString())
        client.poll(timeout = 1, now = time.milliseconds())
        assertFalse(
            actual = client.ready(node, time.milliseconds()),
            message = "After we forced the disconnection the client is no longer ready.",
        )
        leastNode = client.leastLoadedNode(time.milliseconds())
        assertNull(leastNode, "There should be NO leastloadednode")
    }

    @Test
    fun testLeastLoadedNodeProvideDisconnectedNodesPrioritizedByLastConnectionTimestamp() {
        val nodeNumber = 3
        val client = createNetworkClientWithMultipleNodes(
            reconnectBackoffMaxMs = 0,
            connectionSetupTimeoutMsTest = connectionSetupTimeoutMsTest,
            nodeNumber = nodeNumber,
        )
        val providedNodeIds = mutableSetOf<Node>()
        for (i in 0 until nodeNumber * 10) {
            val node = client.leastLoadedNode(time.milliseconds())
            assertNotNull(node, "Should provide a node")
            providedNodeIds.add(node)
            client.ready(node, time.milliseconds())
            client.disconnect(node.idString())
            time.sleep(connectionSetupTimeoutMsTest + 1)
            client.poll(timeout = 0, now = time.milliseconds())
            // Define a round as nodeNumber of nodes have been provided
            // In each round every node should be provided exactly once
            if ((i + 1) % nodeNumber == 0) {
                assertEquals(
                    expected = nodeNumber,
                    actual = providedNodeIds.size,
                    message = "All the nodes should be provided"
                )
                providedNodeIds.clear()
            }
        }
    }

    @Test
    fun testAuthenticationFailureWithInFlightMetadataRequest() {
        val refreshBackoffMs = 50
        val metadataResponse = metadataUpdateWith(numNodes = 2, topicPartitionCounts = emptyMap())
        val metadata = Metadata(
            refreshBackoffMs = refreshBackoffMs.toLong(),
            metadataExpireMs = 5000,
            logContext = LogContext(),
            clusterResourceListeners = ClusterResourceListeners(),
        )
        metadata.updateWithCurrentRequestVersion(
            response = metadataResponse,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        val cluster = metadata.fetch()
        val node1 = cluster.nodes[0]
        val node2 = cluster.nodes[1]
        val client = createNetworkClientWithNoVersionDiscovery(metadata)
        awaitReady(client, node1)
        metadata.requestUpdate()
        time.sleep(refreshBackoffMs.toLong())
        client.poll(timeout = 0, now = time.milliseconds())
        val nodeWithPendingMetadataOpt = cluster.nodes.firstOrNull { node ->
            client.hasInFlightRequests(node.idString())
        }
        assertEquals(expected = node1, actual = nodeWithPendingMetadataOpt)
        assertFalse(client.ready(node2, time.milliseconds()))
        selector.serverAuthenticationFailed(node2.idString())
        client.poll(timeout = 0, now = time.milliseconds())
        assertNotNull(client.authenticationException(node2))
        val requestBuffer = selector.completedSendBuffers()[0].buffer()
        val header = parseHeader(requestBuffer)
        assertEquals(expected = ApiKeys.METADATA, actual = header.apiKey)
        val responseBuffer = serializeResponseWithHeader(
            response = metadataResponse,
            version = header.apiVersion,
            correlationId = header.correlationId,
        )
        selector.delayedReceive(
            DelayedReceive(
                source = node1.idString(),
                receive = NetworkReceive(source = node1.idString(), buffer = responseBuffer),
            )
        )
        val initialUpdateVersion = metadata.updateVersion()
        client.poll(timeout = 0, now = time.milliseconds())
        assertEquals(expected = initialUpdateVersion + 1, actual = metadata.updateVersion())
    }

    @Test
    fun testLeastLoadedNodeConsidersThrottledConnections() {
        client.ready(node, time.milliseconds())
        awaitReady(client, node)
        client.poll(timeout = 1, now = time.milliseconds())
        assertTrue(
            actual = client.isReady(node, time.milliseconds()),
            message = "The client should be ready"
        )
        val correlationId = sendEmptyProduceRequest()
        client.poll(timeout = 1, now = time.milliseconds())
        sendThrottledProduceResponse(correlationId, 100, ApiKeys.PRODUCE.latestVersion())
        client.poll(timeout = 1, now = time.milliseconds())

        // leastloadednode should return null since the node is throttled
        assertNull(client.leastLoadedNode(time.milliseconds()))
    }

    @Test
    fun testConnectionDelayWithNoExponentialBackoff() {
        val now = time.milliseconds()
        val delay = clientWithNoExponentialBackoff.connectionDelay(node, now)
        assertEquals(expected = 0, actual = delay)
    }

    @Test
    fun testConnectionDelayConnectedWithNoExponentialBackoff() {
        awaitReady(clientWithNoExponentialBackoff, node)
        val now = time.milliseconds()
        val delay = clientWithNoExponentialBackoff.connectionDelay(node, now)
        assertEquals(expected = Long.MAX_VALUE, actual = delay)
    }

    @Test
    fun testConnectionDelayDisconnectedWithNoExponentialBackoff() {
        awaitReady(clientWithNoExponentialBackoff, node)
        selector.serverDisconnect(node.idString())
        clientWithNoExponentialBackoff.poll(defaultRequestTimeoutMs.toLong(), time.milliseconds())
        val delay = clientWithNoExponentialBackoff.connectionDelay(node, time.milliseconds())
        assertEquals(reconnectBackoffMsTest, delay)

        // Sleep until there is no connection delay
        time.sleep(delay)
        assertEquals(
            expected = 0,
            actual = clientWithNoExponentialBackoff.connectionDelay(node, time.milliseconds()),
        )

        // Start connecting and disconnect before the connection is established
        client.ready(node, time.milliseconds())
        selector.serverDisconnect(node.idString())
        client.poll(timeout = defaultRequestTimeoutMs.toLong(), now = time.milliseconds())

        // Second attempt should have the same behaviour as exponential backoff is disabled
        assertEquals(expected = reconnectBackoffMsTest, actual = delay)
    }

    @Test
    fun testConnectionDelay() {
        val now = time.milliseconds()
        val delay = client.connectionDelay(node, now)
        assertEquals(expected = 0, actual = delay)
    }

    @Test
    fun testConnectionDelayConnected() {
        awaitReady(client, node)
        val now = time.milliseconds()
        val delay = client.connectionDelay(node, now)
        assertEquals(expected = Long.MAX_VALUE, actual = delay)
    }

    @Test
    fun testConnectionDelayDisconnected() {
        awaitReady(client, node)

        // First disconnection
        selector.serverDisconnect(node.idString())
        client.poll(this.defaultRequestTimeoutMs.toLong(), time.milliseconds())
        var delay = client.connectionDelay(node, time.milliseconds())
        var expectedDelay = reconnectBackoffMsTest
        var jitter = 0.3
        assertEquals(
            expected = expectedDelay.toDouble(),
            actual = delay.toDouble(),
            absoluteTolerance = expectedDelay * jitter,
        )

        // Sleep until there is no connection delay
        time.sleep(delay)
        assertEquals(expected = 0, actual = client.connectionDelay(node, time.milliseconds()))

        // Start connecting and disconnect before the connection is established
        client.ready(node, time.milliseconds())
        selector.serverDisconnect(node.idString())
        client.poll(timeout = defaultRequestTimeoutMs.toLong(), now = time.milliseconds())

        // Second attempt should take twice as long with twice the jitter
        expectedDelay = Math.round((delay * 2).toFloat()).toLong()
        delay = client.connectionDelay(node, time.milliseconds())
        jitter = 0.6
        assertEquals(
            expected = expectedDelay.toDouble(),
            actual = delay.toDouble(),
            absoluteTolerance = expectedDelay * jitter,
        )
    }

    @Test
    fun testDisconnectDuringUserMetadataRequest() {
        // this test ensures that the default metadata updater does not intercept a user-initiated
        // metadata request when the remote node disconnects with the request in-flight.
        awaitReady(client, node)
        val builder = MetadataRequest.Builder(emptyList(), true)
        val now = time.milliseconds()
        val request = client.newClientRequest(node.idString(), builder, now, true)
        client.send(request, now)
        client.poll(timeout = defaultRequestTimeoutMs.toLong(), now = now)
        assertEquals(expected = 1, actual = client.inFlightRequestCount(node.idString()))
        assertTrue(client.hasInFlightRequests(node.idString()))
        assertTrue(client.hasInFlightRequests())
        selector.close(node.idString())
        val responses = client.poll(
            timeout = defaultRequestTimeoutMs.toLong(),
            now = time.milliseconds(),
        )
        assertEquals(expected = 1, actual = responses.size)
        assertTrue(responses.first().disconnected)
    }

    @Test
    @Throws(Exception::class)
    fun testServerDisconnectAfterInternalApiVersionRequest() {
        val numIterations: Long = 5
        val reconnectBackoffMaxExp =
            ln(reconnectBackoffMaxMsTest / max(reconnectBackoffMsTest.toDouble(), 1.0)) /
                    ln(reconnectBackoffExpBase.toDouble())
        for (i in 0 until numIterations) {
            selector.clear()
            awaitInFlightApiVersionRequest()
            selector.serverDisconnect(node.idString())

            // The failed ApiVersion request should not be forwarded to upper layers
            val responses = client.poll(0, time.milliseconds())
            assertFalse(client.hasInFlightRequests(node.idString()))
            assertTrue(responses.isEmpty())
            val expectedBackoff = Math.round(
                reconnectBackoffExpBase.toDouble().pow(
                    min(i.toDouble(), reconnectBackoffMaxExp)
                ) * reconnectBackoffMsTest
            )
            val delay = client.connectionDelay(node, time.milliseconds())
            assertEquals(
                expected = expectedBackoff.toDouble(),
                actual = delay.toDouble(),
                absoluteTolerance = reconnectBackoffJitter * expectedBackoff,
            )
            if (i == numIterations - 1) break
            time.sleep(delay + 1)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testClientDisconnectAfterInternalApiVersionRequest() {
        awaitInFlightApiVersionRequest()
        client.disconnect(node.idString())
        assertFalse(client.hasInFlightRequests(node.idString()))

        // The failed ApiVersion request should not be forwarded to upper layers
        val responses = client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(responses.isEmpty())
    }

    @Test
    fun testDisconnectWithMultipleInFlights() {
        val client = clientWithNoVersionDiscovery
        awaitReady(client, node)
        assertTrue(
            actual = client.isReady(node, time.milliseconds()),
            message = "Expected NetworkClient to be ready to send to node " + node.idString(),
        )
        val builder = MetadataRequest.Builder(emptyList(), true)
        val now = time.milliseconds()
        val callbackResponses: MutableList<ClientResponse> = ArrayList()
        val callback = RequestCompletionHandler { e -> callbackResponses.add(e) }
        val request1 = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = now,
            expectResponse = true,
            requestTimeoutMs = defaultRequestTimeoutMs,
            callback = callback,
        )
        client.send(request1, now)
        client.poll(timeout = 0, now = now)
        val request2 = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = now,
            expectResponse = true,
            requestTimeoutMs = defaultRequestTimeoutMs,
            callback = callback,
        )
        client.send(request2, now)
        client.poll(timeout = 0, now = now)
        assertNotEquals(illegal = request1.correlationId, actual = request2.correlationId)
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 2, actual = client.inFlightRequestCount(node.idString()))
        client.disconnect(node.idString())
        val responses = client.poll(timeout = 0, now = time.milliseconds())
        assertEquals(expected = 2, actual = responses.size)
        assertEquals(expected = responses, actual = callbackResponses)
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertEquals(expected = 0, actual = client.inFlightRequestCount(node.idString()))

        // Ensure that the responses are returned in the order they were sent
        val response1 = responses[0]
        assertTrue(response1.disconnected)
        assertEquals(
            expected = request1.correlationId,
            actual = response1.requestHeader.correlationId,
        )
        val response2 = responses[1]
        assertTrue(response2.disconnected)
        assertEquals(
            expected = request2.correlationId,
            actual = response2.requestHeader.correlationId,
        )
    }

    @Test
    fun testCallDisconnect() {
        awaitReady(client, node)
        assertTrue(
            actual = client.isReady(node, time.milliseconds()),
            message = "Expected NetworkClient to be ready to send to node " + node.idString()
        )
        assertFalse(
            actual = client.connectionFailed(node),
            message = "Did not expect connection to node ${node.idString()} to be failed",
        )
        client.disconnect(node.idString())
        assertFalse(
            actual = client.isReady(node, time.milliseconds()),
            message = "Expected node ${node.idString()} to be disconnected.",
        )
        assertTrue(
            actual = client.connectionFailed(node),
            message = "Expected connection to node ${node.idString()} to be failed after disconnect",
        )
        assertFalse(client.canConnect(node, time.milliseconds()))

        // ensure disconnect does not reset backoff period if already disconnected
        time.sleep(reconnectBackoffMaxMsTest)
        assertTrue(client.canConnect(node, time.milliseconds()))
        client.disconnect(node.idString())
        assertTrue(client.canConnect(node, time.milliseconds()))
    }

    @Test
    fun testCorrelationId() {
        val count = 100
        val ids = generateSequence { client.nextCorrelationId() }.take(count).toSet()
        assertEquals(expected = count, actual = ids.size)
        ids.forEach { id ->
            assertTrue(id < SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID)
        }
    }

    @Test
    fun testReconnectAfterAddressChange() {
        val mockHostResolver = AddressChangeHostResolver(
            initialAddresses = initialAddresses!!.toTypedArray<InetAddress>(),
            newAddresses = newAddresses!!.toTypedArray<InetAddress>(),
        )
        val initialAddressConns = AtomicInteger()
        val newAddressConns = AtomicInteger()
        val selector = MockSelector(time) { inetSocketAddress ->
            val inetAddress = inetSocketAddress.address
            if (initialAddresses!!.contains(inetAddress)) initialAddressConns.incrementAndGet()
            else if (newAddresses!!.contains(inetAddress)) newAddressConns.incrementAndGet()
            mockHostResolver.useNewAddresses()
                    && newAddresses!!.contains(inetAddress)
                    || !mockHostResolver.useNewAddresses()
                    && initialAddresses!!.contains(inetAddress)
        }
        val client = NetworkClient(
            metadataUpdater = metadataUpdater,
            metadata = null,
            selector = selector,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = reconnectBackoffMaxMsTest,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = false,
            apiVersions = ApiVersions(),
            throttleTimeSensor = null,
            logContext = LogContext(),
            hostResolver = mockHostResolver,
        )

        // Connect to one the initial addresses, then change the addresses and disconnect
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(client.isReady(node, time.milliseconds()))
        mockHostResolver.changeAddresses()
        selector.serverDisconnect(node.idString())
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(client.isReady(node, time.milliseconds()))
        time.sleep(reconnectBackoffMaxMsTest)
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(client.isReady(node, time.milliseconds()))

        // We should have tried to connect to one initial address and one new address, and resolved DNS twice
        assertEquals(expected = 1, actual = initialAddressConns.get())
        assertEquals(expected = 1, actual = newAddressConns.get())
        assertEquals(expected = 2, actual = mockHostResolver.resolutionCount())
    }

    @Test
    fun testFailedConnectionToFirstAddress() {
        val mockHostResolver = AddressChangeHostResolver(
            initialAddresses!!.toTypedArray<InetAddress>(),
            newAddresses!!.toTypedArray<InetAddress>()
        )
        val initialAddressConns = AtomicInteger()
        val newAddressConns = AtomicInteger()
        val selector = MockSelector(time) { inetSocketAddress ->
            val inetAddress = inetSocketAddress.address
            if (initialAddresses!!.contains(inetAddress)) initialAddressConns.incrementAndGet()
            else if (newAddresses!!.contains(inetAddress)) newAddressConns.incrementAndGet()
            initialAddressConns.get() > 1
        }
        val client = NetworkClient(
            metadataUpdater = metadataUpdater,
            metadata = null,
            selector = selector,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = reconnectBackoffMaxMsTest,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = false,
            apiVersions = ApiVersions(),
            throttleTimeSensor = null,
            logContext = LogContext(),
            hostResolver = mockHostResolver,
        )

        // First connection attempt should fail
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(client.isReady(node, time.milliseconds()))

        // Second connection attempt should succeed
        time.sleep(reconnectBackoffMaxMsTest)
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(client.isReady(node, time.milliseconds()))

        // We should have tried to connect to two of the initial addresses, none of the new address,
        // and should only have resolved DNS once
        assertEquals(expected = 2, actual = initialAddressConns.get())
        assertEquals(expected = 0, actual = newAddressConns.get())
        assertEquals(expected = 1, actual = mockHostResolver.resolutionCount())
    }

    @Test
    fun testFailedConnectionToFirstAddressAfterReconnect() {
        val mockHostResolver = AddressChangeHostResolver(
            initialAddresses = initialAddresses!!.toTypedArray<InetAddress>(),
            newAddresses = newAddresses!!.toTypedArray<InetAddress>(),
        )
        val initialAddressConns = AtomicInteger()
        val newAddressConns = AtomicInteger()
        val selector = MockSelector(time) { inetSocketAddress ->
            val inetAddress = inetSocketAddress.address
            if (initialAddresses!!.contains(inetAddress)) initialAddressConns.incrementAndGet()
            else if (newAddresses!!.contains(inetAddress)) newAddressConns.incrementAndGet()
            initialAddresses!!.contains(inetAddress) || newAddressConns.get() > 1
        }
        val client = NetworkClient(
            metadataUpdater = metadataUpdater,
            metadata = null,
            selector = selector,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = reconnectBackoffMsTest,
            reconnectBackoffMax = reconnectBackoffMaxMsTest,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = defaultRequestTimeoutMs,
            connectionSetupTimeoutMs = connectionSetupTimeoutMsTest,
            connectionSetupTimeoutMaxMs = connectionSetupTimeoutMaxMsTest,
            time = time,
            discoverBrokerVersions = false,
            apiVersions = ApiVersions(),
            throttleTimeSensor = null,
            logContext = LogContext(),
            hostResolver = mockHostResolver,
        )

        // Connect to one the initial addresses, then change the addresses and disconnect
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(client.isReady(node, time.milliseconds()))
        mockHostResolver.changeAddresses()
        selector.serverDisconnect(node.idString())
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(client.isReady(node, time.milliseconds()))

        // First connection attempt to new addresses should fail
        time.sleep(reconnectBackoffMaxMsTest)
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(client.isReady(node, time.milliseconds()))

        // Second connection attempt to new addresses should succeed
        time.sleep(reconnectBackoffMaxMsTest)
        client.ready(node, time.milliseconds())
        time.sleep(connectionSetupTimeoutMaxMsTest)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(client.isReady(node, time.milliseconds()))

        // We should have tried to connect to one of the initial addresses and two of the new
        // addresses (the first one failed), and resolved DNS twice, once for each set of addresses
        assertEquals(expected = 1, actual = initialAddressConns.get())
        assertEquals(expected = 2, actual = newAddressConns.get())
        assertEquals(expected = 2, actual = mockHostResolver.resolutionCount())
    }

    @Test
    fun testCloseConnectingNode() {
        val cluster = TestUtils.clusterWith(2)
        val node0 = cluster.nodeById(0)
        val node1 = cluster.nodeById(1)
        client.ready(node0!!, time.milliseconds())
        selector.serverConnectionBlocked(node0.idString())
        client.poll(1, time.milliseconds())
        client.close(node0.idString())

        // Poll without any connections should return without exceptions
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(isReady(client, node0, time.milliseconds()))
        assertFalse(isReady(client, node1, time.milliseconds()))

        // Connection to new node should work
        client.ready(node1!!, time.milliseconds())
        var buffer = serializeResponseWithHeader(
            response = defaultApiVersionsResponse(),
            version = ApiKeys.API_VERSIONS.latestVersion(),
            correlationId = 0,
        )
        selector.delayedReceive(
            DelayedReceive(
                source = node1.idString(),
                receive = NetworkReceive(source = node1.idString(), buffer = buffer),
            )
        )
        while (!client.ready(node1, time.milliseconds()))
            client.poll(timeout = 1, now = time.milliseconds())
        assertTrue(client.isReady(node1, time.milliseconds()))
        selector.clear()

        // New connection to node closed earlier should work
        client.ready(node0, time.milliseconds())
        buffer = serializeResponseWithHeader(
            response = defaultApiVersionsResponse(),
            version = ApiKeys.API_VERSIONS.latestVersion(),
            correlationId = 1,
        )
        selector.delayedReceive(
            DelayedReceive(
                source = node0.idString(),
                receive = NetworkReceive(source = node0.idString(), buffer = buffer),
            )
        )
        while (!client.ready(node0, time.milliseconds()))
            client.poll(timeout = 1, now = time.milliseconds())
        assertTrue(client.isReady(node0, time.milliseconds()))
    }

    @Test
    fun testConnectionDoesNotRemainStuckInCheckingApiVersionsStateIfChannelNeverBecomesReady() {
        val cluster = TestUtils.clusterWith(1)
        val node = cluster.nodeById(0)

        // Channel is ready by default so we mark it as not ready.
        client.ready(node!!, time.milliseconds())
        selector.channelNotReady(node.idString())

        // Channel should not be ready.
        client.poll(timeout = 0, now = time.milliseconds())
        assertFalse(isReady(client, node, time.milliseconds()))

        // Connection should time out if the channel does not become ready within
        // the connection setup timeout. This ensures that the client does not remain
        // stuck in the CHECKING_API_VERSIONS state.
        time.sleep((connectionSetupTimeoutMsTest * 1.2).toLong() + 1)
        client.poll(timeout = 0, now = time.milliseconds())
        assertTrue(client.connectionFailed(node))
    }

    private fun parseHeader(buffer: ByteBuffer): RequestHeader {
        buffer.getInt() // skip size
        return RequestHeader.parse(buffer.slice())
    }

    @Throws(Exception::class)
    private fun awaitInFlightApiVersionRequest() {
        client.ready(node, time.milliseconds())
        waitForCondition(
            testCondition = {
                client.poll(0, time.milliseconds())
                client.hasInFlightRequests(node.idString())
            },
            maxWaitMs = 1000,
            conditionDetails = "",
        )
        assertFalse(client.isReady(node, time.milliseconds()))
    }

    private fun defaultApiVersionsResponse(): ApiVersionsResponse {
        return TestUtils.defaultApiVersionsResponse(
            listenerType = ApiMessageType.ListenerType.ZK_BROKER,
        )
    }

    private class TestCallbackHandler : RequestCompletionHandler {
        var executed = false
        var response: ClientResponse? = null
        override fun onComplete(response: ClientResponse) {
            executed = true
            this.response = response
        }
    }

    // ManualMetadataUpdater with ability to keep track of failures
    private class TestMetadataUpdater(nodes: List<Node>) : ManualMetadataUpdater(nodes) {
        var failure: KafkaException? = null
        override fun handleServerDisconnect(
            now: Long,
            destinationId: String?,
            maybeAuthException: AuthenticationException?,
        ) {
            maybeAuthException?.let { exception -> failure = exception }
            super.handleServerDisconnect(now, destinationId, maybeAuthException)
        }

        override fun handleFailedRequest(now: Long, maybeFatalException: KafkaException?) {
            maybeFatalException?.let { exception -> failure = exception }
        }

        val andClearFailure: KafkaException?
            get() {
                val failure = failure
                this.failure = null
                return failure
            }
    }

    companion object {
        private var initialAddresses: ArrayList<InetAddress>? = null
        private var newAddresses: ArrayList<InetAddress>? = null

        init {
            try {
                initialAddresses = ArrayList(
                    listOf(
                        InetAddress.getByName("10.200.20.100"),
                        InetAddress.getByName("10.200.20.101"),
                        InetAddress.getByName("10.200.20.102")
                    )
                )
                newAddresses = ArrayList(
                    listOf(
                        InetAddress.getByName("10.200.20.103"),
                        InetAddress.getByName("10.200.20.104"),
                        InetAddress.getByName("10.200.20.105")
                    )
                )
            } catch (e: UnknownHostException) {
                fail("Attempted to create an invalid InetAddress, this should not happen")
            }
        }
    }
}
