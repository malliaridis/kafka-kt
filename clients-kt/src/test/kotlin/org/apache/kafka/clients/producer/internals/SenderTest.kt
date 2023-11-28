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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientRequest
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.internals.RecordAccumulator.AppendCallbacks
import org.apache.kafka.clients.producer.internals.RecordAccumulator.NodeLatencyStats
import org.apache.kafka.clients.producer.internals.RecordAccumulator.PartitionerConfig
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricNameTemplate
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.NetworkException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TransactionAbortedException
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.EndTxnResponseData
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection
import org.apache.kafka.common.message.ProduceResponseData
import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionRatioEstimator
import org.apache.kafka.common.record.CompressionRatioEstimator.estimation
import org.apache.kafka.common.record.CompressionRatioEstimator.setEstimation
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MutableRecordBatch
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.EndTxnRequest
import org.apache.kafka.common.requests.EndTxnResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.InitProducerIdRequest
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.RequestTestUtils
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.DelayedReceive
import org.apache.kafka.test.MockSelector
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.apache.kafka.test.TestUtils.checkEquals
import org.apache.kafka.test.TestUtils.singletonCluster
import org.apache.kafka.test.TestUtils.toList
import org.apache.kafka.test.TestUtils.waitForCondition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertTrue
import org.mockito.AdditionalMatchers.geq
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame

class SenderTest {

    private val tp0 = TopicPartition("test", 0)

    private val tp1 = TopicPartition("test", 1)

    private var time = MockTime()

    private val batchSize = 16 * 1024

    private val metadata = ProducerMetadata(
        0, Long.MAX_VALUE, TOPIC_IDLE_MS,
        LogContext(), ClusterResourceListeners(), time
    )

    private var client = MockClient(time, metadata)

    private val apiVersions = ApiVersions()

    private lateinit var metrics: Metrics

    private lateinit var accumulator: RecordAccumulator

    private lateinit var sender: Sender

    private var senderMetricsRegistry: SenderMetricsRegistry? = null

    private val logContext = LogContext()

    @BeforeEach
    fun setup() {
        setupWithTransactionState(transactionManager = null)
    }

    @AfterEach
    fun tearDown() {
        metrics.close()
    }

    @Test
    @Throws(Exception::class)
    fun testSimple() {
        val offset: Long = 0
        val future: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp0,
            timestamp = 0L,
            key = "key",
            value = "value",
        )
        sender.runOnce() // connect
        sender.runOnce() // send produce request
        assertEquals(
            expected = 1,
            actual = client.inFlightRequestCount(),
            message = "We should have a single produce request in flight.",
        )
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        assertTrue(client.hasInFlightRequests())
        client.respond(
            produceResponse(
                tp = tp0,
                offset = offset,
                error = Errors.NONE,
                throttleTimeMs = 0
            )
        )
        sender.runOnce()
        assertEquals(
            expected = 0,
            actual = client.inFlightRequestCount(),
            message = "All requests completed.",
        )
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        assertFalse(client.hasInFlightRequests())
        sender.runOnce()
        assertTrue(future!!.isDone, "Request should be completed")
        assertEquals(expected = offset, actual = future.get().offset)
    }

    @Test
    @Throws(Exception::class)
    fun testMessageFormatDownConversion() {
        // this test case verifies the behavior when the version of the produce request supported by the
        // broker changes after the record set is created
        val offset: Long = 0

        // start off support produce request v3
        apiVersions.update(nodeId = "0", nodeApiVersions = NodeApiVersions.create())
        val future = appendToAccumulator(tp = tp0, timestamp = 0L, key = "key", value = "value")

        // now the partition leader supports only v2
        apiVersions.update(
            "0",
            NodeApiVersions.create(ApiKeys.PRODUCE.id, 0.toShort(), 2.toShort())
        )
        client.prepareResponse(
            matcher = { body ->
                val request: ProduceRequest? = body as ProduceRequest?
                if (request!!.version.toInt() != 2) return@prepareResponse false
                val records: MemoryRecords? = partitionRecords(request)[tp0]
                (records != null
                        && records.sizeInBytes() > 0
                        && records.hasMatchingMagic(RecordBatch.MAGIC_VALUE_V1))
            },
            response = produceResponse(
                tp = tp0,
                offset = offset,
                error = Errors.NONE,
                throttleTimeMs = 0,
            ),
        )
        sender.runOnce() // connect
        sender.runOnce() // send produce request
        assertTrue(future!!.isDone, "Request should be completed")
        assertEquals(offset, future.get().offset)
    }

    @Suppress("Deprecation")
    @Test
    @Throws(Exception::class)
    fun testDownConversionForMismatchedMagicValues() {
        // it can happen that we construct a record set with mismatching magic values (perhaps
        // because the partition leader changed after the record set was initially constructed)
        // in this case, we down-convert record sets with newer magic values to match the oldest
        // created record set
        val offset: Long = 0

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create())
        val future1 = appendToAccumulator(tp0, 0L, "key", "value")

        // now the partition leader supports only v2
        apiVersions.update(
            "0",
            NodeApiVersions.create(ApiKeys.PRODUCE.id, 0.toShort(), 2.toShort())
        )
        val future2: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp1,
            timestamp = 0L,
            key = "key",
            value = "value",
        )

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create())
        val resp = PartitionResponse(
            error = Errors.NONE,
            baseOffset = offset,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            logStartOffset = 100,
        )
        val partResp: MutableMap<TopicPartition, PartitionResponse> = HashMap()
        partResp[tp0] = resp
        partResp[tp1] = resp
        val produceResponse = ProduceResponse(partResp, 0)
        client.prepareResponse(
            matcher = { body ->
                val request = body as ProduceRequest?
                if (request!!.version.toInt() != 2) return@prepareResponse false

                val recordsMap = partitionRecords(request)
                if (recordsMap.size != 2) return@prepareResponse false

                for (records: MemoryRecords? in recordsMap.values) {
                    if (
                        (records == null)
                        || (records.sizeInBytes() == 0)
                        || !records.hasMatchingMagic(RecordBatch.MAGIC_VALUE_V1)
                    ) return@prepareResponse false
                }
                true
            },
            response = produceResponse,
        )
        sender.runOnce() // connect
        sender.runOnce() // s
        assertNotNull(future1, "Request should not be null.")
        assertNotNull(future2, "Request should not be null.")
        assertTrue(future1.isDone, "Request should be completed")
        assertTrue(future2.isDone, "Request should be completed")
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    fun testQuotaMetrics() {
        val selector = MockSelector(time)
        val throttleTimeSensor = Sender.throttleTimeSensor(senderMetricsRegistry!!)
        val cluster = singletonCluster("test", 1)
        val node = cluster.nodes[0]
        val client = NetworkClient(
            selector = selector,
            metadata = metadata,
            clientId = "mock",
            maxInFlightRequestsPerConnection = Int.MAX_VALUE,
            reconnectBackoffMs = 1000,
            reconnectBackoffMax = 1000,
            socketSendBuffer = 64 * 1024,
            socketReceiveBuffer = 64 * 1024,
            defaultRequestTimeoutMs = 1000,
            connectionSetupTimeoutMs = 10 * 1000,
            connectionSetupTimeoutMaxMs = (127 * 1000).toLong(),
            time = time,
            discoverBrokerVersions = true,
            apiVersions = ApiVersions(),
            throttleTimeSensor = throttleTimeSensor,
            logContext = logContext,
        )
        val apiVersionsResponse = ApiVersionsResponse.defaultApiVersionsResponse(
            throttleTimeMs = 400,
            listenerType = ApiMessageType.ListenerType.ZK_BROKER,
        )
        var buffer = RequestTestUtils.serializeResponseWithHeader(
            apiVersionsResponse,
            ApiKeys.API_VERSIONS.latestVersion(),
            0,
        )
        selector.delayedReceive(
            DelayedReceive(
                source = node.idString(),
                receive = NetworkReceive(source = node.idString(), buffer = buffer)
            )
        )

        while (!client.ready(node, time.milliseconds())) {
            client.poll(1, time.milliseconds())
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()))
        }
        selector.clear()
        for (i in 1..3) {
            val throttleTimeMs = 100 * i
            val builder = ProduceRequest.forCurrentMagic(
                ProduceRequestData()
                    .setTopicData(TopicProduceDataCollection())
                    .setAcks(1.toShort())
                    .setTimeoutMs(1000)
            )
            val request =
                client.newClientRequest(node.idString(), builder, time.milliseconds(), true)
            client.send(request, time.milliseconds())
            client.poll(1, time.milliseconds())
            val response = produceResponse(
                tp = tp0,
                offset = i.toLong(),
                error = Errors.NONE,
                throttleTimeMs = throttleTimeMs
            )
            buffer = RequestTestUtils.serializeResponseWithHeader(
                response,
                ApiKeys.PRODUCE.latestVersion(),
                request.correlationId
            )
            selector.completeReceive(NetworkReceive(source = node.idString(), buffer = buffer))
            client.poll(1, time.milliseconds())
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()))
            selector.clear()
        }
        val allMetrics = metrics.metrics
        val avgMetric = allMetrics[senderMetricsRegistry!!.produceThrottleTimeAvg]
        val maxMetric = allMetrics[senderMetricsRegistry!!.produceThrottleTimeMax]
        // Throttle times are ApiVersions=400, Produce=(100, 200, 300)
        assertEquals(
            expected = 250.0,
            actual = (avgMetric!!.metricValue() as Double),
            absoluteTolerance = EPS,
        )
        assertEquals(
            expected = 400.0,
            actual = (maxMetric!!.metricValue() as Double),
            absoluteTolerance = EPS,
        )
        client.close()
    }

    @Test
    @Throws(Exception::class)
    fun testSenderMetricsTemplates() {
        metrics.close()
        val clientTags = mapOf("client-id" to "clientA")
        metrics = Metrics(MetricConfig().apply { tags = clientTags })
        val metricsRegistry = SenderMetricsRegistry(metrics)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = false,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = 1,
            metricsRegistry = metricsRegistry,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = null,
            apiVersions = apiVersions,
        )

        // Append a message so that topic metrics are created
        appendToAccumulator(tp0, 0L, "key", "value")
        sender.runOnce() // connect
        sender.runOnce() // send produce request
        client.respond(
            produceResponse(
                tp = tp0,
                offset = 0,
                error = Errors.NONE,
                throttleTimeMs = 0
            )
        )
        sender.runOnce()
        // Create throttle time metrics
        Sender.throttleTimeSensor(metricsRegistry)

        // Verify that all metrics except metrics-count have registered templates
        val allMetrics: MutableSet<MetricNameTemplate> = HashSet()
        for ((name, group, _, tags) in metrics.metrics.keys) {
            if (group != "kafka-metrics-count") allMetrics.add(
                MetricNameTemplate(
                    name = name,
                    group = group,
                    description = "",
                    tagsNames = tags.keys,
                )
            )
        }
        checkEquals(allMetrics, HashSet(metricsRegistry.allTemplates()), "metrics", "templates")
    }

    @Test
    @Throws(Exception::class)
    fun testRetries() {
        // create a sender with retries = 1
        val maxRetries = 1
        val metrics = Metrics()
        val senderMetrics = SenderMetricsRegistry(metrics)
        metrics.use { m ->
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = maxRetries,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = null,
                apiVersions = apiVersions
            )
            // do a successful retry
            var future = appendToAccumulator(
                tp = tp0,
                timestamp = 0L,
                key = "key",
                value = "value",
            )
            sender.runOnce() // connect
            sender.runOnce() // send produce request
            val id = client.requests().peek().destination
            val node = Node(
                id = id.toInt(),
                host = "localhost",
                port = 0,
            )
            assertEquals(expected = 1, actual = client.inFlightRequestCount())
            assertTrue(client.hasInFlightRequests())
            assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
            assertTrue(
                actual = client.isReady(node, time.milliseconds()),
                message = "Client ready status should be true",
            )
            client.disconnect(id)
            assertEquals(expected = 0, actual = client.inFlightRequestCount())
            assertFalse(client.hasInFlightRequests())
            assertFalse(
                actual = client.isReady(node, time.milliseconds()),
                message = "Client ready status should be false",
            )
            // the batch is in accumulator.inFlightBatches until it expires
            assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
            sender.runOnce() // receive error
            sender.runOnce() // reconnect
            sender.runOnce() // resend
            assertEquals(expected = 1, actual = client.inFlightRequestCount())
            assertTrue(client.hasInFlightRequests())
            assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
            val offset: Long = 0
            client.respond(
                produceResponse(
                    tp = tp0,
                    offset = offset,
                    error = Errors.NONE,
                    throttleTimeMs = 0
                )
            )
            sender.runOnce()
            assertTrue(future!!.isDone, "Request should have retried and completed")
            assertEquals(offset, future.get().offset)
            assertEquals(0, sender.inFlightBatches(tp0).size)

            // do an unsuccessful retry
            future = appendToAccumulator(tp0, 0L, "key", "value")
            sender.runOnce() // send produce request
            assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
            for (i in 0 until maxRetries + 1) {
                client.disconnect(client.requests().peek().destination)
                sender.runOnce() // receive error
                assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
                sender.runOnce() // reconnect
                sender.runOnce() // resend
                assertEquals(
                    expected = if (i > 0) 0 else 1,
                    actual = sender.inFlightBatches(tp0).size,
                )
            }
            sender.runOnce()
            assertFutureFailure(future, NetworkException::class.java)
            assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSendInOrder() {
        val maxRetries = 1
        val metrics = Metrics()
        val senderMetrics = SenderMetricsRegistry(metrics)
        metrics.use { m ->
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = true,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = maxRetries,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = null,
                apiVersions = apiVersions,
            )
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            val metadataUpdate1 = RequestTestUtils.metadataUpdateWith(
                numNodes = 2,
                topicPartitionCounts = mapOf("test" to 2),
            )
            client.prepareMetadataUpdate(metadataUpdate1)

            // Send the first message.
            val tp2 = TopicPartition("test", 1)
            appendToAccumulator(tp2, 0L, "key1", "value1")
            sender.runOnce() // connect
            sender.runOnce() // send produce request
            val id = client.requests().peek().destination
            assertEquals(
                expected = ApiKeys.PRODUCE,
                actual = client.requests().peek().requestBuilder().apiKey,
            )
            val node = Node(id.toInt(), "localhost", 0)
            assertEquals(1, client.inFlightRequestCount())
            assertTrue(client.hasInFlightRequests())
            assertTrue(
                client.isReady(node, time.milliseconds()),
                "Client ready status should be true"
            )
            assertEquals(1, sender.inFlightBatches(tp2).size)
            time.sleep(900)
            // Now send another message to tp2
            appendToAccumulator(tp2, 0L, "key2", "value2")

            // Update metadata before sender receives response from broker 0. Now partition 2 moves to broker 0
            val metadataUpdate2 = RequestTestUtils.metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            )
            client.prepareMetadataUpdate(metadataUpdate2)
            // Sender should not send the second message to node 0.
            assertEquals(1, sender.inFlightBatches(tp2).size)
            sender.runOnce() // receive the response for the previous send, and send the new batch
            assertEquals(1, client.inFlightRequestCount())
            assertTrue(client.hasInFlightRequests())
            assertEquals(1, sender.inFlightBatches(tp2).size)
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAppendInExpiryCallback() {
        val messagesPerBatch = 10
        val expiryCallbackCount = AtomicInteger(0)
        val unexpectedException = AtomicReference<Exception?>()
        val key = "key".toByteArray()
        val value = "value".toByteArray()
        val maxBlockTimeMs: Long = 1000
        val cluster = singletonCluster()
        val callbacks = object : AppendCallbacks {

            override fun setPartition(partition: Int) = Unit

            override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                if (exception is TimeoutException) {
                    expiryCallbackCount.incrementAndGet()
                    try {
                        accumulator.append(
                            tp1.topic,
                            tp1.partition,
                            0L,
                            key,
                            value,
                            Record.EMPTY_HEADERS,
                            null,
                            maxBlockTimeMs,
                            false,
                            time.milliseconds(),
                            cluster
                        )
                    } catch (e: InterruptedException) {
                        throw RuntimeException("Unexpected interruption", e)
                    }
                } else if (exception != null) unexpectedException.compareAndSet(null, exception)
            }
        }
        val nowMs = time.milliseconds()
        for (i in 0 until messagesPerBatch) accumulator.append(
            topic = tp1.topic,
            partition = tp1.partition,
            timestamp = 0L,
            key = key,
            value = value,
            headers = null,
            callbacks = callbacks,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = nowMs,
            cluster = cluster,
        )

        // Advance the clock to expire the first batch.
        time.sleep(10000)
        val clusterNode = metadata.fetch().nodes[0]
        val drainedBatches = accumulator.drain(
            metadata.fetch(),
            setOf(clusterNode),
            Int.MAX_VALUE,
            time.milliseconds()
        )
        sender.addToInflightBatches(drainedBatches)

        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        client.disconnect(clusterNode.idString())
        client.backoff(clusterNode, 100)
        sender.runOnce() // We should try to flush the batch, but we expire it instead without sending anything.
        assertEquals(
            expected = messagesPerBatch,
            actual = expiryCallbackCount.get(),
            message = "Callbacks not invoked for expiry"
        )
        Assertions.assertNull(unexpectedException.get(), "Unexpected exception")
        // Make sure that the reconds were appended back to the batch.
        assertNotNull(accumulator.getDeque(tp1))
        assertEquals(expected = 1, actual = accumulator.getDeque(tp1)!!.size)
        assertEquals(
            expected = messagesPerBatch,
            actual = accumulator.getDeque(tp1)!!.peekFirst().recordCount
        )
    }

    /**
     * Tests that topics are added to the metadata list when messages are available to send
     * and expired if not used during a metadata refresh interval.
     */
    @Test
    @Throws(Exception::class)
    fun testMetadataTopicExpiry() {
        val offset: Long = 0
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            ),
        )
        var future: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertTrue(metadata.containsTopic(tp0.topic), "Topic not added to metadata")
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            )
        )
        sender.runOnce() // send produce request
        client.respond(
            produceResponse(
                tp = tp0,
                offset = offset,
                error = Errors.NONE,
                throttleTimeMs = 0
            )
        )
        sender.runOnce()
        assertEquals(
            expected = 0,
            actual = client.inFlightRequestCount(),
            message = "Request completed.",
        )
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        sender.runOnce()
        assertTrue(future!!.isDone, "Request should be completed")
        assertTrue(
            actual = metadata.containsTopic(tp0.topic),
            message = "Topic not retained in metadata list",
        )
        time.sleep(TOPIC_IDLE_MS)
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            ),
        )
        assertFalse(
            metadata.containsTopic(tp0.topic),
            "Unused topic has not been expired"
        )
        future = appendToAccumulator(tp0)
        sender.runOnce()
        assertTrue(metadata.containsTopic(tp0.topic), "Topic not added to metadata")
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            ),
        )
        sender.runOnce() // send produce request
        client.respond(
            produceResponse(
                tp = tp0,
                offset = offset + 1,
                error = Errors.NONE,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce()
        assertEquals(
            expected = 0,
            actual = client.inFlightRequestCount(),
            message = "Request completed."
        )
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        sender.runOnce()
        assertTrue(future!!.isDone(), "Request should be completed")
    }

    @Test
    @Throws(Exception::class)
    fun testNodeLatencyStats() {
        Metrics().use { m ->
            // Create a new record accumulator with non-0 partitionAvailabilityTimeoutMs
            // otherwise it wouldn't update the stats.
            val config = PartitionerConfig(
                enableAdaptivePartitioning = false,
                partitionAvailabilityTimeoutMs = 42,
            )
            val totalSize: Long = (1024 * 1024).toLong()
            accumulator = RecordAccumulator(
                logContext = logContext,
                batchSize = batchSize,
                compression = CompressionType.NONE,
                lingerMs = 0,
                retryBackoffMs = 0L,
                deliveryTimeoutMs = DELIVERY_TIMEOUT_MS,
                partitionerConfig = config,
                metrics = m,
                metricGrpName = "producer-metrics",
                time = time,
                apiVersions = apiVersions,
                transactionManager = null,
                free = BufferPool(
                    totalMemory = totalSize,
                    poolableSize = batchSize,
                    metrics = m,
                    time = time,
                    metricGrpName = "producer-internal-metrics",
                ),
            )
            val senderMetrics = SenderMetricsRegistry(m)
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = 1,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = 1000L,
                transactionManager = null,
                apiVersions = ApiVersions(),
            )

            // Produce and send batch.
            val time1: Long = time.milliseconds()
            appendToAccumulator(tp0, 0L, "key", "value")
            sender.runOnce()
            assertEquals(
                expected = 1,
                actual = client.inFlightRequestCount(),
                message = "We should have a single produce request in flight."
            )

            // We were able to send the batch out, so both the ready and drain values should be the same.
            val stats: NodeLatencyStats? = accumulator.getNodeLatencyStats(0)
            assertEquals(expected = time1, actual = stats!!.drainTimeMs)
            assertEquals(expected = time1, actual = stats.readyTimeMs)

            // Make the node 1 not ready.
            client.throttle(metadata.fetch().nodeById(0)!!, 100)

            // Time passes, but we don't have anything to send.
            time.sleep(10)
            sender.runOnce()
            assertEquals(
                expected = 1,
                actual = client.inFlightRequestCount(),
                message = "We should have a single produce request in flight.",
            )

            // Stats shouldn't change as we didn't have anything ready.
            assertEquals(expected = time1, actual = stats.drainTimeMs)
            assertEquals(expected = time1, actual = stats.readyTimeMs)

            // Produce a new batch, but we won't be able to send it because node is not ready.
            var time2: Long = time.milliseconds()
            appendToAccumulator(tp0, 0L, "key", "value")
            sender.runOnce()
            assertEquals(
                1,
                client.inFlightRequestCount(),
                "We should have a single produce request in flight."
            )

            // The ready time should move forward, but drain time shouldn't change.
            assertEquals(time1, stats.drainTimeMs)
            assertEquals(time2, stats.readyTimeMs)

            // Time passes, we keep trying to send, but the node is not ready.
            time.sleep(10)
            time2 = time.milliseconds()
            sender.runOnce()
            assertEquals(
                expected = 1,
                actual = client.inFlightRequestCount(),
                message = "We should have a single produce request in flight.",
            )

            // The ready time should move forward, but drain time shouldn't change.
            assertEquals(expected = time1, actual = stats.drainTimeMs)
            assertEquals(expected = time2, actual = stats.readyTimeMs)

            // Finally, time passes beyond the throttle and the node is ready.
            time.sleep(100)
            time2 = time.milliseconds()
            sender.runOnce()
            assertEquals(
                expected = 2,
                actual = client.inFlightRequestCount(),
                message = "We should have 2 produce requests in flight."
            )

            // Both times should move forward
            assertEquals(expected = time2, actual = stats.drainTimeMs)
            assertEquals(expected = time2, actual = stats.readyTimeMs)
        }
    }

    @Test
    fun testInitProducerIdRequest() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(
            expected = producerId,
            actual = transactionManager.producerIdAndEpoch.producerId
        )
        assertEquals(expected = 0, actual = transactionManager.producerIdAndEpoch.epoch)
    }

    /**
     * Verifies that InitProducerId of transactional producer succeeds even if metadata requests
     * are pending with only one bootstrap node available and maxInFlight=1, where multiple
     * polls are necessary to send requests.
     */
    @Test
    @Throws(Exception::class)
    fun testInitProducerIdWithMaxInFlightOne() {
        val producerId = 123456L
        createMockClientWithMaxFlightOneMetadataPending()

        // Initialize transaction manager. InitProducerId will be queued up until metadata response
        // is processed and FindCoordinator can be sent to `leastLoadedNode`.
        val transactionManager = TransactionManager(
            logContext = LogContext(),
            transactionalId = "testInitProducerIdWithPendingMetadataRequest",
            transactionTimeoutMs = 60000,
            retryBackoffMs = 100L,
            apiVersions = ApiVersions(),
        )
        setupWithTransactionState(
            transactionManager = transactionManager,
            guaranteeOrder = false,
            customPool = null,
            updateMetadata = false,
        )
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId, 0.toShort())
        transactionManager.initializeTransactions()
        sender.runOnce()

        // Process metadata response, prepare FindCoordinator and InitProducerId responses.
        // Verify producerId after the sender is run to process responses.
        val metadataUpdate = RequestTestUtils.metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = emptyMap(),
        )
        client.respond(metadataUpdate)
        prepareFindCoordinatorResponse(Errors.NONE, "testInitProducerIdWithPendingMetadataRequest")
        prepareInitProducerResponse(
            error = Errors.NONE,
            producerId = producerIdAndEpoch.producerId,
            producerEpoch = producerIdAndEpoch.epoch,
        )
        waitForProducerId(transactionManager, producerIdAndEpoch)
    }

    /**
     * Verifies that InitProducerId of idempotent producer succeeds even if metadata requests
     * are pending with only one bootstrap node available and maxInFlight=1, where multiple
     * polls are necessary to send requests.
     */
    @Test
    @Throws(Exception::class)
    fun testIdempotentInitProducerIdWithMaxInFlightOne() {
        val producerId = 123456L
        createMockClientWithMaxFlightOneMetadataPending()

        // Initialize transaction manager. InitProducerId will be queued up until metadata response
        // is processed.
        val transactionManager = createTransactionManager()
        setupWithTransactionState(
            transactionManager = transactionManager,
            guaranteeOrder = false,
            customPool = null,
            updateMetadata = false
        )
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId, 0.toShort())

        // Process metadata and InitProducerId responses.
        // Verify producerId after the sender is run to process responses.
        val metadataUpdate = RequestTestUtils.metadataUpdateWith(
            numNodes = 1,
            topicPartitionCounts = emptyMap(),
        )
        client.respond(metadataUpdate)
        sender.runOnce()
        sender.runOnce()
        client.respond(
            initProducerIdResponse(
                producerId = producerIdAndEpoch.producerId,
                producerEpoch = producerIdAndEpoch.epoch,
                error = Errors.NONE,
            )
        )
        waitForProducerId(transactionManager, producerIdAndEpoch)
    }

    /**
     * Tests the code path where the target node to send FindCoordinator or InitProducerId
     * is not ready.
     */
    @Test
    fun testNodeNotReady() {
        val producerId = 123456L
        time = MockTime(10)
        client = MockClient(time, metadata)
        val transactionManager = TransactionManager(
            logContext = LogContext(),
            transactionalId = "testNodeNotReady",
            transactionTimeoutMs = 60000,
            retryBackoffMs = 100L,
            apiVersions = ApiVersions(),
        )
        setupWithTransactionState(
            transactionManager = transactionManager,
            guaranteeOrder = false,
            customPool = null,
            updateMetadata = true,
        )
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId, 0.toShort())
        transactionManager.initializeTransactions()
        sender.runOnce()
        val node = metadata.fetch().nodes[0]
        client.delayReady(node, (REQUEST_TIMEOUT + 20).toLong())
        prepareFindCoordinatorResponse(Errors.NONE, "testNodeNotReady")
        sender.runOnce()
        sender.runOnce()
        assertNotNull(
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
            message = "Coordinator not found",
        )
        client.throttle(node, (REQUEST_TIMEOUT + 20).toLong())
        prepareFindCoordinatorResponse(Errors.NONE, "Coordinator not found")
        prepareInitProducerResponse(
            error = Errors.NONE,
            producerId = producerIdAndEpoch.producerId,
            producerEpoch = producerIdAndEpoch.epoch,
        )
        waitForProducerId(transactionManager, producerIdAndEpoch)
    }

    @Test
    @Throws(Exception::class)
    fun testClusterAuthorizationExceptionInInitProducerIdRequest() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.CLUSTER_AUTHORIZATION_FAILED)
        assertFalse(transactionManager.hasProducerId)
        assertTrue(transactionManager.hasError)
        assertTrue(transactionManager.lastError() is ClusterAuthorizationException)

        // cluster authorization is a fatal error for the producer
        assertSendFailure(ClusterAuthorizationException::class.java)
    }

    @Test
    @Throws(Exception::class)
    fun testCanRetryWithoutIdempotence() {
        // do a successful retry
        val future: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp0,
            timestamp = 0L,
            key = "key",
            value = "value",
        )
        sender.runOnce() // connect
        sender.runOnce() // send produce request
        val id = client.requests().peek().destination
        val node = Node(id.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertTrue(client.hasInFlightRequests())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        assertTrue(
            actual = client.isReady(node, time.milliseconds()),
            message = "Client ready status should be true"
        )
        assertFalse(future!!.isDone)
        client.respond(
            { body ->
                val request = body as ProduceRequest?
                assertFalse(RequestTestUtils.hasIdempotentRecords(request))
                true
            }, produceResponse(
                tp = tp0,
                offset = -1L,
                error = Errors.TOPIC_AUTHORIZATION_FAILED,
                throttleTimeMs = 0
            )
        )
        sender.runOnce()
        assertTrue(future.isDone)
        try {
            future.get()
        } catch (e: Exception) {
            assertTrue(e.cause is TopicAuthorizationException)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testIdempotenceWithMultipleInflights() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = null, actual = transactionManager.lastAckedSequence(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = null, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(request1!!.isDone)
        assertFalse(request2!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0L,
        )
        sender.runOnce() // receive response 0
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request1.isDone)
        assertEquals(expected = 0, actual = request1.get().offset)
        assertFalse(request2.isDone)
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1L,
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        assertTrue(request2.isDone)
        assertEquals(expected = 1, actual = request2.get().offset)
    }

    @Test
    @Throws(Exception::class)
    fun testIdempotenceWithMultipleInflightsRetriedInOrder() {
        // Send multiple in flight requests, retry them all one at a time, in the correct order.
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = null, actual = transactionManager.lastAckedSequence(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()

        // Send third ProduceRequest
        val request3: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 3, actual = client.inFlightRequestCount())
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertFalse(request1!!.isDone)
        assertFalse(request2!!.isDone)
        assertFalse(request3!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.LEADER_NOT_AVAILABLE,
            responseOffset = -1L,
        )
        sender.runOnce() // receive response 0

        // Queue the fourth request, it shouldn't be sent until the first 3 complete.
        val request4: Future<RecordMetadata>? = appendToAccumulator(tp0)
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertNull(actual = transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = -1L,
        )
        sender.runOnce() // re send request 1, receive response 2
        sendIdempotentProducerResponse(
            expectedSequence = 2,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = -1L,
        )
        sender.runOnce() // receive response 3
        assertEquals(expected = null, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        sender.runOnce() // Do nothing, we are reduced to one in flight request during retries.
        // the batch for request 4 shouldn't have been drained, and hence the sequence should not have been incremented.
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0L,
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request1.isDone)
        assertEquals(expected = 0, actual = request1.get().offset)
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        sender.runOnce() // send request 2;
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1L,
        )
        sender.runOnce() // receive response 2
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request2.isDone)
        assertEquals(expected = 1, actual = request2.get().offset)
        assertFalse(client.hasInFlightRequests())
        assertEquals(0, sender.inFlightBatches(tp0).size)
        sender.runOnce() // send request 3
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        sendIdempotentProducerResponse(
            expectedSequence = 2,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 2L,
        )
        sender.runOnce() // receive response 3, send request 4 since we are out of 'retry' mode.
        assertEquals(expected = 2, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request3.isDone)
        assertEquals(expected = 2, actual = request3.get().offset)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        sendIdempotentProducerResponse(
            expectedSequence = 3,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 3L,
        )
        sender.runOnce() // receive response 4
        assertEquals(expected = 3, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request4!!.isDone)
        assertEquals(expected = 3, actual = request4.get().offset)
    }

    @Test
    @Throws(Exception::class)
    fun testIdempotenceWithMultipleInflightsWhereFirstFailsFatallyAndSequenceOfFutureBatchesIsAdjusted() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(0, transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertFalse(request1!!.isDone)
        assertFalse(request2!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.MESSAGE_TOO_LARGE,
            responseOffset = -1L,
        )
        sender.runOnce() // receive response 0, should adjust sequences of future batches.
        assertFutureFailure(request1, RecordTooLargeException::class.java)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = -1L,
        )
        sender.runOnce() // receive response 1
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        sender.runOnce() // resend request 1
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0L,
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertTrue(request1.isDone)
        assertEquals(expected = 0, actual = request2.get().offset)
    }

    @Test
    @Throws(Exception::class)
    fun testEpochBumpOnOutOfOrderSequenceForNextBatch() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest with multiple messages.
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())

        // make sure the next sequence number accounts for multi-message batches.
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0,
        )
        sender.runOnce()

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request1!!.isDone)
        assertEquals(expected = 0, actual = request1.get().offset)
        assertFalse(request2!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))

        // This OutOfOrderSequence triggers an epoch bump since it is returned for the batch succeeding the last acknowledged batch.
        sendIdempotentProducerResponse(
            expectedSequence = 2,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = -1L,
        )
        sender.runOnce()
        sender.runOnce()

        // epoch should be bumped and sequence numbers reset
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch.toInt())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.firstInFlightSequence(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testEpochBumpOnOutOfOrderSequenceForNextBatchWhenThereIsNoBatchInFlight() {
        // Verify that partitions without in-flight batches when the producer epoch
        // is bumped get their sequence number reset correctly.
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)

        // Init producer id/epoch
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertEquals(producerId, transactionManager.producerIdAndEpoch.producerId)
        assertEquals(0, transactionManager.producerIdAndEpoch.epoch.toInt())

        // Partition 0 - Send first batch
        appendToAccumulator(tp0)
        sender.runOnce()

        // Partition 0 - State is lazily initialized
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )

        // Partition 0 - Successful response
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 0 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 1 - Send first batch
        appendToAccumulator(tp1)
        sender.runOnce()

        // Partition 1 - State is lazily initialized
        assertPartitionState(
            transactionManager,
            tp1,
            producerId,
            0.toShort(),
            1,
            null
        )

        // Partition 1 - Successful response
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 0,
            tp = tp1,
            responseError = Errors.NONE,
            responseOffset = 0,
            logStartOffset = -1
        )
        sender.runOnce()

        // Partition 1 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 0 - Send second batch
        appendToAccumulator(tp0)
        sender.runOnce()

        // Partition 0 - Sequence is incremented
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )

        // Partition 0 - Failed response with OUT_OF_ORDER_SEQUENCE_NUMBER
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = -1,
            logStartOffset = -1,
        )
        sender.runOnce() // Receive
        sender.runOnce() // Bump epoch & Retry

        // Producer epoch is bumped
        assertEquals(1, transactionManager.producerIdAndEpoch.epoch.toInt())

        // Partition 0 - State is reset to current producer epoch
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )

        // Partition 1 - State is not changed
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1))

        // Partition 0 - Successful Response
        sendIdempotentProducerResponse(
            expectedEpoch = 1,
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 0 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 1 - Send second batch
        appendToAccumulator(tp1)
        sender.runOnce()

        // Partition 1 - Epoch is bumped, sequence is reset and incremented
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )
        assertFalse(transactionManager.hasStaleProducerIdAndEpoch(tp1))

        // Partition 1 - Successful Response
        sendIdempotentProducerResponse(
            expectedEpoch = 1,
            expectedSequence = 0,
            tp = tp1,
            responseError = Errors.NONE,
            responseOffset = 1,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 1 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testEpochBumpOnOutOfOrderSequenceForNextBatchWhenBatchInFlightFails() {
        // When a batch failed after the producer epoch is bumped, the sequence number of
        // that partition must be reset for any subsequent batches sent.
        val producerId = 343434L
        val transactionManager = createTransactionManager()

        // Retries once
        setupWithTransactionState(
            transactionManager = transactionManager,
            guaranteeOrder = false,
            customPool = null,
            updateMetadata = true,
            retries = 1,
            lingerMs = 0,
        )

        // Init producer id/epoch
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertEquals(
            expected = producerId,
            actual = transactionManager.producerIdAndEpoch.producerId,
        )
        assertEquals(expected = 0, actual = transactionManager.producerIdAndEpoch.epoch.toInt())

        // Partition 0 - Send first batch
        appendToAccumulator(tp0)
        sender.runOnce()

        // Partition 0 - State is lazily initialized
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )

        // Partition 0 - Successful response
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 0 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 1 - Send first batch
        appendToAccumulator(tp1)
        sender.runOnce()

        // Partition 1 - State is lazily initialized
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )

        // Partition 1 - Successful response
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 0,
            tp = tp1,
            responseError = Errors.NONE,
            responseOffset = 0,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 1 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 0 - Send second batch
        appendToAccumulator(tp0)
        sender.runOnce()

        // Partition 0 - Sequence is incremented
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )

        // Partition 1 - Send second batch
        appendToAccumulator(tp1)
        sender.runOnce()

        // Partition 1 - Sequence is incremented
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0.toShort(),
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )

        // Partition 0 - Failed response with OUT_OF_ORDER_SEQUENCE_NUMBER
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = -1,
            logStartOffset = -1,
        )
        sender.runOnce() // Receive
        sender.runOnce() // Bump epoch & Retry

        // Producer epoch is bumped
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch.toInt())

        // Partition 0 - State is reset to current producer epoch
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1,
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )

        // Partition 1 - State is not changed. The epoch will be lazily bumped when all in-flight
        // batches are completed
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0,
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1))

        // Partition 1 - Failed response with NOT_LEADER_OR_FOLLOWER
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 1,
            tp = tp1,
            responseError = Errors.NOT_LEADER_OR_FOLLOWER,
            responseOffset = -1,
            logStartOffset = -1,
        )
        sender.runOnce() // Receive & Retry

        // Partition 1 - State is not changed.
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0,
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1))

        // Partition 0 - Successful Response
        sendIdempotentProducerResponse(
            expectedEpoch = 1,
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 0 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1,
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 1 - Failed response with NOT_LEADER_OR_FOLLOWER
        sendIdempotentProducerResponse(
            expectedEpoch = 0,
            expectedSequence = 1,
            tp = tp1,
            responseError = Errors.NOT_LEADER_OR_FOLLOWER,
            responseOffset = -1,
            logStartOffset = -1,
        )
        sender.runOnce() // Receive & Fail the batch (retries exhausted)

        // Partition 1 - State is not changed. It will be lazily updated when the next batch is sent.
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 0,
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1))

        // Partition 1 - Send third batch
        appendToAccumulator(tp1)
        sender.runOnce()

        // Partition 1 - Epoch is bumped, sequence is reset
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1,
            expectedSequenceValue = 1,
            expectedLastAckedSequence = null,
        )
        assertFalse(transactionManager.hasStaleProducerIdAndEpoch(tp1))

        // Partition 1 - Successful Response
        sendIdempotentProducerResponse(
            expectedEpoch = 1,
            expectedSequence = 0,
            tp = tp1,
            responseError = Errors.NONE,
            responseOffset = 0,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 1 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp1,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1,
            expectedSequenceValue = 1,
            expectedLastAckedSequence = 0,
        )

        // Partition 0 - Send third batch
        appendToAccumulator(tp0)
        sender.runOnce()

        // Partition 0 - Sequence is incremented
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1,
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 0,
        )

        // Partition 0 - Successful Response
        sendIdempotentProducerResponse(
            expectedEpoch = 1,
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0,
            logStartOffset = -1,
        )
        sender.runOnce()

        // Partition 0 - Last ack is updated
        assertPartitionState(
            transactionManager = transactionManager,
            tp = tp0,
            expectedProducerId = producerId,
            expectedProducerEpoch = 1,
            expectedSequenceValue = 2,
            expectedLastAckedSequence = 1,
        )
    }

    private fun assertPartitionState(
        transactionManager: TransactionManager,
        tp: TopicPartition,
        expectedProducerId: Long,
        expectedProducerEpoch: Short,
        expectedSequenceValue: Long,
        expectedLastAckedSequence: Int?,
    ) {
        assertEquals(
            expectedProducerId,
            transactionManager.producerIdAndEpoch(tp).producerId,
            "Producer Id:"
        )
        assertEquals(
            expected = expectedProducerEpoch,
            actual = transactionManager.producerIdAndEpoch(tp).epoch,
            message = "Producer Epoch:",
        )
        assertEquals(
            expected = expectedSequenceValue,
            actual = transactionManager.sequenceNumber(tp).toLong(),
            message = "Seq Number:",
        )
        assertEquals(
            expected = expectedLastAckedSequence,
            actual = transactionManager.lastAckedSequence(tp),
            message = "Last Acked Seq Number:",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCorrectHandlingOfOutOfOrderResponses() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertFalse(request1!!.isDone)
        assertFalse(request2!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))
        val firstClientRequest = client.requests().peek()
        val secondClientRequest = client.requests().toTypedArray()[1] as ClientRequest
        client.respondToRequest(
            secondClientRequest,
            produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
                throttleTimeMs = -1
            )
        )
        sender.runOnce() // receive response 1
        val queuedBatches = accumulator.getDeque(tp0)

        // Make sure that we are queueing the second batch first.
        assertEquals(expected = 1, actual = queuedBatches!!.size)
        assertEquals(expected = 1, actual = queuedBatches.peekFirst().baseSequence)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertNull(transactionManager.lastAckedSequence(tp0))
        client.respondToRequest(
            clientRequest = firstClientRequest,
            response = produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                throttleTimeMs = -1,
            )
        )
        sender.runOnce() // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(expected = 2, actual = queuedBatches.size)
        assertEquals(expected = 0, actual = queuedBatches.peekFirst().baseSequence)
        assertEquals(expected = 1, actual = queuedBatches.peekLast().baseSequence)
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertFalse(request1.isDone)
        assertFalse(request2.isDone)
        sender.runOnce() // send request 0
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        sender.runOnce() // don't do anything, only one inflight allowed once we are retrying.
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertNull(transactionManager.lastAckedSequence(tp0))

        // Make sure that the requests are sent in order, even though the previous responses were not in order.
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0L,
        )
        sender.runOnce() // receive response 0
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertTrue(request1.isDone)
        assertEquals(expected = 0, actual = request1.get().offset)
        sender.runOnce() // send request 1
        assertEquals(1, client.inFlightRequestCount())
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1L,
        )
        sender.runOnce() // receive response 1
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertTrue(request2.isDone)
        assertEquals(expected = 1, actual = request2.get().offset)
    }

    @Test
    @Throws(Exception::class)
    fun testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(2, client.inFlightRequestCount())
        assertFalse(request1!!.isDone)
        assertFalse(request2!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))
        val firstClientRequest = client.requests().peek()
        val secondClientRequest = client.requests().toTypedArray()[1] as ClientRequest
        client.respondToRequest(
            clientRequest = secondClientRequest,
            response = produceResponse(
                tp = tp0,
                offset = 1,
                error = Errors.NONE,
                throttleTimeMs = 1,
            )
        )
        sender.runOnce() // receive response 1
        assertTrue(request2.isDone)
        assertEquals(expected = 1, actual = request2.get().offset)
        assertFalse(request1.isDone)
        val queuedBatches = accumulator.getDeque(tp0)
        assertEquals(expected = 0, actual = queuedBatches!!.size)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        client.respondToRequest(
            clientRequest = firstClientRequest,
            response = produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.REQUEST_TIMED_OUT,
                throttleTimeMs = -1,
            )
        )
        sender.runOnce() // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(expected = 1, actual = queuedBatches.size)
        assertEquals(expected = 0, actual = queuedBatches.peekFirst().baseSequence)
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        sender.runOnce() // resend request 0
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))

        // Make sure we handle the out of order successful responses correctly.
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 0L,
        )
        sender.runOnce() // receive response 0
        assertEquals(expected = 0, actual = queuedBatches.size)
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertFalse(client.hasInFlightRequests())
        assertTrue(request1.isDone)
        assertEquals(expected = 0, actual = request1.get().offset)
    }

    @Test
    @Throws(Exception::class)
    fun testExpiryOfUnsentBatchesShouldNotCauseUnresolvedSequences() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp0,
            timestamp = 0L,
            key = "key",
            value = "value",
        )
        val node = metadata.fetch().nodes[0]
        time.sleep(10000L)
        client.disconnect(node.idString())
        client.backoff(node, 10)
        sender.runOnce()
        assertFutureFailure(request1, TimeoutException::class.java)
        assertFalse(transactionManager.hasUnresolvedSequence(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(
            transactionManager = transactionManager,
            guaranteeOrder = false,
            customPool = null,
        )
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        // We separate the two appends by 1 second so that the two batches
        // don't expire at the same time.
        time.sleep(1000L)
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 2, actual = sender.inFlightBatches(tp0).size)
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.REQUEST_TIMED_OUT,
            responseOffset = -1,
        )
        sender.runOnce() // receive first response
        assertEquals(1, sender.inFlightBatches(tp0).size)
        val node = metadata.fetch().nodes[0]
        // We add 600 millis to expire the first batch but not the second.
        // Note deliveryTimeoutMs is 1500.
        time.sleep(600L)
        client.disconnect(node.idString())
        client.backoff(node, 10)
        sender.runOnce() // now expire the first batch.
        assertFutureFailure(request1, TimeoutException::class.java)
        assertTrue(transactionManager.hasUnresolvedSequence(tp0))
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)

        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        val request3: Future<RecordMetadata>? = appendToAccumulator(tp0)
        time.sleep(20)
        assertFalse(request2!!.isDone)
        sender.runOnce() // send second request
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1,
        )
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        sender.runOnce() // receive second response, the third request shouldn't be sent since we are in an unresolved state.
        assertTrue(request2.isDone)
        assertEquals(expected = 1, actual = request2.get().offset)
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        val batches = accumulator.getDeque(tp0)
        assertEquals(expected = 1, actual = batches!!.size)
        assertFalse(batches.peekFirst().hasSequence)
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertTrue(transactionManager.hasUnresolvedSequence(tp0))
        sender.runOnce() // clear the unresolved state, send the pending request.
        assertFalse(transactionManager.hasUnresolvedSequence(tp0))
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = batches.size)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertFalse(request3!!.isDone)
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
    }

    @Test
    @Throws(Exception::class)
    fun testExpiryOfFirstBatchShouldCauseEpochBumpIfFutureBatchesFail() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        time.sleep(1000L)
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(2, client.inFlightRequestCount())
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NOT_LEADER_OR_FOLLOWER,
            responseOffset = -1,
        )
        sender.runOnce() // receive first response
        val node = metadata.fetch().nodes[0]
        time.sleep(1000L)
        client.disconnect(node.idString())
        client.backoff(node, 10)
        sender.runOnce() // now expire the first batch.
        assertFutureFailure(request1, TimeoutException::class.java)
        assertTrue(transactionManager.hasUnresolvedSequence(tp0))
        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        appendToAccumulator(tp0)
        time.sleep(20)
        assertFalse(request2!!.isDone)
        sender.runOnce() // send second request
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
            responseOffset = 1,
        )
        sender.runOnce() // receive second response, the third request shouldn't be sent since we are in an unresolved state.
        val batches = accumulator.getDeque(tp0)

        // The epoch should be bumped and the second request should be requeued
        assertEquals(2, batches!!.size)
        sender.runOnce()
        assertEquals(1.toShort(), transactionManager.producerIdAndEpoch.epoch)
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(transactionManager.hasUnresolvedSequence(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testUnresolvedSequencesAreNotFatal() {
        val producerIdAndEpoch = ProducerIdAndEpoch(123456L, 0.toShort())
        apiVersions.update(
            nodeId = "0",
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.INIT_PRODUCER_ID.id,
                minVersion = 0,
                maxVersion = 3,
            )
        )
        val txnManager = TransactionManager(
            logContext = logContext,
            transactionalId = "testUnresolvedSeq",
            transactionTimeoutMs = 60000,
            retryBackoffMs = 100,
            apiVersions = apiVersions,
        )
        setupWithTransactionState(txnManager)
        doInitTransactions(txnManager, producerIdAndEpoch)
        txnManager.beginTransaction()
        txnManager.maybeAddPartition(tp0)
        client.prepareResponse(
            response = AddPartitionsToTxnResponse(
                throttleTimeMs = 0,
                errors = mapOf(tp0 to Errors.NONE),
            )
        )
        sender.runOnce()

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        time.sleep(1000L)
        appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(2, client.inFlightRequestCount())
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NOT_LEADER_OR_FOLLOWER,
            responseOffset = -1,
        )
        sender.runOnce() // receive first response
        val node = metadata.fetch().nodes[0]
        time.sleep(1000L)
        client.disconnect(node.idString())
        client.backoff(node, 10)
        sender.runOnce() // now expire the first batch.
        assertFutureFailure(request1, TimeoutException::class.java)
        assertTrue(txnManager.hasUnresolvedSequence(tp0))

        // Loop once and confirm that the transaction manager does not enter a fatal error state
        sender.runOnce()
        assertTrue(txnManager.hasAbortableError)
    }

    @Test
    @Throws(Exception::class)
    fun testExpiryOfAllSentBatchesShouldCauseUnresolvedSequences() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0, 0L, "key", "value")
        sender.runOnce() // send request
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NOT_LEADER_OR_FOLLOWER,
            responseOffset = -1,
        )
        sender.runOnce() // receive response
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        val node = metadata.fetch().nodes[0]
        time.sleep(15000L)
        client.disconnect(node.idString())
        client.backoff(node, 10)
        sender.runOnce() // now expire the batch.
        assertFutureFailure(request1, TimeoutException::class.java)
        assertTrue(transactionManager.hasUnresolvedSequence(tp0))
        assertFalse(client.hasInFlightRequests())
        val batches = accumulator.getDeque(tp0)
        assertEquals(expected = 0, actual = batches!!.size)
        assertEquals(
            expected = producerId,
            actual = transactionManager.producerIdAndEpoch.producerId,
        )

        // In the next run loop, we bump the epoch and clear the unresolved sequences
        sender.runOnce()
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch.toInt())
        assertFalse(transactionManager.hasUnresolvedSequence(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testResetOfProducerStateShouldAllowQueuedBatchesToDrain() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Short.MAX_VALUE, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val maxRetries = 10
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = maxRetries,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        appendToAccumulator(tp0) // failed response
        val successfulResponse: Future<RecordMetadata>? = appendToAccumulator(tp1)
        sender.runOnce() // connect and send.
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        val responses: MutableMap<TopicPartition, OffsetAndError> = LinkedHashMap()
        responses[tp1] = OffsetAndError(offset = -1, error = Errors.NOT_LEADER_OR_FOLLOWER)
        responses[tp0] = OffsetAndError(offset = -1, error = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER)
        client.respond(produceResponse(responses))
        sender.runOnce() // trigger epoch bump
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE) // also send request to tp1
        sender.runOnce() // reset producer ID because epoch is maxed out
        assertEquals(
            expected = producerId + 1,
            actual = transactionManager.producerIdAndEpoch.producerId,
        )
        assertFalse(successfulResponse!!.isDone)
        client.respond(
            produceResponse(
                tp = tp1,
                offset = 10,
                error = Errors.NONE,
                throttleTimeMs = -1
            )
        )
        sender.runOnce()
        assertTrue(successfulResponse.isDone)
        assertEquals(expected = 10, actual = successfulResponse.get().offset)

        // The epoch and the sequence are updated when the next batch is sent.
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp1))
    }

    @Test
    @Throws(Exception::class)
    fun testCloseWithProducerIdReset() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Short.MAX_VALUE, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = 10,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        appendToAccumulator(tp0) // failed response
        appendToAccumulator(tp1) // success response
        sender.runOnce() // connect and send.
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        val responses: MutableMap<TopicPartition, OffsetAndError> = LinkedHashMap()
        responses[tp1] = OffsetAndError(offset = -1, error = Errors.NOT_LEADER_OR_FOLLOWER)
        responses[tp0] = OffsetAndError(offset = -1, error = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER)
        client.respond(produceResponse(responses))
        sender.initiateClose() // initiate close
        sender.runOnce() // out of order sequence error triggers producer ID reset because epoch is maxed out
        waitForCondition(
            testCondition = {
                prepareInitProducerResponse(
                    error = Errors.NONE,
                    producerId = producerId + 1,
                    producerEpoch = 1,
                )
                sender.runOnce()
                !accumulator.hasUndrained()
            },
            maxWaitMs = 5000,
            conditionDetails = "Failed to drain batches",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testForceCloseWithProducerIdReset() {
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(1L, Short.MAX_VALUE, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = 10,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        val failedResponse: Future<RecordMetadata>? = appendToAccumulator(tp0)
        val successfulResponse: Future<RecordMetadata>? = appendToAccumulator(tp1)
        sender.runOnce() // connect and send.
        assertEquals(1, client.inFlightRequestCount())
        val responses: MutableMap<TopicPartition, OffsetAndError> = LinkedHashMap()
        responses[tp1] = OffsetAndError(offset = -1, error = Errors.NOT_LEADER_OR_FOLLOWER)
        responses[tp0] = OffsetAndError(offset = -1, error = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER)
        client.respond(produceResponse(responses))
        sender.runOnce() // out of order sequence error triggers producer ID reset because epoch is maxed out
        sender.forceClose() // initiate force close
        sender.runOnce() // this should not block
        sender.run() // run main loop to test forceClose flag
        assertFalse(accumulator.hasUndrained(), "Pending batches are not aborted.")
        assertTrue(successfulResponse!!.isDone)
    }

    @Test
    @Throws(Exception::class)
    fun testBatchesDrainedWithOldProducerIdShouldSucceedOnSubsequentRetry() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val maxRetries = 10
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = maxRetries,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        val outOfOrderResponse: Future<RecordMetadata>? = appendToAccumulator(tp0)
        val successfulResponse: Future<RecordMetadata>? = appendToAccumulator(tp1)
        sender.runOnce() // connect.
        sender.runOnce() // send.
        assertEquals(1, client.inFlightRequestCount())
        val responses: MutableMap<TopicPartition, OffsetAndError> = LinkedHashMap()
        responses[tp1] = OffsetAndError(offset = -1, error = Errors.NOT_LEADER_OR_FOLLOWER)
        responses[tp0] = OffsetAndError(offset = -1, error = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER)
        client.respond(produceResponse(responses))
        sender.runOnce()
        assertFalse(outOfOrderResponse!!.isDone)
        sender.runOnce() // bump epoch send request to tp1 with the old producerId
        assertEquals(1, transactionManager.producerIdAndEpoch.epoch.toInt())
        assertFalse(successfulResponse!!.isDone)
        // The response comes back with a retriable error.
        client.respond(
            produceResponse(
                tp = tp1,
                offset = 0,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                throttleTimeMs = -1
            )
        )
        sender.runOnce()

        // The response
        assertFalse(successfulResponse.isDone)
        sender.runOnce() // retry one more time
        client.respond(
            produceResponse(
                tp = tp1,
                offset = 0,
                error = Errors.NONE,
                throttleTimeMs = -1,
            )
        )
        sender.runOnce()
        assertTrue(successfulResponse.isDone)
        // epoch of partition is bumped and sequence is reset when the next batch is sent
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp1))
    }

    @Test
    @Throws(Exception::class)
    fun testCorrectHandlingOfDuplicateSequenceError() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(0, transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val nodeId = client.requests().peek().destination
        val node = Node(nodeId.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertFalse(request1!!.isDone)
        assertFalse(request2!!.isDone)
        assertTrue(client.isReady(node, time.milliseconds()))
        val firstClientRequest = client.requests().peek()
        val secondClientRequest = client.requests().toTypedArray()[1] as ClientRequest
        client.respondToRequest(
            clientRequest = secondClientRequest, response = produceResponse(
                tp = tp0,
                offset = 1000,
                error = Errors.NONE,
                throttleTimeMs = 0
            )
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 1000, actual = transactionManager.lastAckedOffset(tp0))
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        client.respondToRequest(
            clientRequest = firstClientRequest,
            response = produceResponse(
                tp = tp0,
                offset = ProduceResponse.INVALID_OFFSET,
                error = Errors.DUPLICATE_SEQUENCE_NUMBER,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce() // receive response 0

        // Make sure that the last ack'd sequence doesn't change.
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1000, actual = transactionManager.lastAckedOffset(tp0))
        assertFalse(client.hasInFlightRequests())
        val unknownMetadata = request1.get()
        assertFalse(unknownMetadata.hasOffset())
        assertEquals(expected = -1L, actual = unknownMetadata.offset)
    }

    @Test
    @Throws(Exception::class)
    fun testTransactionalUnknownProducerHandlingWhenRetentionLimitReached() {
        val producerId = 343434L
        val transactionManager =
            TransactionManager(logContext, "testUnresolvedSeq", 60000, 100, apiVersions)
        setupWithTransactionState(transactionManager)
        doInitTransactions(transactionManager, ProducerIdAndEpoch(producerId, 0.toShort()))
        assertTrue(transactionManager.hasProducerId)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        client.prepareResponse(
            response = AddPartitionsToTxnResponse(
                throttleTimeMs = 0,
                errors = mapOf(tp0 to Errors.NONE),
            )
        )
        sender.runOnce() // Receive AddPartitions response
        assertEquals(0, transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1000L,
            logStartOffset = 10L,
        )
        sender.runOnce() // receive the response.
        assertTrue(request1!!.isDone)
        assertEquals(expected = 1000L, actual = request1.get().offset)
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1000L, actual = transactionManager.lastAckedOffset(tp0))

        // Send second ProduceRequest, a single batch with 2 records.
        appendToAccumulator(tp0)
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(request2!!.isDone)
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.UNKNOWN_PRODUCER_ID,
            responseOffset = -1L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 0, should be retried since the logStartOffset > lastAckedOffset.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(request2.isDone)
        assertFalse(client.hasInFlightRequests())
        sender.runOnce() // should retry request 1

        // resend the request. Note that the expected sequence is 0, since we have lost producer state on the broker.
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1011L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(client.hasInFlightRequests())
        assertTrue(request2.isDone)
        assertEquals(expected = 1012L, actual = request2.get().offset)
        assertEquals(expected = 1012L, actual = transactionManager.lastAckedOffset(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testIdempotentUnknownProducerHandlingWhenRetentionLimitReached() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(0, transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1000L,
            logStartOffset = 10L,
        )
        sender.runOnce() // receive the response.
        assertTrue(request1!!.isDone)
        assertEquals(1000L, request1.get().offset)
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1000L, actual = transactionManager.lastAckedOffset(tp0))

        // Send second ProduceRequest, a single batch with 2 records.
        appendToAccumulator(tp0)
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(request2!!.isDone)
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.UNKNOWN_PRODUCER_ID,
            responseOffset = -1L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 0, should be retried since the logStartOffset > lastAckedOffset.
        sender.runOnce() // bump epoch and retry request

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(request2.isDone)
        assertTrue(client.hasInFlightRequests())
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch)

        // resend the request. Note that the expected sequence is 0, since we have lost producer state on the broker.
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1011L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(client.hasInFlightRequests())
        assertTrue(request2.isDone)
        assertEquals(expected = 1012L, actual = request2.get().offset)
        assertEquals(expected = 1012L, actual = transactionManager.lastAckedOffset(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testUnknownProducerErrorShouldBeRetriedWhenLogStartOffsetIsUnknown() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1000L,
            logStartOffset = 10L,
        )
        sender.runOnce() // receive the response.
        assertTrue(request1!!.isDone)
        assertEquals(expected = 1000L, actual = request1.get().offset)
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1000L, actual = transactionManager.lastAckedOffset(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(request2!!.isDone)
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.UNKNOWN_PRODUCER_ID,
            responseOffset = -1L,
            logStartOffset = -1L
        )
        sender.runOnce() // receive response 0, should be retried without resetting the sequence numbers since the log start offset is unknown.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(request2.isDone)
        assertFalse(client.hasInFlightRequests())
        sender.runOnce() // should retry request 1

        // resend the request. Note that the expected sequence is 1, since we never got the logStartOffset in the previous
        // response and hence we didn't reset the sequence numbers.
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1011L,
            logStartOffset = 1010L
        )
        sender.runOnce() // receive response 1
        assertEquals(expected = 1, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(2, transactionManager.sequenceNumber(tp0))
        assertFalse(client.hasInFlightRequests())
        assertTrue(request2.isDone)
        assertEquals(expected = 1011L, actual = request2.get().offset)
        assertEquals(expected = 1011L, actual = transactionManager.lastAckedOffset(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testUnknownProducerErrorShouldBeRetriedForFutureBatchesWhenFirstFails() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(0, transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1000L,
            logStartOffset = 10L,
        )
        sender.runOnce() // receive the response.
        assertTrue(request1!!.isDone)
        assertEquals(expected = 1000L, actual = request1.get().offset)
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1000L, actual = transactionManager.lastAckedOffset(tp0))

        // Send second ProduceRequest
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))

        // Send the third ProduceRequest, in parallel with the second. It should be retried even though the
        // lastAckedOffset > logStartOffset when its UnknownProducerResponse comes back.
        val request3: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(request2!!.isDone)
        assertFalse(request3!!.isDone)
        assertEquals(2, client.inFlightRequestCount())
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.UNKNOWN_PRODUCER_ID,
            responseOffset = -1L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 2, should reset the sequence numbers and be retried.
        sender.runOnce() // bump epoch and retry request 2

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertFalse(request2.isDone)
        assertFalse(request3.isDone)
        assertEquals(expected = 2, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch)

        // receive the original response 3. note the expected sequence is still the originally assigned sequence.
        sendIdempotentProducerResponse(
            expectedSequence = 2,
            tp = tp0,
            responseError = Errors.UNKNOWN_PRODUCER_ID,
            responseOffset = -1,
            logStartOffset = 1010L
        )
        sender.runOnce() // receive response 3
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1011L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 2, don't send request 3 since we can have at most 1 in flight when retrying
        assertTrue(request2.isDone)
        assertFalse(request3.isDone)
        assertFalse(client.hasInFlightRequests())
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1011L, actual = request2.get().offset)
        assertEquals(expected = 1011L, actual = transactionManager.lastAckedOffset(tp0))
        sender.runOnce() // resend request 3.
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1012L,
            logStartOffset = 1010L,
        )
        sender.runOnce() // receive response 3.
        assertFalse(client.hasInFlightRequests())
        assertTrue(request3.isDone)
        assertEquals(expected = 1012L, actual = request3.get().offset)
        assertEquals(expected = 1012L, actual = transactionManager.lastAckedOffset(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testShouldRaiseOutOfOrderSequenceExceptionToUserIfLogWasNotTruncated() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        sendIdempotentProducerResponse(
            expectedSequence = 0,
            tp = tp0,
            responseError = Errors.NONE,
            responseOffset = 1000L,
            logStartOffset = 10L,
        )
        sender.runOnce() // receive the response.
        assertTrue(request1!!.isDone)
        assertEquals(expected = 1000L, actual = request1.get().offset)
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1000L, actual = transactionManager.lastAckedOffset(tp0))

        // Send second ProduceRequest,
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertFalse(request2!!.isDone)
        sendIdempotentProducerResponse(
            expectedSequence = 1,
            tp = tp0,
            responseError = Errors.UNKNOWN_PRODUCER_ID,
            responseOffset = -1L,
            logStartOffset = 10L,
        )
        sender.runOnce() // receive response 0, should request an epoch bump
        sender.runOnce() // bump epoch
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch.toInt())
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertFalse(request2.isDone)
    }

    fun sendIdempotentProducerResponse(
        expectedEpoch: Int = -1,
        expectedSequence: Int,
        tp: TopicPartition,
        responseError: Errors?,
        responseOffset: Long,
        logStartOffset: Long = -1L,
    ) {
        client.respond(
            matcher = { body: AbstractRequest? ->
                val produceRequest: ProduceRequest? = body as ProduceRequest?
                assertTrue(RequestTestUtils.hasIdempotentRecords(produceRequest))
                val records: MemoryRecords? = partitionRecords(produceRequest)[tp]
                val batchIterator: Iterator<MutableRecordBatch> = records!!.batches().iterator()
                val firstBatch: RecordBatch = batchIterator.next()
                assertFalse(batchIterator.hasNext())
                if (expectedEpoch > -1) assertEquals(
                    expected = expectedEpoch.toShort(),
                    actual = firstBatch.producerEpoch(),
                )
                assertEquals(expected = expectedSequence, actual = firstBatch.baseSequence())
                true
            },
            response = produceResponse(
                tp = tp,
                offset = responseOffset,
                error = responseError,
                throttleTimeMs = 0,
                logStartOffset = logStartOffset,
                errorMessage = null
            ),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testClusterAuthorizationExceptionInProduceRequest() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)

        // cluster authorization is a fatal error for the producer
        val future: Future<RecordMetadata>? = appendToAccumulator(tp0)
        client.prepareResponse(
            matcher = { body: AbstractRequest? ->
                body is ProduceRequest
                        && RequestTestUtils.hasIdempotentRecords(body as ProduceRequest?)
            },
            response = produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.CLUSTER_AUTHORIZATION_FAILED,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce()
        assertFutureFailure(future, ClusterAuthorizationException::class.java)

        // cluster authorization errors are fatal, so we should continue seeing it on future sends
        assertTrue(transactionManager.hasFatalError)
        assertSendFailure(ClusterAuthorizationException::class.java)
    }

    @Test
    @Throws(Exception::class)
    fun testCancelInFlightRequestAfterFatalError() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)

        // cluster authorization is a fatal error for the producer
        val future1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        val future2: Future<RecordMetadata>? = appendToAccumulator(tp1)
        sender.runOnce()
        client.respond(
            matcher = { body: AbstractRequest? ->
                body is ProduceRequest
                        && RequestTestUtils.hasIdempotentRecords(body as ProduceRequest?)
            },
            response = produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.CLUSTER_AUTHORIZATION_FAILED,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce()
        assertTrue(transactionManager.hasFatalError)
        assertFutureFailure(future1, ClusterAuthorizationException::class.java)
        sender.runOnce()
        assertFutureFailure(future2, ClusterAuthorizationException::class.java)

        // Should be fine if the second response eventually returns
        client.respond(
            matcher = { body ->
                body is ProduceRequest
                        && RequestTestUtils.hasIdempotentRecords(body as ProduceRequest?)
            },
            response = produceResponse(
                tp = tp1,
                offset = 0,
                error = Errors.NONE,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce()
    }

    @Test
    @Throws(Exception::class)
    fun testUnsupportedForMessageFormatInProduceRequest() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val future: Future<RecordMetadata>? = appendToAccumulator(tp0)
        client.prepareResponse(
            matcher = { body ->
                body is ProduceRequest
                        && RequestTestUtils.hasIdempotentRecords(body as ProduceRequest?)
            },
            response = produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce()
        assertFutureFailure(future, UnsupportedForMessageFormatException::class.java)

        // unsupported for message format is not a fatal error
        assertFalse(transactionManager.hasError)
    }

    @Test
    @Throws(Exception::class)
    fun testUnsupportedVersionInProduceRequest() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val future: Future<RecordMetadata>? = appendToAccumulator(tp0)
        client.prepareUnsupportedVersionResponse { body ->
            body is ProduceRequest
                    && RequestTestUtils.hasIdempotentRecords(body as ProduceRequest?)
        }
        sender.runOnce()
        assertFutureFailure(future, UnsupportedVersionException::class.java)

        // unsupported version errors are fatal, so we should continue seeing it on future sends
        assertTrue(transactionManager.hasFatalError)
        assertSendFailure(UnsupportedVersionException::class.java)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSequenceNumberIncrement() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val maxRetries = 10
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = maxRetries,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        val responseFuture: Future<RecordMetadata>? = appendToAccumulator(tp0)
        client.prepareResponse(
            matcher = { body ->
                if (body is ProduceRequest) {
                    val records = partitionRecords(body)[tp0]
                    val batchIterator = records!!.batches().iterator()
                    assertTrue(batchIterator.hasNext())
                    val batch = batchIterator.next()
                    assertFalse(batchIterator.hasNext())
                    assertEquals(expected = 0, actual = batch.baseSequence())
                    assertEquals(expected = producerId, actual = batch.producerId())
                    assertEquals(expected = 0, actual = batch.producerEpoch().toInt())
                    return@prepareResponse true
                }
                false
            },
            response = produceResponse(
                tp = tp0,
                offset = 0,
                error = Errors.NONE,
                throttleTimeMs = 0
            )
        )
        sender.runOnce() // connect.
        sender.runOnce() // send.
        sender.runOnce() // receive response
        assertTrue(responseFuture!!.isDone)
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = 1L, actual = transactionManager.sequenceNumber(tp0).toLong())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRetryWhenProducerIdChanges() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Short.MAX_VALUE, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val maxRetries = 10
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = maxRetries,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        val responseFuture: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // connect.
        sender.runOnce() // send.
        val id = client.requests().peek().destination
        val node = Node(id.toInt(), "localhost", 0)
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertTrue(
            actual = client.isReady(node, time.milliseconds()),
            message = "Client ready status should be true"
        )
        client.disconnect(id)
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertFalse(
            actual = client.isReady(node, time.milliseconds()),
            message = "Client ready status should be false"
        )
        sender.runOnce() // receive error
        sender.runOnce() // reset producer ID because epoch is maxed out
        prepareAndReceiveInitProducerId(producerId = producerId + 1, error = Errors.NONE)
        sender.runOnce() // nothing to do, since the pid has changed. We should check the metrics for errors.
        assertEquals(
            expected = 1,
            actual = client.inFlightRequestCount(),
            message = "Expected requests to be retried after pid change",
        )
        assertFalse(responseFuture!!.isDone)
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0).toLong())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testBumpEpochWhenOutOfOrderSequenceReceived() {
        val producerId = 343434L
        val transactionManager = createTransactionManager()
        setupWithTransactionState(transactionManager)
        prepareAndReceiveInitProducerId(producerId, Errors.NONE)
        assertTrue(transactionManager.hasProducerId)
        val maxRetries = 10
        val m = Metrics()
        val senderMetrics = SenderMetricsRegistry(m)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = maxRetries,
            metricsRegistry = senderMetrics,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        val responseFuture: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // connect.
        sender.runOnce() // send.
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        client.respond(
            produceResponse(
                tp = tp0,
                offset = 0,
                error = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce() // receive the out of order sequence error
        sender.runOnce() // bump the epoch
        assertFalse(responseFuture!!.isDone)
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        assertEquals(expected = 1, actual = transactionManager.producerIdAndEpoch.epoch.toInt())
    }

    @Test
    @Throws(Exception::class)
    fun testIdempotentSplitBatchAndSend() {
        val tp = TopicPartition("testSplitBatchAndSend", 1)
        val txnManager = createTransactionManager()
        val producerIdAndEpoch = ProducerIdAndEpoch(123456L, 0.toShort())
        setupWithTransactionState(txnManager)
        prepareAndReceiveInitProducerId(123456L, Errors.NONE)
        assertTrue(txnManager.hasProducerId)
        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tp)
    }

    @Test
    @Throws(Exception::class)
    fun testTransactionalSplitBatchAndSend() {
        val producerIdAndEpoch = ProducerIdAndEpoch(123456L, 0.toShort())
        val tp = TopicPartition("testSplitBatchAndSend", 1)
        val txnManager =
            TransactionManager(logContext, "testSplitBatchAndSend", 60000, 100, apiVersions)
        setupWithTransactionState(txnManager)
        doInitTransactions(txnManager, producerIdAndEpoch)
        txnManager.beginTransaction()
        txnManager.maybeAddPartition(tp)
        client.prepareResponse(
            response = AddPartitionsToTxnResponse(
                throttleTimeMs = 0,
                errors = mapOf(tp to Errors.NONE),
            )
        )
        sender.runOnce()
        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tp)
    }

    @Suppress("Deprecation")
    @Throws(Exception::class)
    private fun testSplitBatchAndSend(
        txnManager: TransactionManager,
        producerIdAndEpoch: ProducerIdAndEpoch,
        tp: TopicPartition
    ) {
        val maxRetries = 1
        val topic = tp.topic
        val deliveryTimeoutMs = 3000
        val totalSize = (1024 * 1024).toLong()
        val metricGrpName = "producer-metrics"
        // Set a good compression ratio.
        setEstimation(topic, CompressionType.GZIP, 0.2f)
        Metrics().use { m ->
            accumulator = RecordAccumulator(
                logContext = logContext,
                batchSize = batchSize,
                compression = CompressionType.GZIP,
                lingerMs = 0,
                retryBackoffMs = 0L,
                deliveryTimeoutMs = deliveryTimeoutMs,
                metrics = m,
                metricGrpName = metricGrpName,
                time = time,
                apiVersions = ApiVersions(),
                transactionManager = txnManager,
                free = BufferPool(
                    totalMemory = totalSize,
                    poolableSize = batchSize,
                    metrics = metrics,
                    time = time,
                    metricGrpName = "producer-internal-metrics",
                )
            )
            val senderMetrics = SenderMetricsRegistry(m)
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = true,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = maxRetries,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = 1000L,
                transactionManager = txnManager,
                apiVersions = ApiVersions(),
            )
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            val metadataUpdate1: MetadataResponse =
                RequestTestUtils.metadataUpdateWith(numNodes = 2, topicPartitionCounts = mapOf(topic to 2))
            client.prepareMetadataUpdate(metadataUpdate1)
            // Send the first message.
            val nowMs: Long = time.milliseconds()
            val cluster: Cluster = singletonCluster()
            val f1: Future<RecordMetadata>? = accumulator.append(
                topic = tp.topic,
                partition = tp.partition,
                timestamp = 0L,
                key = "key1".toByteArray(),
                value = ByteArray(batchSize / 2),
                headers = null,
                callbacks = null,
                maxTimeToBlock = MAX_BLOCK_TIMEOUT.toLong(),
                abortOnNewBatch = false,
                nowMs = nowMs,
                cluster = cluster,
            ).future
            val f2: Future<RecordMetadata>? = accumulator.append(
                topic = tp.topic,
                partition = tp.partition,
                timestamp = 0L,
                key = "key2".toByteArray(),
                value = ByteArray(batchSize / 2),
                headers = null,
                callbacks = null,
                maxTimeToBlock = MAX_BLOCK_TIMEOUT.toLong(),
                abortOnNewBatch = false,
                nowMs = nowMs,
                cluster = cluster,
            ).future
            sender.runOnce() // connect
            sender.runOnce() // send produce request
            assertEquals(
                expected = 2,
                actual = txnManager.sequenceNumber(tp),
                message = "The next sequence should be 2",
            )
            var id: String = client.requests().peek().destination()
            assertEquals(
                expected = ApiKeys.PRODUCE,
                actual = client.requests().peek().requestBuilder().apiKey,
            )
            var node: Node? = Node(id = id.toInt(), host = "localhost", port = 0)
            assertEquals(1, client.inFlightRequestCount())
            assertTrue(
                actual = client.isReady(node!!, time.milliseconds()),
                message = "Client ready status should be true",
            )
            val responseMap: MutableMap<TopicPartition, PartitionResponse> =
                HashMap()
            responseMap[tp] = PartitionResponse(Errors.MESSAGE_TOO_LARGE)
            client.respond(ProduceResponse(responseMap))
            sender.runOnce() // split and reenqueue
            assertEquals(
                expected = 2,
                actual = txnManager.sequenceNumber(tp),
                message = "The next sequence should be 2",
            )
            // The compression ratio should have been improved once.
            assertEquals(
                expected = (CompressionType.GZIP.rate - CompressionRatioEstimator.COMPRESSION_RATIO_IMPROVING_STEP).toDouble(),
                actual = estimation(topic, CompressionType.GZIP).toDouble(),
                absoluteTolerance = 0.01,
            )
            sender.runOnce() // send the first produce request
            assertEquals(
                expected = 2,
                actual = txnManager.sequenceNumber(tp),
                message = "The next sequence number should be 2",
            )
            assertFalse(f1!!.isDone, "The future shouldn't have been done.")
            assertFalse(f2!!.isDone, "The future shouldn't have been done.")
            id = client.requests().peek().destination
            assertEquals(
                expected = ApiKeys.PRODUCE,
                actual = client.requests().peek().requestBuilder().apiKey
            )
            node = Node(id = id.toInt(), host = "localhost", port = 0)
            assertEquals(expected = 1, actual = client.inFlightRequestCount())
            assertTrue(
                actual = client.isReady(node, time.milliseconds()),
                message = "Client ready status should be true"
            )
            responseMap[tp] = PartitionResponse(
                error = Errors.NONE,
                baseOffset = 0L,
                logAppendTime = 0L,
                logStartOffset = 0L,
            )
            client.respond(
                matcher = produceRequestMatcher(
                    tp = tp,
                    producerIdAndEpoch = producerIdAndEpoch,
                    sequence = 0,
                    isTransactional = txnManager.isTransactional,
                ),
                response = ProduceResponse(responseMap)
            )
            sender.runOnce() // receive
            assertTrue(f1.isDone, "The future should have been done.")
            assertEquals(
                expected = 2,
                actual = txnManager.sequenceNumber(tp),
                message = "The next sequence number should still be 2",
            )
            assertEquals(
                expected = 0,
                actual = txnManager.lastAckedSequence(tp),
                message = "The last ack'd sequence number should be 0",
            )
            assertFalse(
                f2.isDone,
                "The future shouldn't have been done."
            )
            assertEquals(
                expected = 0L,
                actual = f1.get().offset,
                message = "Offset of the first message should be 0",
            )
            sender.runOnce() // send the seconcd produce request
            id = client.requests().peek().destination
            assertEquals(
                expected = ApiKeys.PRODUCE,
                actual = client.requests().peek().requestBuilder().apiKey,
            )
            node = Node(id = id.toInt(), host = "localhost", port = 0)
            assertEquals(expected = 1, actual = client.inFlightRequestCount())
            assertTrue(
                actual = client.isReady(node, time.milliseconds()),
                message = "Client ready status should be true",
            )
            responseMap[tp] = PartitionResponse(
                error = Errors.NONE,
                baseOffset = 1L,
                logAppendTime = 0L,
                logStartOffset = 0L,
            )
            client.respond(
                matcher = produceRequestMatcher(
                    tp = tp,
                    producerIdAndEpoch = producerIdAndEpoch,
                    sequence = 1,
                    isTransactional = txnManager.isTransactional,
                ),
                response = ProduceResponse(responseMap)
            )
            sender.runOnce() // receive
            assertTrue(f2.isDone, "The future should have been done.")
            assertEquals(
                expected = 2,
                actual = txnManager.sequenceNumber(tp),
                message = "The next sequence number should be 2",
            )
            assertEquals(
                expected = 1,
                actual = txnManager.lastAckedSequence(tp),
                message = "The last ack'd sequence number should be 1",
            )
            assertEquals(
                expected = 1L,
                actual = f2.get().offset,
                message = "Offset of the first message should be 1",
            )
            assertTrue(
                actual = accumulator.getDeque(tp)!!.isEmpty(),
                message = "There should be no batch in the accumulator"
            )
            assertTrue(
                actual = (m.metrics[senderMetrics.batchSplitRate]!!.metricValue()) as Double > 0,
                message = "There should be a split"
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testNoDoubleDeallocation() {
        val totalSize = (1024 * 1024).toLong()
        val metricGrpName = "producer-custom-metrics"
        val pool = MatchingBufferPool(
            totalSize = totalSize,
            batchSize = batchSize,
            metrics = metrics,
            time = time,
            metricGrpName = metricGrpName,
        )
        setupWithTransactionState(
            transactionManager = null,
            guaranteeOrder = false,
            customPool = pool,
        )

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(1, client.inFlightRequestCount())
        assertEquals(1, sender.inFlightBatches(tp0).size)
        time.sleep(REQUEST_TIMEOUT.toLong())
        assertFalse(pool.allMatch())
        sender.runOnce() // expire the batch
        assertTrue(request1!!.isDone)
        assertTrue(pool.allMatch(), "The batch should have been de-allocated")
        assertTrue(pool.allMatch())
        sender.runOnce()
        assertTrue(pool.allMatch(), "The batch should have been de-allocated")
        assertEquals(0, client.inFlightRequestCount())
        assertEquals(0, sender.inFlightBatches(tp0).size)
    }

    @Suppress("Deprecation")
    @Test
    @Throws(InterruptedException::class)
    fun testInflightBatchesExpireOnDeliveryTimeout() {
        val deliveryTimeoutMs = 1500L
        setupWithTransactionState(
            transactionManager = null,
            guaranteeOrder = true,
            customPool = null,
        )

        // Send first ProduceRequest
        val request: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(1, client.inFlightRequestCount())
        assertEquals(
            expected = 1,
            actual = sender.inFlightBatches(tp0).size,
            message = "Expect one in-flight batch in accumulator",
        )
        val responseMap: MutableMap<TopicPartition, PartitionResponse> = HashMap()
        responseMap[tp0] = PartitionResponse(
            error = Errors.NONE,
            baseOffset = 0L,
            logAppendTime = 0L,
            logStartOffset = 0L,
        )
        client.respond(ProduceResponse(responseMap))
        time.sleep(deliveryTimeoutMs)
        sender.runOnce() // receive first response
        assertEquals(
            expected = 0,
            actual = sender.inFlightBatches(tp0).size,
            message = "Expect zero in-flight batch in accumulator",
        )
        try {
            request!!.get()
            Assertions.fail<Any>("The expired batch should throw a TimeoutException")
        } catch (e: ExecutionException) {
            assertTrue(e.cause is TimeoutException)
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRecordErrorPropagatedToApplication() {
        val recordCount = 5
        setup()
        val futures: MutableMap<Int, FutureRecordMetadata?> = HashMap(recordCount)
        for (i in 0 until recordCount) {
            futures[i] = appendToAccumulator(tp0)
        }
        sender.runOnce() // send request
        assertEquals(1, client.inFlightRequestCount())
        assertEquals(1, sender.inFlightBatches(tp0).size)
        val offsetAndError = OffsetAndError(
            offset = -1L,
            error = Errors.INVALID_RECORD,
            recordErrors = listOf(
                BatchIndexAndErrorMessage().setBatchIndex(0).setBatchIndexErrorMessage("0"),
                BatchIndexAndErrorMessage().setBatchIndex(2).setBatchIndexErrorMessage("2"),
                BatchIndexAndErrorMessage().setBatchIndex(3),
            )
        )
        client.respond(produceResponse(mapOf(tp0 to offsetAndError)))
        sender.runOnce()
        for ((index, future) in futures) {
            assertTrue(future!!.isDone())
            val exception = assertFutureThrows(future, KafkaException::class.java)
            when (index) {
                0, 2 -> {
                    assertTrue(exception is InvalidRecordException)
                    assertEquals(index.toString(), exception.message)
                }

                3 -> {
                    assertTrue(exception is InvalidRecordException)
                    assertEquals(Errors.INVALID_RECORD.message, exception.message)
                }

                else -> {
                    assertEquals(KafkaException::class.java, exception.javaClass)
                }
            }
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testWhenFirstBatchExpireNoSendSecondBatchIfGuaranteeOrder() {
        val deliveryTimeoutMs = 1500L
        setupWithTransactionState(
            transactionManager = null,
            guaranteeOrder = true,
            customPool = null
        )

        // Send first ProduceRequest
        appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        time.sleep(deliveryTimeoutMs / 2)

        // Send second ProduceRequest
        appendToAccumulator(tp0)
        sender.runOnce() // must not send request because the partition is muted
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        time.sleep(deliveryTimeoutMs / 2) // expire the first batch only
        client.respond(
            produceResponse(
                tp = tp0,
                offset = 0L,
                error = Errors.NONE,
                throttleTimeMs = 0,
                logStartOffset = 0L,
                errorMessage = null,
            )
        )
        sender.runOnce() // receive response (offset=0)
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        sender.runOnce() // Drain the second request only this time
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
    }

    @Test
    @Throws(Exception::class)
    fun testExpiredBatchDoesNotRetry() {
        val deliverTimeoutMs = 1500L
        setupWithTransactionState(
            transactionManager = null,
            guaranteeOrder = false,
            customPool = null
        )

        // Send first ProduceRequest
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce() // send request
        assertEquals(1, client.inFlightRequestCount())
        time.sleep(deliverTimeoutMs)
        client.respond(
            produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.NOT_LEADER_OR_FOLLOWER,
                throttleTimeMs = -1,
            )
        ) // return a retriable error
        sender.runOnce() // expire the batch
        assertTrue(request1!!.isDone)
        assertEquals(0, client.inFlightRequestCount())
        assertEquals(0, sender.inFlightBatches(tp0).size)
        sender.runOnce() // receive first response and do not reenqueue.
        assertEquals(0, client.inFlightRequestCount())
        assertEquals(0, sender.inFlightBatches(tp0).size)
        sender.runOnce() // run again and must not send anything.
        assertEquals(0, client.inFlightRequestCount())
        assertEquals(0, sender.inFlightBatches(tp0).size)
    }

    @Test
    @Throws(Exception::class)
    fun testExpiredBatchDoesNotSplitOnMessageTooLargeError() {
        val deliverTimeoutMs = 1500L
        // create a producer batch with more than one record so it is eligible for splitting
        val request1: Future<RecordMetadata>? = appendToAccumulator(tp0)
        val request2: Future<RecordMetadata>? = appendToAccumulator(tp0)

        // send request
        sender.runOnce()
        assertEquals(1, client.inFlightRequestCount())
        // return a MESSAGE_TOO_LARGE error
        client.respond(
            produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.MESSAGE_TOO_LARGE,
                throttleTimeMs = -1
            )
        )
        time.sleep(deliverTimeoutMs)
        // expire the batch and process the response
        sender.runOnce()
        assertTrue(request1!!.isDone)
        assertTrue(request2!!.isDone)
        assertEquals(0, client.inFlightRequestCount())
        assertEquals(0, sender.inFlightBatches(tp0).size)

        // run again and must not split big batch and resend anything.
        sender.runOnce()
        assertEquals(0, client.inFlightRequestCount())
        assertEquals(0, sender.inFlightBatches(tp0).size)
    }

    @Test
    @Throws(Exception::class)
    fun testResetNextBatchExpiry() {
        client = Mockito.spy(MockClient(time, metadata))
        setupWithTransactionState(transactionManager = null)
        appendToAccumulator(tp0, 0L, "key", "value")
        sender.runOnce()
        sender.runOnce()
        time.setCurrentTimeMs(time.milliseconds() + accumulator.getDeliveryTimeoutMs() + 1)
        sender.runOnce()
        val inOrder = Mockito.inOrder(client)
        inOrder.verify(client, Mockito.atLeastOnce())
            .ready(ArgumentMatchers.any(), ArgumentMatchers.anyLong())
        inOrder.verify(client, Mockito.atLeastOnce())
            .newClientRequest(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.any(),
                ArgumentMatchers.anyLong(),
                ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyInt(),
                ArgumentMatchers.any()
            )
        inOrder.verify(client, Mockito.atLeastOnce())
            .send(ArgumentMatchers.any(), ArgumentMatchers.anyLong())
        inOrder.verify(client).poll(eq(0L), ArgumentMatchers.anyLong())
        inOrder.verify(client)
            .poll(eq(accumulator.getDeliveryTimeoutMs()), ArgumentMatchers.anyLong())
        inOrder.verify(client).poll(geq(1L), ArgumentMatchers.anyLong())
    }

    @Suppress("Deprecation")
    @Test
    @Throws(Exception::class)
    fun testExpiredBatchesInMultiplePartitions() {
        val deliveryTimeoutMs = 1500L
        setupWithTransactionState(
            transactionManager = null,
            guaranteeOrder = true,
            customPool = null,
        )

        // Send multiple ProduceRequest across multiple partitions.
        val request1: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp0,
            timestamp = time.milliseconds(),
            key = "k1",
            value = "v1",
        )
        val request2: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp1,
            timestamp = time.milliseconds(),
            key = "k2",
            value = "v2",
        )

        // Send request.
        sender.runOnce()
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertEquals(
            expected = 1,
            actual = sender.inFlightBatches(tp0).size,
            message = "Expect one in-flight batch in accumulator",
        )
        val responseMap: MutableMap<TopicPartition, PartitionResponse> = HashMap()
        responseMap[tp0] = PartitionResponse(
            error = Errors.NONE,
            baseOffset = 0L,
            logAppendTime = 0L,
            logStartOffset = 0L,
        )
        client.respond(ProduceResponse(responseMap))

        // Successfully expire both batches.
        time.sleep(deliveryTimeoutMs)
        sender.runOnce()
        assertEquals(
            expected = 0,
            actual = sender.inFlightBatches(tp0).size,
            message = "Expect zero in-flight batch in accumulator",
        )
        var e = Assertions.assertThrows(ExecutionException::class.java) { request1!!.get() }
        assertTrue(e.cause is TimeoutException)
        e = Assertions.assertThrows(ExecutionException::class.java) { request2!!.get() }
        assertTrue(e.cause is TimeoutException)
    }

    @Test
    fun testTransactionalRequestsSentOnShutdown() {
        // create a sender with retries = 1
        val maxRetries = 1
        val metrics = Metrics()
        val senderMetrics = SenderMetricsRegistry(metrics)
        metrics.use {
            val txnManager = TransactionManager(
                logContext = logContext,
                transactionalId = "testTransactionalRequestsSentOnShutdown",
                transactionTimeoutMs = 6000,
                retryBackoffMs = 100,
                apiVersions = apiVersions,
            )
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = maxRetries,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = txnManager,
                apiVersions = apiVersions,
            )
            val producerIdAndEpoch = ProducerIdAndEpoch(123456L, 0.toShort())
            val tp = TopicPartition("testTransactionalRequestsSentOnShutdown", 1)
            setupWithTransactionState(txnManager)
            doInitTransactions(txnManager, producerIdAndEpoch)
            txnManager.beginTransaction()
            txnManager.maybeAddPartition(tp)
            client.prepareResponse(
                response = AddPartitionsToTxnResponse(0, mapOf(tp to Errors.NONE))
            )
            sender.runOnce()
            sender.initiateClose()
            txnManager.beginCommit()
            val endTxnMatcher = AssertEndTxnRequestMatcher(TransactionResult.COMMIT)
            client.prepareResponse(
                matcher = endTxnMatcher,
                response = EndTxnResponse(
                    EndTxnResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setThrottleTimeMs(0)
                ),
            )
            sender.run()
            assertTrue(endTxnMatcher.matched, "Response didn't match in test")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRecordsFlushedImmediatelyOnTransactionCompletion() {
        Metrics().use { m ->
            val lingerMs = 50
            val senderMetrics = SenderMetricsRegistry(m)
            val txnManager = TransactionManager(
                logContext = logContext,
                transactionalId = "txnId",
                transactionTimeoutMs = 6000,
                retryBackoffMs = 100,
                apiVersions = apiVersions,
            )
            setupWithTransactionState(transactionManager = txnManager, lingerMs = lingerMs)
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = 1,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = txnManager,
                apiVersions = apiVersions,
            )

            // Begin a transaction and successfully add one partition to it.
            val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
            doInitTransactions(txnManager, producerIdAndEpoch)
            txnManager.beginTransaction()
            addPartitionToTxn(sender, txnManager, tp0)

            // Send a couple records and assert that they are not sent immediately (due to linger).
            appendToAccumulator(tp0)
            appendToAccumulator(tp0)
            sender.runOnce()
            assertFalse(client.hasInFlightRequests())

            // Now begin the commit and assert that the Produce request is sent immediately
            // without waiting for the linger.
            val commitResult: TransactionalRequestResult = txnManager.beginCommit()
            ProducerTestUtils.runUntil(
                sender = sender,
                condition = { client.hasInFlightRequests() },
            )

            // Respond to the produce request and wait for the EndTxn request to be sent.
            respondToProduce(tp0, Errors.NONE, 1L)
            ProducerTestUtils.runUntil(
                sender = sender,
                condition = { txnManager.hasInFlightRequest },
            )

            // Respond to the expected EndTxn request.
            respondToEndTxn(Errors.NONE)
            ProducerTestUtils.runUntil(sender, txnManager::isReady)
            assertTrue(commitResult.isSuccessful)
            commitResult.await()

            // Finally, we want to assert that the linger time is still effective
            // when the new transaction begins.
            txnManager.beginTransaction()
            addPartitionToTxn(sender, txnManager, tp0)
            appendToAccumulator(tp0)
            appendToAccumulator(tp0)
            time.sleep((lingerMs - 1).toLong())
            sender.runOnce()
            assertFalse(client.hasInFlightRequests())
            assertTrue(accumulator.hasUndrained())
            time.sleep(1)
            ProducerTestUtils.runUntil(
                sender = sender,
                condition = { client.hasInFlightRequests() },
            )
            assertFalse(accumulator.hasUndrained())
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAwaitPendingRecordsBeforeCommittingTransaction() {
        Metrics().use { m ->
            val senderMetrics = SenderMetricsRegistry(m)
            val txnManager = TransactionManager(
                logContext = logContext,
                transactionalId = "txnId",
                transactionTimeoutMs = 6000,
                retryBackoffMs = 100,
                apiVersions = apiVersions,
            )
            setupWithTransactionState(txnManager)
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = 1,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = txnManager,
                apiVersions = apiVersions,
            )

            // Begin a transaction and successfully add one partition to it.
            val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
            doInitTransactions(txnManager, producerIdAndEpoch)
            txnManager.beginTransaction()
            addPartitionToTxn(sender, txnManager, tp0)

            // Send one Produce request.
            appendToAccumulator(tp0)
            ProducerTestUtils.runUntil(
                sender = sender,
                condition = { client.requests().size == 1 },
            )
            assertFalse(accumulator.hasUndrained())
            assertTrue(client.hasInFlightRequests())
            assertTrue(txnManager.hasInflightBatches(tp0))

            // Enqueue another record and then commit the transaction. We expect the unsent record to
            // get sent before the transaction can be completed.
            appendToAccumulator(tp0)
            txnManager.beginCommit()
            ProducerTestUtils.runUntil(
                sender = sender,
                condition = { client.requests().size == 2 },
            )
            assertTrue(txnManager.isCompleting)
            assertFalse(txnManager.hasInFlightRequest)
            assertTrue(txnManager.hasInflightBatches(tp0))

            // Now respond to the pending Produce requests.
            respondToProduce(tp0, Errors.NONE, 0L)
            respondToProduce(tp0, Errors.NONE, 1L)
            ProducerTestUtils.runUntil(
                sender = sender,
                condition = { txnManager.hasInFlightRequest },
            )

            // Finally, respond to the expected EndTxn request.
            respondToEndTxn(Errors.NONE)
            ProducerTestUtils.runUntil(sender, txnManager::isReady)
        }
    }

    private fun addPartitionToTxn(
        sender: Sender,
        txnManager: TransactionManager,
        tp: TopicPartition
    ) {
        txnManager.maybeAddPartition(tp)
        client.prepareResponse(
            response = AddPartitionsToTxnResponse(0, mapOf(tp to Errors.NONE)),
        )
        ProducerTestUtils.runUntil(
            sender = sender,
            condition = { txnManager.isPartitionAdded(tp) },
        )
        assertFalse(txnManager.hasInFlightRequest)
    }

    private fun respondToProduce(tp: TopicPartition, error: Errors, offset: Long) {
        client.respond(
            matcher = { request -> request is ProduceRequest },
            response = produceResponse(tp = tp, offset = offset, error = error, throttleTimeMs = 0),
        )
    }

    private fun respondToEndTxn(error: Errors) {
        client.respond(
            matcher = { request -> request is EndTxnRequest },
            response = EndTxnResponse(
                EndTxnResponseData()
                    .setErrorCode(error.code)
                    .setThrottleTimeMs(0)
            )
        )
    }

    @Test
    fun testIncompleteTransactionAbortOnShutdown() {
        // create a sender with retries = 1
        val maxRetries = 1
        val metrics = Metrics()
        val senderMetrics = SenderMetricsRegistry(metrics)
        metrics.use {
            val txnManager = TransactionManager(
                logContext = logContext,
                transactionalId = "testIncompleteTransactionAbortOnShutdown",
                transactionTimeoutMs = 6000,
                retryBackoffMs = 100,
                apiVersions = apiVersions,
            )
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = maxRetries,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = txnManager,
                apiVersions = apiVersions,
            )
            val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
            val tp = TopicPartition(
                topic = "testIncompleteTransactionAbortOnShutdown",
                partition = 1,
            )
            setupWithTransactionState(txnManager)
            doInitTransactions(txnManager, producerIdAndEpoch)
            txnManager.beginTransaction()
            txnManager.maybeAddPartition(tp)
            client.prepareResponse(
                response = AddPartitionsToTxnResponse(
                    throttleTimeMs = 0,
                    errors = mapOf(tp to Errors.NONE),
                ),
            )
            sender.runOnce()
            sender.initiateClose()
            val endTxnMatcher = AssertEndTxnRequestMatcher(TransactionResult.ABORT)
            client.prepareResponse(
                matcher = endTxnMatcher,
                response = EndTxnResponse(
                    EndTxnResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setThrottleTimeMs(0)
                ),
            )
            sender.run()
            assertTrue(endTxnMatcher.matched, "Response didn't match in test")
        }
    }

    @Timeout(10L)
    @Test
    fun testForceShutdownWithIncompleteTransaction() {
        // create a sender with retries = 1
        val maxRetries = 1
        val metrics = Metrics()
        val senderMetrics = SenderMetricsRegistry(metrics)
        metrics.use {
            val txnManager = TransactionManager(
                logContext = logContext,
                transactionalId = "testForceShutdownWithIncompleteTransaction",
                transactionTimeoutMs = 6000,
                retryBackoffMs = 100,
                apiVersions = apiVersions,
            )
            val sender = Sender(
                logContext = logContext,
                client = client,
                metadata = metadata,
                accumulator = accumulator,
                guaranteeMessageOrder = false,
                maxRequestSize = MAX_REQUEST_SIZE,
                acks = ACKS_ALL,
                retries = maxRetries,
                metricsRegistry = senderMetrics,
                time = time,
                requestTimeoutMs = REQUEST_TIMEOUT,
                retryBackoffMs = RETRY_BACKOFF_MS,
                transactionManager = txnManager,
                apiVersions = apiVersions,
            )
            val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
            val tp = TopicPartition(
                topic = "testForceShutdownWithIncompleteTransaction",
                partition = 1,
            )
            setupWithTransactionState(txnManager)
            doInitTransactions(txnManager, producerIdAndEpoch)
            txnManager.beginTransaction()
            txnManager.maybeAddPartition(tp)
            client.prepareResponse(
                response = AddPartitionsToTxnResponse(
                    throttleTimeMs = 0,
                    errors = mapOf(tp to Errors.NONE),
                ),
            )
            sender.runOnce()

            // Try to commit the transaction but it won't happen as we'll forcefully close the sender
            val commitResult = txnManager.beginCommit()
            sender.forceClose()
            sender.run()
            assertFailsWith<KafkaException>(
                "The test expected to throw a KafkaException for forcefully closing the sender"
            ) { commitResult.await() }
        }
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testTransactionAbortedExceptionOnAbortWithoutError() {
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
        val txnManager = TransactionManager(
            logContext = logContext,
            transactionalId = "testTransactionAbortedExceptionOnAbortWithoutError",
            transactionTimeoutMs = 60000,
            retryBackoffMs = 100,
            apiVersions = apiVersions,
        )
        setupWithTransactionState(
            transactionManager = txnManager,
            guaranteeOrder = false,
            customPool = null
        )
        doInitTransactions(txnManager, producerIdAndEpoch)
        // Begin the transaction
        txnManager.beginTransaction()
        txnManager.maybeAddPartition(tp0)
        client.prepareResponse(
            response = AddPartitionsToTxnResponse(
                throttleTimeMs = 0,
                errors = mapOf(tp0 to Errors.NONE),
            ),
        )
        // Run it once so that the partition is added to the transaction.
        sender.runOnce()
        // Append a record to the accumulator.
        val metadata = appendToAccumulator(tp0, time.milliseconds(), "key", "value")
        // Now abort the transaction manually.
        txnManager.beginAbort()
        // Try to send.
        // This should abort the existing transaction and
        // drain all the unsent batches with a TransactionAbortedException.
        sender.runOnce()
        // Now attempt to fetch the result for the record.
        assertFutureThrows(
            future = metadata!!,
            exceptionCauseClass = TransactionAbortedException::class.java,
        )
    }

    @Test
    fun testDoNotPollWhenNoRequestSent() {
        client = Mockito.spy(MockClient(time, metadata))
        val txnManager =
            TransactionManager(
                logContext = logContext,
                transactionalId = "testDoNotPollWhenNoRequestSent",
                transactionTimeoutMs = 6000,
                retryBackoffMs = 100,
                apiVersions = apiVersions,
            )
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
        setupWithTransactionState(txnManager)
        doInitTransactions(txnManager, producerIdAndEpoch)

        // doInitTransactions calls sender.doOnce three times, only two requests are sent, so we should only poll twice
        Mockito.verify(client, Mockito.times(2))
            .poll(eq(RETRY_BACKOFF_MS), ArgumentMatchers.anyLong())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTooLargeBatchesAreSafelyRemoved() {
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId = 123456L, epoch = 0)
        val txnManager = TransactionManager(
            logContext = logContext,
            transactionalId = "testSplitBatchAndSend",
            transactionTimeoutMs = 60000,
            retryBackoffMs = 100,
            apiVersions = apiVersions,
        )
        setupWithTransactionState(
            transactionManager = txnManager,
            guaranteeOrder = false,
            customPool = null,
        )
        doInitTransactions(txnManager, producerIdAndEpoch)
        txnManager.beginTransaction()
        txnManager.maybeAddPartition(tp0)
        client.prepareResponse(
            response = AddPartitionsToTxnResponse(
                throttleTimeMs = 0,
                errors = mapOf(tp0 to Errors.NONE),
            ),
        )
        sender.runOnce()

        // create a producer batch with more than one record so it is eligible for splitting
        appendToAccumulator(
            tp = tp0,
            timestamp = time.milliseconds(),
            key = "key1",
            value = "value1",
        )
        appendToAccumulator(
            tp = tp0,
            timestamp = time.milliseconds(),
            key = "key2",
            value = "value2",
        )

        // send request
        sender.runOnce()
        assertEquals(expected = 1, actual = sender.inFlightBatches(tp0).size)
        // return a MESSAGE_TOO_LARGE error
        client.respond(
            produceResponse(
                tp = tp0,
                offset = -1,
                error = Errors.MESSAGE_TOO_LARGE,
                throttleTimeMs = -1,
            )
        )
        sender.runOnce()

        // process retried response
        sender.runOnce()
        client.respond(
            produceResponse(
                tp = tp0,
                offset = 0,
                error = Errors.NONE,
                throttleTimeMs = 0,
            )
        )
        sender.runOnce()

        // In-flight batches should be empty. Sleep past the expiration time of the batch and run once, no error should be thrown
        assertEquals(expected = 0, actual = sender.inFlightBatches(tp0).size)
        time.sleep(2000)
        sender.runOnce()
    }

    @Test
    @Throws(Exception::class)
    fun testDefaultErrorMessage() {
        verifyErrorMessage(
            response = produceResponse(
                tp = tp0,
                offset = 0L,
                error = Errors.INVALID_REQUEST,
                throttleTimeMs = 0,
            ),
            expectedMessage = Errors.INVALID_REQUEST.message,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCustomErrorMessage() {
        val errorMessage = "testCustomErrorMessage"
        verifyErrorMessage(
            response = produceResponse(
                tp = tp0,
                offset = 0L,
                error = Errors.INVALID_REQUEST,
                throttleTimeMs = 0,
                logStartOffset = -1,
                errorMessage = errorMessage,
            ),
            expectedMessage = errorMessage,
        )
    }

    @Throws(Exception::class)
    private fun verifyErrorMessage(response: ProduceResponse, expectedMessage: String) {
        val future: Future<RecordMetadata>? = appendToAccumulator(
            tp = tp0,
            timestamp = 0L,
            key = "key",
            value = "value",
        )
        sender.runOnce() // connect
        sender.runOnce() // send produce request
        client.respond(response)
        sender.runOnce()
        sender.runOnce()
        val cause = assertFailsWith<ExecutionException> { future!!.get(5, TimeUnit.SECONDS) }.cause
        assertTrue(cause is InvalidRequestException)
        assertEquals(expected = expectedMessage, actual = cause.message)
    }

    internal inner class AssertEndTxnRequestMatcher(
        private val requiredResult: TransactionResult,
    ) : RequestMatcher {

        var matched = false

        override fun matches(body: AbstractRequest?): Boolean {
            return if (body is EndTxnRequest) {
                assertSame(requiredResult, body.result)
                matched = true
                true
            } else false
        }
    }

    private inner class MatchingBufferPool(
        totalSize: Long,
        batchSize: Int,
        metrics: Metrics,
        time: Time,
        metricGrpName: String,
    ) : BufferPool(
        totalMemory = totalSize,
        poolableSize = batchSize,
        metrics = metrics,
        time = time,
        metricGrpName = metricGrpName,
    ) {
        var allocatedBuffers: IdentityHashMap<ByteBuffer, Boolean> = IdentityHashMap()

        @Throws(InterruptedException::class)
        override fun allocate(size: Int, maxTimeToBlockMs: Long): ByteBuffer {
            val buffer = super.allocate(size, maxTimeToBlockMs)
            allocatedBuffers[buffer] = java.lang.Boolean.TRUE
            return buffer
        }

        override fun deallocate(buffer: ByteBuffer, size: Int) {
            check(allocatedBuffers.containsKey(buffer)) { "Deallocating a buffer that is not allocated" }
            allocatedBuffers.remove(buffer)
            super.deallocate(buffer, size)
        }

        fun allMatch(): Boolean {
            return allocatedBuffers.isEmpty()
        }
    }

    private fun produceRequestMatcher(
        tp: TopicPartition,
        producerIdAndEpoch: ProducerIdAndEpoch,
        sequence: Int,
        isTransactional: Boolean
    ): RequestMatcher {
        return RequestMatcher { body: AbstractRequest? ->
            if (body !is ProduceRequest) return@RequestMatcher false
            val recordsMap = partitionRecords(body)
            val records = recordsMap[tp] ?: return@RequestMatcher false
            val batches = toList(records.batches())
            if (batches.size != 1) return@RequestMatcher false
            val batch = batches[0]

            batch.baseOffset() == 0L
                    && batch.baseSequence() == sequence
                    && batch.producerId() == producerIdAndEpoch.producerId
                    && batch.producerEpoch() == producerIdAndEpoch.epoch
                    && batch.isTransactional == isTransactional
        }
    }

    private class OffsetAndError(
        val offset: Long,
        val error: Errors,
        val recordErrors: List<BatchIndexAndErrorMessage> = emptyList(),
    )

    @Throws(InterruptedException::class)
    private fun appendToAccumulator(
        tp: TopicPartition,
        timestamp: Long = time.milliseconds(),
        key: String = "key",
        value: String = "value"
    ): FutureRecordMetadata? = accumulator.append(
        topic = tp.topic,
        partition = tp.partition,
        timestamp = timestamp,
        key = key.toByteArray(),
        value = value.toByteArray(),
        headers = Record.EMPTY_HEADERS,
        callbacks = null,
        maxTimeToBlock = MAX_BLOCK_TIMEOUT.toLong(),
        abortOnNewBatch = false,
        nowMs = time.milliseconds(),
        cluster = singletonCluster(),
    ).future

    @Suppress("Deprecation")
    private fun produceResponse(
        tp: TopicPartition,
        offset: Long,
        error: Errors?,
        throttleTimeMs: Int,
        logStartOffset: Long = -1L,
        errorMessage: String? = null
    ): ProduceResponse {
        val resp = PartitionResponse(
            error!!, offset,
            RecordBatch.NO_TIMESTAMP, logStartOffset, emptyList(), errorMessage
        )
        val partResp = mapOf(tp to resp)
        return ProduceResponse(partResp, throttleTimeMs)
    }

    private fun produceResponse(responses: Map<TopicPartition, OffsetAndError>): ProduceResponse {
        val data = ProduceResponseData()
        for ((topicPartition, offsetAndError) in responses) {
            var topicData = data.responses.find(topicPartition.topic)
            if (topicData == null) {
                topicData = TopicProduceResponse().setName(topicPartition.topic)
                data.responses.add(topicData)
            }
            val partitionData = PartitionProduceResponse()
                .setIndex(topicPartition.partition)
                .setBaseOffset(offsetAndError.offset)
                .setErrorCode(offsetAndError.error.code)
                .setRecordErrors(offsetAndError.recordErrors)
            topicData.partitionResponses += partitionData
        }
        return ProduceResponse(data)
    }

    private fun createTransactionManager(): TransactionManager = TransactionManager(
        logContext = LogContext(),
        transactionalId = null,
        transactionTimeoutMs = 0,
        retryBackoffMs = 100L,
        apiVersions = ApiVersions()
    )

    private fun setupWithTransactionState(
        transactionManager: TransactionManager?,
        guaranteeOrder: Boolean = false,
        customPool: BufferPool? = null,
        updateMetadata: Boolean = true,
        retries: Int = Int.MAX_VALUE,
        lingerMs: Int = 0
    ) {
        val totalSize = (1024 * 1024).toLong()
        val metricGrpName = "producer-metrics"
        val metricConfig = MetricConfig().apply {
            tags = mapOf("client-id" to CLIENT_ID)
        }
        metrics = Metrics(config = metricConfig, time = time)
        val pool = customPool ?: BufferPool(
            totalMemory = totalSize,
            poolableSize = batchSize,
            metrics = metrics,
            time = time,
            metricGrpName = metricGrpName,
        )
        accumulator = RecordAccumulator(
            logContext = logContext,
            batchSize = batchSize,
            compression = CompressionType.NONE,
            lingerMs = lingerMs,
            retryBackoffMs = 0L,
            deliveryTimeoutMs = DELIVERY_TIMEOUT_MS,
            metrics = metrics,
            metricGrpName = metricGrpName,
            time = time,
            apiVersions = apiVersions,
            transactionManager = transactionManager,
            free = pool,
        )
        senderMetricsRegistry = SenderMetricsRegistry(metrics)
        sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = guaranteeOrder,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = retries,
            metricsRegistry = senderMetricsRegistry!!,
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = RETRY_BACKOFF_MS,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        metadata.add("test", time.milliseconds())
        if (updateMetadata) client.updateMetadata(
            RequestTestUtils.metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            ),
        )
    }

    @Throws(Exception::class)
    private fun assertSendFailure(expectedError: Class<out RuntimeException?>) {
        val future: Future<RecordMetadata>? = appendToAccumulator(tp0)
        sender.runOnce()
        assertTrue(future!!.isDone)
        try {
            future.get()
            Assertions.fail<Any>("Future should have raised " + expectedError.getSimpleName())
        } catch (e: ExecutionException) {
            assertTrue(expectedError.isAssignableFrom(e.cause!!.javaClass))
        }
    }

    private fun prepareAndReceiveInitProducerId(producerId: Long, error: Errors) {
        prepareAndReceiveInitProducerId(producerId, 0.toShort(), error)
    }

    private fun prepareAndReceiveInitProducerId(
        producerId: Long,
        producerEpoch: Short,
        error: Errors
    ) {
        var producerEpoch = producerEpoch
        if (error !== Errors.NONE) producerEpoch = RecordBatch.NO_PRODUCER_EPOCH
        client.prepareResponse(
            matcher = { body -> body is InitProducerIdRequest && body.data().transactionalId == null },
            response = initProducerIdResponse(producerId, producerEpoch, error),
        )
        sender.runOnce()
    }

    private fun initProducerIdResponse(
        producerId: Long,
        producerEpoch: Short,
        error: Errors,
    ): InitProducerIdResponse {
        val responseData = InitProducerIdResponseData()
            .setErrorCode(error.code)
            .setProducerEpoch(producerEpoch)
            .setProducerId(producerId)
            .setThrottleTimeMs(0)
        return InitProducerIdResponse(responseData)
    }

    private fun doInitTransactions(
        transactionManager: TransactionManager,
        producerIdAndEpoch: ProducerIdAndEpoch
    ) {
        val result = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(Errors.NONE, transactionManager.transactionalId)
        sender.runOnce()
        sender.runOnce()
        prepareInitProducerResponse(
            Errors.NONE,
            producerIdAndEpoch.producerId,
            producerIdAndEpoch.epoch
        )
        sender.runOnce()
        assertTrue(transactionManager.hasProducerId)
        result.await()
    }

    private fun prepareFindCoordinatorResponse(error: Errors, txnid: String?) {
        val node = metadata.fetch().nodes[0]
        client.prepareResponse(
            response = FindCoordinatorResponse.prepareResponse(error, txnid!!, node),
        )
    }

    private fun prepareInitProducerResponse(error: Errors, producerId: Long, producerEpoch: Short) {
        client.prepareResponse(
            response = initProducerIdResponse(producerId, producerEpoch, error),
        )
    }

    @Throws(InterruptedException::class)
    private fun assertFutureFailure(
        future: Future<*>?,
        expectedExceptionType: Class<out Exception>
    ) {
        assertTrue(future!!.isDone)
        try {
            future.get()
            Assertions.fail<Any>("Future should have raised " + expectedExceptionType.getName())
        } catch (e: ExecutionException) {
            val causeType: Class<out Throwable?> = e.cause!!.javaClass
            assertTrue(
                actual = expectedExceptionType.isAssignableFrom(causeType),
                message = "Unexpected cause " + causeType.getName(),
            )
        }
    }

    private fun createMockClientWithMaxFlightOneMetadataPending() {
        client = object : MockClient(time, metadata) {

            @Volatile
            var canSendMore = true

            override fun leastLoadedNode(now: Long): Node? {
                for (node in metadata.fetch().nodes) {
                    if (isReady(node, now) && canSendMore) return node
                }
                return null
            }

            override fun poll(timeoutMs: Long, now: Long): List<ClientResponse> {
                canSendMore = inFlightRequestCount() < 1
                return super.poll(timeoutMs, now)
            }
        }

        // Send metadata request and wait until request is sent. `leastLoadedNode` will be null once
        // request is in progress since no more requests can be sent to the node. Node will be ready
        // on the next poll() after response is processed later on in tests which use this method.
        val builder = MetadataRequest.Builder(topics = emptyList(), allowAutoTopicCreation = false)
        val node = metadata.fetch().nodes[0]
        val request = client.newClientRequest(
            nodeId = node.idString(),
            requestBuilder = builder,
            createdTimeMs = time.milliseconds(),
            expectResponse = true,
        )
        while (!client.ready(node, time.milliseconds())) client.poll(0, time.milliseconds())
        client.send(request!!, time.milliseconds())
        while (client.leastLoadedNode(time.milliseconds()) != null)
            client.poll(timeout = 0, now = time.milliseconds())
    }

    private fun waitForProducerId(
        transactionManager: TransactionManager,
        producerIdAndEpoch: ProducerIdAndEpoch,
    ) {
        var i = 0
        while (i < 5 && !transactionManager.hasProducerId) {
            sender.runOnce()
            i++
        }
        assertTrue(transactionManager.hasProducerId)
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
    }

    companion object {

        private const val MAX_REQUEST_SIZE = 1024 * 1024

        private const val ACKS_ALL: Short = -1

        private const val CLIENT_ID = "clientId"

        private const val EPS = 0.0001

        private const val MAX_BLOCK_TIMEOUT = 1000

        private const val REQUEST_TIMEOUT = 5000

        private const val RETRY_BACKOFF_MS: Long = 50

        private const val DELIVERY_TIMEOUT_MS = 1500

        private const val TOPIC_IDLE_MS = (60 * 1000).toLong()

        private fun partitionRecords(request: ProduceRequest?): Map<TopicPartition, MemoryRecords> {
            val partitionRecords: MutableMap<TopicPartition, MemoryRecords> = HashMap()
            request!!.data().topicData.forEach { tpData ->
                tpData.partitionData.forEach { p ->
                    val tp = TopicPartition(tpData.name, p.index)
                    partitionRecords[tp] = p.records as MemoryRecords
                }
            }
            return Collections.unmodifiableMap(partitionRecords)
        }
    }
}
