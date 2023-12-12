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

import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.RequestMatcher
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.InvalidTxnStateException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.EndTxnResponseData
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse
import org.apache.kafka.common.requests.EndTxnRequest
import org.apache.kafka.common.requests.EndTxnResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.InitProducerIdRequest
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWith
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.requests.TxnOffsetCommitRequest
import org.apache.kafka.common.requests.TxnOffsetCommitResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.apache.kafka.test.TestUtils.singletonCluster
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

class TransactionManagerTest {

    private val transactionalId = "foobar"
    private val transactionTimeoutMs = 1121

    private val topic = "test"
    private val tp0 = TopicPartition(topic, 0)
    private val tp1 = TopicPartition(topic, 1)
    private val producerId = 13131L
    private val epoch: Short = 1
    private val consumerGroupId = "myConsumerGroup"
    private val memberId = "member"
    private val generationId = 5
    private val groupInstanceId = "instance"

    private val logContext = LogContext()
    private val time = MockTime()
    private val metadata = ProducerMetadata(
        refreshBackoffMs = 0,
        metadataExpireMs = Long.MAX_VALUE,
        metadataIdleMs = Long.MAX_VALUE,
        logContext = logContext,
        clusterResourceListeners = ClusterResourceListeners(),
        time = time,
    )
    private val client = MockClient(time, metadata)
    private val apiVersions = ApiVersions()

    private lateinit var accumulator: RecordAccumulator
    private lateinit var sender: Sender
    private lateinit var transactionManager: TransactionManager
    private lateinit var brokerNode: Node

    @BeforeEach
    fun setup() {
        metadata.add("test", time.milliseconds())
        client.updateMetadata(
            metadataUpdateWith(
                numNodes = 1,
                topicPartitionCounts = mapOf("test" to 2),
            ),
        )
        brokerNode = Node(0, "localhost", 2211)
        initializeTransactionManager(transactionalId)
    }

    private fun initializeTransactionManager(transactionalId: String?) {
        val metrics = Metrics(time = time)
        apiVersions.update(
            nodeId = "0",
            nodeApiVersions = NodeApiVersions.create(
                listOf(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                        .setMinVersion(0)
                        .setMaxVersion(3),
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.PRODUCE.id)
                        .setMinVersion(0)
                        .setMaxVersion(7),
                )
            )
        )
        transactionManager = TransactionManager(
            logContext = logContext,
            transactionalId = transactionalId,
            transactionTimeoutMs = transactionTimeoutMs,
            retryBackoffMs = DEFAULT_RETRY_BACKOFF_MS,
            apiVersions = apiVersions,
        )
        val batchSize = 16 * 1024
        val deliveryTimeoutMs = 3000
        val totalSize = (1024 * 1024).toLong()
        val metricGrpName = "producer-metrics"
        brokerNode = Node(0, "localhost", 2211)
        accumulator = RecordAccumulator(
            logContext = logContext,
            batchSize = batchSize,
            compression = CompressionType.NONE,
            lingerMs = 0,
            retryBackoffMs = 0L,
            deliveryTimeoutMs = deliveryTimeoutMs,
            metrics = metrics,
            metricGrpName = metricGrpName,
            time = time,
            apiVersions = apiVersions,
            transactionManager = transactionManager,
            free = BufferPool(
                totalMemory = totalSize,
                poolableSize = batchSize,
                metrics = metrics,
                time = time,
                metricGrpName = metricGrpName,
            ),
        )
        sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = true,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = MAX_RETRIES,
            metricsRegistry = SenderMetricsRegistry(metrics),
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = 50,
            transactionManager = transactionManager,
            apiVersions = apiVersions
        )
    }

    @Test
    @Throws(Exception::class)
    fun testSenderShutdownWithPendingTransactions() {
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(tp0)
        val sendFuture = appendToAccumulator(tp0)

        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { !client.hasPendingResponses() }

        sender.initiateClose()
        sender.runOnce()

        val result = transactionManager.beginCommit()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(result::isCompleted)
        runUntil { sendFuture!!.isDone() }
    }

    @Test
    fun testEndTxnNotSentIfIncompleteBatches() {
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(tp0) }

        transactionManager.beginCommit()
        assertNull(transactionManager.nextRequest(true))
        assertTrue(transactionManager.nextRequest(false)!!.isEndTxn)
    }

    @Test
    fun testFailIfNotReadyForSendNoProducerId() {
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testFailIfNotReadyForSendIdempotentProducer() {
        initializeTransactionManager(null)
        transactionManager.maybeAddPartition(tp0)
    }

    @Test
    fun testFailIfNotReadyForSendIdempotentProducerFatalError() {
        initializeTransactionManager(null)
        transactionManager.transitionToFatalError(KafkaException())

        assertFailsWith<KafkaException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testFailIfNotReadyForSendNoOngoingTransaction() {
        doInitTransactions()

        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testFailIfNotReadyForSendAfterAbortableError() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.transitionToAbortableError(KafkaException())

        assertFailsWith<KafkaException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testFailIfNotReadyForSendAfterFatalError() {
        doInitTransactions()
        transactionManager.transitionToFatalError(KafkaException())

        assertFailsWith<KafkaException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testHasOngoingTransactionSuccessfulAbort() {
        val partition = TopicPartition(topic = "foo", partition = 0)

        assertFalse(transactionManager.hasOngoingTransaction)
        doInitTransactions()
        assertFalse(transactionManager.hasOngoingTransaction)

        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)

        transactionManager.maybeAddPartition(partition)
        runUntil { transactionManager.hasOngoingTransaction }

        prepareAddPartitionsToTxn(partition, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(partition) }

        transactionManager.beginAbort()
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil { !transactionManager.hasOngoingTransaction }
    }

    @Test
    fun testHasOngoingTransactionSuccessfulCommit() {
        val partition = TopicPartition(topic = "foo", partition = 0)

        assertFalse(transactionManager.hasOngoingTransaction)
        doInitTransactions()
        assertFalse(transactionManager.hasOngoingTransaction)

        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareAddPartitionsToTxn(partition, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(partition) }

        transactionManager.beginCommit()
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil { !transactionManager.hasOngoingTransaction }
    }

    @Test
    fun testHasOngoingTransactionAbortableError() {
        val partition = TopicPartition(topic = "foo", partition = 0)

        assertFalse(transactionManager.hasOngoingTransaction)
        doInitTransactions()
        assertFalse(transactionManager.hasOngoingTransaction)

        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareAddPartitionsToTxn(partition, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(partition) }

        transactionManager.transitionToAbortableError(KafkaException())
        assertTrue(transactionManager.hasOngoingTransaction)

        transactionManager.beginAbort()
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil { !transactionManager.hasOngoingTransaction }
    }

    @Test
    fun testHasOngoingTransactionFatalError() {
        val partition = TopicPartition(topic = "foo", partition = 0)

        assertFalse(transactionManager.hasOngoingTransaction)
        doInitTransactions()
        assertFalse(transactionManager.hasOngoingTransaction)

        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareAddPartitionsToTxn(partition, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(partition) }

        transactionManager.transitionToFatalError(KafkaException())
        assertFalse(transactionManager.hasOngoingTransaction)
    }

    @Test
    fun testMaybeAddPartitionToTransaction() {
        val partition = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasPartitionsToAdd)
        assertFalse(transactionManager.isPartitionAdded(partition))
        assertTrue(transactionManager.isPartitionPendingAdd(partition))

        prepareAddPartitionsToTxn(partition, Errors.NONE)
        assertTrue(transactionManager.hasPartitionsToAdd)

        runUntil { transactionManager.isPartitionAdded(partition) }
        assertFalse(transactionManager.hasPartitionsToAdd)
        assertFalse(transactionManager.isPartitionPendingAdd(partition))

        // adding the partition again should not have any effect
        transactionManager.maybeAddPartition(partition)
        assertFalse(transactionManager.hasPartitionsToAdd)
        assertTrue(transactionManager.isPartitionAdded(partition))
        assertFalse(transactionManager.isPartitionPendingAdd(partition))
    }

    @Test
    fun testAddPartitionToTransactionOverridesRetryBackoffForConcurrentTransactions() {
        val partition = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasPartitionsToAdd)
        assertFalse(transactionManager.isPartitionAdded(partition))
        assertTrue(transactionManager.isPartitionPendingAdd(partition))

        prepareAddPartitionsToTxn(partition, Errors.CONCURRENT_TRANSACTIONS)
        runUntil { !client.hasPendingResponses() }

        val handler = transactionManager.nextRequest(false)
        assertNotNull(handler)
        assertEquals(20, handler.retryBackoffMs())
    }

    @Test
    fun testAddPartitionToTransactionRetainsRetryBackoffForRegularRetriableError() {
        val partition = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasPartitionsToAdd)
        assertFalse(transactionManager.isPartitionAdded(partition))
        assertTrue(transactionManager.isPartitionPendingAdd(partition))

        prepareAddPartitionsToTxn(partition, Errors.COORDINATOR_NOT_AVAILABLE)
        runUntil { !client.hasPendingResponses() }

        val handler = transactionManager.nextRequest(false)
        assertNotNull(handler)
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs())
    }

    @Test
    fun testAddPartitionToTransactionRetainsRetryBackoffWhenPartitionsAlreadyAdded() {
        val partition = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(partition)
        assertTrue(transactionManager.hasPartitionsToAdd)
        assertFalse(transactionManager.isPartitionAdded(partition))
        assertTrue(transactionManager.isPartitionPendingAdd(partition))

        prepareAddPartitionsToTxn(partition, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(partition) }

        val otherPartition = TopicPartition(topic = "foo", partition = 1)
        transactionManager.maybeAddPartition(otherPartition)

        prepareAddPartitionsToTxn(otherPartition, Errors.CONCURRENT_TRANSACTIONS)
        val handler = transactionManager.nextRequest(false)
        assertNotNull(handler)
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs())
    }

    @Test
    fun testNotReadyForSendBeforeInitTransactions() {
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testNotReadyForSendBeforeBeginTransaction() {
        doInitTransactions()
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testNotReadyForSendAfterAbortableError() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.transitionToAbortableError(KafkaException())
        assertFailsWith<KafkaException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testNotReadyForSendAfterFatalError() {
        doInitTransactions()
        transactionManager.transitionToFatalError(KafkaException())
        assertFailsWith<KafkaException> { transactionManager.maybeAddPartition(tp0) }
    }

    @Test
    fun testIsSendToPartitionAllowedWithPendingPartitionAfterAbortableError() {
        doInitTransactions()

        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        transactionManager.transitionToAbortableError(KafkaException())

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    fun testIsSendToPartitionAllowedWithInFlightPartitionAddAfterAbortableError() {
        doInitTransactions()

        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)

        // Send the AddPartitionsToTxn request and leave it in-flight
        runUntil { transactionManager.hasInFlightRequest }
        transactionManager.transitionToAbortableError(KafkaException())

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    fun testIsSendToPartitionAllowedWithPendingPartitionAfterFatalError() {
        doInitTransactions()

        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        transactionManager.transitionToFatalError(KafkaException())

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        assertTrue(transactionManager.hasFatalError)
    }

    @Test
    fun testIsSendToPartitionAllowedWithInFlightPartitionAddAfterFatalError() {
        doInitTransactions()

        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)

        // Send the AddPartitionsToTxn request and leave it in-flight
        runUntil { transactionManager.hasInFlightRequest }
        transactionManager.transitionToFatalError(KafkaException())

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        assertTrue(transactionManager.hasFatalError)
    }

    @Test
    fun testIsSendToPartitionAllowedWithAddedPartitionAfterAbortableError() {
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)

        runUntil { !transactionManager.hasPartitionsToAdd }
        transactionManager.transitionToAbortableError(KafkaException())

        assertTrue(transactionManager.isSendToPartitionAllowed(tp0))
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    fun testIsSendToPartitionAllowedWithAddedPartitionAfterFatalError() {
        doInitTransactions()
        transactionManager.beginTransaction()

        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)

        runUntil { !transactionManager.hasPartitionsToAdd }
        transactionManager.transitionToFatalError(KafkaException())

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        assertTrue(transactionManager.hasFatalError)
    }

    @Test
    fun testIsSendToPartitionAllowedWithPartitionNotAdded() {
        doInitTransactions()
        transactionManager.beginTransaction()

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
    }

    @Test
    fun testDefaultSequenceNumber() {
        initializeTransactionManager(null)
        assertEquals(0, transactionManager.sequenceNumber(tp0))
        transactionManager.incrementSequenceNumber(tp0, 3)
        assertEquals(transactionManager.sequenceNumber(tp0), 3)
    }

    @Test
    fun testBumpEpochAndResetSequenceNumbersAfterUnknownProducerId() {
        initializeTransactionManager(null)
        initializeIdempotentProducerId(producerId, epoch)

        val b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        val b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        val b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3")
        val b4 = writeIdempotentBatchWithValue(transactionManager, tp0, "4")
        val b5 = writeIdempotentBatchWithValue(transactionManager, tp0, "5")
        assertEquals(5, transactionManager.sequenceNumber(tp0))

        // First batch succeeds
        val b1AppendTime = time.milliseconds()
        val b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        b1.complete(baseOffset = 500, logAppendTime = b1AppendTime)
        transactionManager.handleCompletedBatch(b1, b1Response)

        // We get an UNKNOWN_PRODUCER_ID, so bump the epoch and set sequence numbers back to 0
        val b2Response = ProduceResponse.PartitionResponse(
            error = Errors.UNKNOWN_PRODUCER_ID,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 500,
        )
        assertTrue(transactionManager.canRetry(b2Response, b2))

        // Run sender loop to trigger epoch bump
        runUntil { transactionManager.producerIdAndEpoch.epoch.toInt() == 2 }
        assertEquals(expected = 2, actual = b2.producerEpoch.toInt())
        assertEquals(expected = 0, actual = b2.baseSequence)
        assertEquals(expected = 1, actual = b3.baseSequence)
        assertEquals(expected = 2, actual = b4.baseSequence)
        assertEquals(expected = 3, actual = b5.baseSequence)
    }

    @Test
    fun testBatchFailureAfterProducerReset() {
        // This tests a scenario where the producerId is reset while pending requests are still
        // inflight. The partition(s) that triggered the reset will have their sequence number
        // reset, while any others will not
        val epoch = Short.MAX_VALUE

        initializeTransactionManager(null)
        initializeIdempotentProducerId(producerId, epoch)

        val tp0b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        val tp1b1 = writeIdempotentBatchWithValue(transactionManager, tp1, "1")

        val tp0b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 400,
        )
        transactionManager.handleCompletedBatch(tp0b1, tp0b1Response)

        val tp1b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 400,
        )
        transactionManager.handleCompletedBatch(tp1b1, tp1b1Response)

        val tp0b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        val tp1b2 = writeIdempotentBatchWithValue(transactionManager, tp1, "2")

        assertEquals(2, transactionManager.sequenceNumber(tp0))
        assertEquals(2, transactionManager.sequenceNumber(tp1))

        val b1Response = ProduceResponse.PartitionResponse(
            error = Errors.UNKNOWN_PRODUCER_ID,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 400,
        )
        assertTrue(transactionManager.canRetry(b1Response, tp0b1))

        val b2Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 400,
        )
        transactionManager.handleCompletedBatch(tp1b1, b2Response)

        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()

        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = tp0b2, actual = transactionManager.nextBatchBySequence(tp0))
        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp1))
        assertEquals(expected = tp1b2, actual = transactionManager.nextBatchBySequence(tp1))
    }

    @Test
    fun testBatchCompletedAfterProducerReset() {
        val epoch = Short.MAX_VALUE

        initializeTransactionManager(null)
        initializeIdempotentProducerId(producerId, epoch)

        val b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        writeIdempotentBatchWithValue(transactionManager, tp1, "1")

        val b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        assertEquals(2, transactionManager.sequenceNumber(tp0))

        // The producerId might be reset due to a failure on another partition
        transactionManager.requestEpochBumpForPartition(tp1)
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        initializeIdempotentProducerId(producerId + 1, 0.toShort())

        // We continue to track the state of tp0 until in-flight requests complete
        val b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = time.milliseconds(),
            logStartOffset = 0,
        )
        transactionManager.handleCompletedBatch(b1, b1Response)

        assertEquals(expected = 2, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.lastAckedSequence(tp0))
        assertEquals(expected = b2, actual = transactionManager.nextBatchBySequence(tp0))
        assertEquals(
            expected = epoch,
            actual = transactionManager.nextBatchBySequence(tp0)!!.producerEpoch,
        )

        val b2Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = time.milliseconds(),
            logStartOffset = 0,
        )
        transactionManager.handleCompletedBatch(b2, b2Response)

        transactionManager.maybeUpdateProducerIdAndEpoch(tp0)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))
        assertNull(transactionManager.lastAckedSequence(tp0))
        assertNull(transactionManager.nextBatchBySequence(tp0))
    }

    @Test
    @Throws(Exception::class)
    fun testDuplicateSequenceAfterProducerReset() {
        initializeTransactionManager(null)
        initializeIdempotentProducerId(producerId, epoch)

        val metrics = Metrics(time = time)
        val requestTimeout = 10000
        val deliveryTimeout = 15000

        val accumulator = RecordAccumulator(
            logContext = logContext,
            batchSize = 16 * 1024,
            compression = CompressionType.NONE,
            lingerMs = 0,
            retryBackoffMs = 0L,
            deliveryTimeoutMs = deliveryTimeout,
            metrics = metrics,
            metricGrpName = "",
            time = time,
            apiVersions = apiVersions,
            transactionManager = transactionManager,
            free = BufferPool(
                totalMemory = 1024 * 1024L,
                poolableSize = 16 * 1024,
                metrics = metrics,
                time = time,
                metricGrpName = "",
            ),
        )

        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = false,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = MAX_RETRIES,
            metricsRegistry = SenderMetricsRegistry(metrics),
            time = time,
            requestTimeoutMs = requestTimeout,
            retryBackoffMs = 0,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )

        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        val responseFuture1 = accumulator.append(
            topic = tp0.topic,
            partition = tp0.partition,
            timestamp = time.milliseconds(),
            key = "1".toByteArray(),
            value = "1".toByteArray(),
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = MAX_BLOCK_TIMEOUT.toLong(),
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = singletonCluster(),
        ).future
        sender.runOnce()
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))

        time.sleep(requestTimeout.toLong())
        sender.runOnce()
        assertEquals(expected = 0, actual = client.inFlightRequestCount())
        assertTrue(transactionManager.hasInflightBatches(tp0))
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))
        sender.runOnce() // retry
        assertEquals(expected = 1, actual = client.inFlightRequestCount())
        assertTrue(transactionManager.hasInflightBatches(tp0))
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))

        time.sleep(5000) // delivery time out
        sender.runOnce()

        // The retried request will remain inflight until the request timeout
        // is reached even though the delivery timeout has expired and the
        // future has completed exceptionally.
        assertTrue(responseFuture1!!.isDone)
        assertFutureThrows(responseFuture1, TimeoutException::class.java)
        assertFalse(transactionManager.hasInFlightRequest)
        assertEquals(1, client.inFlightRequestCount())

        sender.runOnce() // bump the epoch
        assertEquals(
            expected = epoch + 1,
            actual = transactionManager.producerIdAndEpoch.epoch.toInt(),
        )
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))

        val responseFuture2 = accumulator.append(
            topic = tp0.topic,
            partition = tp0.partition,
            timestamp = time.milliseconds(),
            key = "2".toByteArray(),
            value = "2".toByteArray(),
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = MAX_BLOCK_TIMEOUT.toLong(),
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = singletonCluster(),
        ).future
        sender.runOnce()
        sender.runOnce()
        assertEquals(expected = 0, actual = transactionManager.firstInFlightSequence(tp0))
        assertEquals(expected = 1, actual = transactionManager.sequenceNumber(tp0))

        time.sleep(5000) // request time out again
        sender.runOnce()

        assertTrue(transactionManager.hasInflightBatches(tp0)) // the latter batch failed and retried
        assertFalse(responseFuture2!!.isDone)
    }

    private fun writeIdempotentBatchWithValue(
        manager: TransactionManager,
        tp: TopicPartition,
        value: String,
    ): ProducerBatch {
        manager.maybeUpdateProducerIdAndEpoch(tp)
        val seq = manager.sequenceNumber(tp)
        manager.incrementSequenceNumber(tp, 1)
        val batch = batchWithValue(tp, value)
        batch.setProducerState(manager.producerIdAndEpoch, seq, false)
        manager.addInFlightBatch(batch)
        batch.close()
        return batch
    }

    private fun batchWithValue(tp: TopicPartition, value: String): ProducerBatch {
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(64),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0,
        )
        val currentTimeMs = time.milliseconds()
        val batch = ProducerBatch(tp, builder, currentTimeMs)
        batch.tryAppend(
            timestamp = currentTimeMs,
            key = ByteArray(0),
            value = value.toByteArray(),
            headers = emptyArray(),
            callback = null,
            now = currentTimeMs,
        )
        return batch
    }

    @Test
    fun testSequenceNumberOverflow() {
        initializeTransactionManager(null)
        assertEquals(transactionManager.sequenceNumber(tp0), 0)
        transactionManager.incrementSequenceNumber(tp0, Int.MAX_VALUE)
        assertEquals(transactionManager.sequenceNumber(tp0), Int.MAX_VALUE)
        transactionManager.incrementSequenceNumber(tp0, 100)
        assertEquals(transactionManager.sequenceNumber(tp0), 99)
        transactionManager.incrementSequenceNumber(tp0, Int.MAX_VALUE)
        assertEquals(transactionManager.sequenceNumber(tp0), 98)
    }

    @Test
    fun testProducerIdReset() {
        initializeTransactionManager(null)
        initializeIdempotentProducerId(15L, Short.MAX_VALUE)
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp1))
        transactionManager.incrementSequenceNumber(tp0, 3)
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp0))
        transactionManager.incrementSequenceNumber(tp1, 3)
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp1))

        transactionManager.requestEpochBumpForPartition(tp0)
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        assertEquals(expected = 0, actual = transactionManager.sequenceNumber(tp0))
        assertEquals(expected = 3, actual = transactionManager.sequenceNumber(tp1))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testBasicTransaction() {
        doInitTransactions()

        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)

        val responseFuture = appendToAccumulator(tp0)

        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)

        prepareProduceResponse(Errors.NONE, producerId, epoch)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0))
        assertFalse(responseFuture.isDone)
        runUntil { responseFuture.isDone }

        val offsets = mapOf(tp1 to OffsetAndMetadata(1))
        val addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = offsets,
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )

        assertFalse(transactionManager.hasPendingOffsetCommits)

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)

        runUntil { transactionManager.hasPendingOffsetCommits }
        assertFalse(addOffsetsResult.isCompleted) // the result doesn't complete until TxnOffsetCommit returns

        val txnOffsetCommitResponse = mapOf(tp1 to Errors.NONE)
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse)

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP))
        runUntil { transactionManager.coordinator(CoordinatorType.GROUP) != null }
        assertTrue(transactionManager.hasPendingOffsetCommits)

        runUntil { !transactionManager.hasPendingOffsetCommits }
        assertTrue(addOffsetsResult.isCompleted) // We should only be done after both RPCs complete.

        transactionManager.beginCommit()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil { !transactionManager.hasOngoingTransaction }
        assertFalse(transactionManager.isCompleting)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
    }

    @Test
    fun testDisconnectAndRetry() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = true,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) == null }

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
    }

    @Test
    fun testInitializeTransactionsTwiceRaisesError() {
        doInitTransactions(producerId, epoch)
        assertTrue(transactionManager.hasProducerId)
        assertFailsWith<IllegalStateException> { transactionManager.initializeTransactions() }
    }

    @Test
    fun testUnsupportedFindCoordinator() {
        transactionManager.initializeTransactions()
        client.prepareUnsupportedVersionResponse { body ->
            val findCoordinatorRequest = body as FindCoordinatorRequest
            assertEquals(
                expected = CoordinatorType.TRANSACTION,
                actual = CoordinatorType.forId(findCoordinatorRequest.data().keyType),
            )
            assertEquals(transactionalId, findCoordinatorRequest.data().key)
            true
        }

        runUntil { transactionManager.hasFatalError }
        assertTrue(transactionManager.hasFatalError)
        assertIs<UnsupportedVersionException>(transactionManager.lastError())
    }

    @Test
    fun testUnsupportedInitTransactions() {
        transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId
        )

        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertFalse(transactionManager.hasError)
        client.prepareUnsupportedVersionResponse { body ->
            val initProducerIdRequest = (body as InitProducerIdRequest).data()
            assertEquals(
                expected = transactionalId,
                actual = initProducerIdRequest.transactionalId,
            )
            assertEquals(
                expected = transactionTimeoutMs,
                actual = initProducerIdRequest.transactionTimeoutMs,
            )
            true
        }

        runUntil { transactionManager.hasFatalError }
        assertTrue(transactionManager.hasFatalError)
        assertIs<UnsupportedVersionException>(transactionManager.lastError())
    }

    @Test
    fun testUnsupportedForMessageFormatInTxnOffsetCommit() {
        val tp = TopicPartition("foo", 0)

        doInitTransactions()

        transactionManager.beginTransaction()
        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp to OffsetAndMetadata(39)),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )

        prepareAddOffsetsToTxnResponse(
            error = Errors.NONE,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch
        )
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        prepareTxnOffsetCommitResponse(
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
            txnOffsetCommitResponse = mapOf(tp to Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT),
        )
        runUntil { transactionManager.hasError }

        assertIs<UnsupportedForMessageFormatException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<UnsupportedForMessageFormatException>(sendOffsetsResult.error())
        assertFatalError(UnsupportedForMessageFormatException::class.java)
    }

    @Test
    fun testFencedInstanceIdInTxnOffsetCommitByGroupMetadata() {
        val tp = TopicPartition(topic = "foo", partition = 0)
        val fencedMemberId = "fenced_member"

        doInitTransactions()

        transactionManager.beginTransaction()

        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp to OffsetAndMetadata(39)),
            groupMetadata = ConsumerGroupMetadata(
                groupId = consumerGroupId,
                generationId = 5,
                memberId = fencedMemberId,
                groupInstanceId = groupInstanceId,
            )
        )

        prepareAddOffsetsToTxnResponse(
            error = Errors.NONE,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
        )
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.GROUP) != null }

        client.prepareResponse(
            matcher = { request ->
                val txnOffsetCommitRequest = (request as TxnOffsetCommitRequest).data()

                assertEquals(consumerGroupId, txnOffsetCommitRequest.groupId)
                assertEquals(producerId, txnOffsetCommitRequest.producerId)
                assertEquals(epoch, txnOffsetCommitRequest.producerEpoch)

                txnOffsetCommitRequest.groupInstanceId.equals(groupInstanceId)
                        && txnOffsetCommitRequest.memberId != memberId
            },
            response = TxnOffsetCommitResponse(0, mapOf(tp to Errors.FENCED_INSTANCE_ID))
        )

        runUntil { transactionManager.hasError }
        assertIs<FencedInstanceIdException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<FencedInstanceIdException>(sendOffsetsResult.error())
        assertAbortableError(FencedInstanceIdException::class.java)
    }

    @Test
    fun testUnknownMemberIdInTxnOffsetCommitByGroupMetadata() {
        val tp = TopicPartition(topic = "foo", partition = 0)
        val unknownMemberId = "unknownMember"

        doInitTransactions()
        transactionManager.beginTransaction()

        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp to OffsetAndMetadata(39)),
            groupMetadata = ConsumerGroupMetadata(
                groupId = consumerGroupId,
                generationId = 5,
                memberId = unknownMemberId,
                groupInstanceId = null,
            )
        )

        prepareAddOffsetsToTxnResponse(
            error = Errors.NONE,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
        )
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.GROUP) != null }

        client.prepareResponse(
            matcher = { request ->
                val txnOffsetCommitRequest = (request as TxnOffsetCommitRequest).data()

                assertEquals(consumerGroupId, txnOffsetCommitRequest.groupId)
                assertEquals(producerId, txnOffsetCommitRequest.producerId)
                assertEquals(epoch, txnOffsetCommitRequest.producerEpoch)

                txnOffsetCommitRequest.memberId != memberId
            },
            response = TxnOffsetCommitResponse(0, mapOf(tp to Errors.UNKNOWN_MEMBER_ID)),
        )
        runUntil { transactionManager.hasError }

        assertIs<CommitFailedException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<CommitFailedException>(sendOffsetsResult.error())
        assertAbortableError(CommitFailedException::class.java)
    }

    @Test
    fun testIllegalGenerationInTxnOffsetCommitByGroupMetadata() {
        val tp = TopicPartition("foo", 0)
        val illegalGenerationId = 1

        doInitTransactions()
        transactionManager.beginTransaction()

        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp to OffsetAndMetadata(39)),
            groupMetadata = ConsumerGroupMetadata(
                groupId = consumerGroupId,
                generationId = illegalGenerationId,
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                groupInstanceId = null,
            ),
        )

        prepareAddOffsetsToTxnResponse(
            error = Errors.NONE,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
        )
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.GROUP) != null }

        prepareTxnOffsetCommitResponse(
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
            txnOffsetCommitResponse = mapOf(tp to Errors.ILLEGAL_GENERATION),
        )
        client.prepareResponse(
            matcher = { request ->
                val txnOffsetCommitRequest = (request as TxnOffsetCommitRequest).data()

                assertEquals(consumerGroupId, txnOffsetCommitRequest.groupId)
                assertEquals(producerId, txnOffsetCommitRequest.producerId)
                assertEquals(epoch, txnOffsetCommitRequest.producerEpoch)

                txnOffsetCommitRequest.generationId != generationId
            },
            response = TxnOffsetCommitResponse(0, mapOf(tp to Errors.ILLEGAL_GENERATION)),
        )
        runUntil { transactionManager.hasError }

        assertIs<CommitFailedException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<CommitFailedException>(sendOffsetsResult.error())
        assertAbortableError(CommitFailedException::class.java)
    }

    @Test
    fun testLookupCoordinatorOnDisconnectAfterSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        val initPidResult = transactionManager.initializeTransactions()

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )

        prepareInitPidResponse(
            error = Errors.NONE,
            shouldDisconnect = true,
            producerId = producerId,
            producerEpoch = epoch,
        )
        // send pid to coordinator, should get disconnected before receiving the response, and resend the
        // FindCoordinator and InitPid requests.
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) == null }

        assertNull(transactionManager.coordinator(CoordinatorType.TRANSACTION))
        assertFalse(initPidResult.isCompleted)
        assertFalse(transactionManager.hasProducerId)

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }

        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
        assertFalse(initPidResult.isCompleted)
        prepareInitPidResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil(initPidResult::isCompleted)

        assertTrue(initPidResult.isCompleted) // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasProducerId)
        assertEquals(producerId, transactionManager.producerIdAndEpoch.producerId)
        assertEquals(epoch, transactionManager.producerIdAndEpoch.epoch)
    }

    @Test
    fun testLookupCoordinatorOnDisconnectBeforeSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        val initPidResult = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )

        client.disconnect(brokerNode.idString())
        client.backoff(brokerNode, 100)
        // send pid to coordinator. Should get disconnected before the send and resend the
        // FindCoordinator and InitPid requests.
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) == null }
        time.sleep(110) // waiting for the backoff period for the node to expire.

        assertFalse(initPidResult.isCompleted)
        assertFalse(transactionManager.hasProducerId)

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
        assertFalse(initPidResult.isCompleted)
        prepareInitPidResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )

        runUntil(initPidResult::isCompleted)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(producerId, transactionManager.producerIdAndEpoch.producerId)
        assertEquals(epoch, transactionManager.producerIdAndEpoch.epoch)
    }

    @Test
    fun testLookupCoordinatorOnNotCoordinatorError() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        val initPidResult = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )

        prepareInitPidResponse(
            error = Errors.NOT_COORDINATOR,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) == null }

        assertFalse(initPidResult.isCompleted)
        assertFalse(transactionManager.hasProducerId)

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION))
        assertFalse(initPidResult.isCompleted)
        prepareInitPidResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )

        runUntil(initPidResult::isCompleted)
        assertTrue(transactionManager.hasProducerId)
        assertEquals(producerId, transactionManager.producerIdAndEpoch.producerId)
        assertEquals(epoch, transactionManager.producerIdAndEpoch.epoch)
    }

    @Test
    fun testTransactionalIdAuthorizationFailureInFindCoordinator() {
        val initPidResult = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.hasError }
        assertTrue(transactionManager.hasFatalError)
        assertIs<TransactionalIdAuthorizationException>(transactionManager.lastError())
        assertFalse(initPidResult.isSuccessful)
        assertFailsWith<TransactionalIdAuthorizationException> { initPidResult.await() }
        assertFatalError(TransactionalIdAuthorizationException::class.java)
    }

    @Test
    fun testTransactionalIdAuthorizationFailureInInitProducerId() {
        val initPidResult = transactionManager.initializeTransactions()

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }

        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION))

        prepareInitPidResponse(
            error = Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
        )
        runUntil { transactionManager.hasError }

        assertTrue(initPidResult.isCompleted)
        assertFalse(initPidResult.isSuccessful)
        assertFailsWith<TransactionalIdAuthorizationException> { initPidResult.await() }
        assertAbortableError(TransactionalIdAuthorizationException::class.java)
    }

    @Test
    fun testGroupAuthorizationFailureInFindCoordinator() {
        doInitTransactions()

        transactionManager.beginTransaction()
        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(
                TopicPartition(topic = "foo", partition = 0) to OffsetAndMetadata(39L),
            ),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId)
        )

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)
        runUntil { !transactionManager.hasPartitionsToAdd }

        prepareFindCoordinatorResponse(
            error = Errors.GROUP_AUTHORIZATION_FAILED,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        runUntil { transactionManager.hasError }
        assertIs<GroupAuthorizationException>(transactionManager.lastError())

        runUntil(sendOffsetsResult::isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)

        val exception = assertIs<GroupAuthorizationException>(sendOffsetsResult.error())
        assertEquals(consumerGroupId, exception.groupId)

        assertAbortableError(GroupAuthorizationException::class.java)
    }

    @Test
    fun testGroupAuthorizationFailureInTxnOffsetCommit() {
        val tp1 = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()

        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp1 to OffsetAndMetadata(39L)),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)
        runUntil { !transactionManager.hasPartitionsToAdd }

        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        prepareTxnOffsetCommitResponse(
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
            txnOffsetCommitResponse = mapOf(tp1 to Errors.GROUP_AUTHORIZATION_FAILED),
        )
        runUntil { transactionManager.hasError }

        assertIs<GroupAuthorizationException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)

        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<GroupAuthorizationException>(sendOffsetsResult.error())
        assertFalse(transactionManager.hasPendingOffsetCommits)

        val exception = sendOffsetsResult.error() as GroupAuthorizationException?
        assertEquals(consumerGroupId, exception!!.groupId)

        assertAbortableError(GroupAuthorizationException::class.java)
    }

    @Test
    fun testTransactionalIdAuthorizationFailureInAddOffsetsToTxn() {
        val tp = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()

        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp to OffsetAndMetadata(39)),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId)
        )
        prepareAddOffsetsToTxnResponse(
            error = Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil { transactionManager.hasError }

        assertIs<TransactionalIdAuthorizationException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<TransactionalIdAuthorizationException>(sendOffsetsResult.error())
        assertFatalError(TransactionalIdAuthorizationException::class.java)
    }

    @Test
    fun testInvalidTxnStateFailureInAddOffsetsToTxn() {
        val tp = TopicPartition("foo", 0)

        doInitTransactions()

        transactionManager.beginTransaction()
        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = Collections.singletonMap(tp, OffsetAndMetadata(39L)),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )

        prepareAddOffsetsToTxnResponse(Errors.INVALID_TXN_STATE, consumerGroupId, producerId, epoch)
        runUntil { transactionManager.hasError }
        assertIs<InvalidTxnStateException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<InvalidTxnStateException>(sendOffsetsResult.error())

        assertFatalError(InvalidTxnStateException::class.java)
    }

    @Test
    fun testTransactionalIdAuthorizationFailureInTxnOffsetCommit() {
        val tp = TopicPartition("foo", 0)
        doInitTransactions()
        transactionManager.beginTransaction()
        val sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = mapOf(tp to OffsetAndMetadata(39L)),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)
        runUntil { !transactionManager.hasPartitionsToAdd }
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        prepareTxnOffsetCommitResponse(
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
            txnOffsetCommitResponse = mapOf(tp to Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        )
        runUntil { transactionManager.hasError }
        assertIs<TransactionalIdAuthorizationException>(transactionManager.lastError())
        assertTrue(sendOffsetsResult.isCompleted)
        assertFalse(sendOffsetsResult.isSuccessful)
        assertIs<TransactionalIdAuthorizationException>(sendOffsetsResult.error())
        assertFatalError(TransactionalIdAuthorizationException::class.java)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTopicAuthorizationFailureInAddPartitions() {
        val tp0 = TopicPartition(topic = "foo", partition = 0)
        val tp1 = TopicPartition(topic = "bar", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        transactionManager.maybeAddPartition(tp1)

        val firstPartitionAppend = appendToAccumulator(tp0)
        val secondPartitionAppend = appendToAccumulator(tp1)
        val errors = mapOf(
            tp0 to Errors.TOPIC_AUTHORIZATION_FAILED,
            tp1 to Errors.OPERATION_NOT_ATTEMPTED,
        )
        prepareAddPartitionsToTxn(errors)
        runUntil { transactionManager.hasError }

        assertIs<TopicAuthorizationException>(transactionManager.lastError())
        assertFalse(transactionManager.isPartitionPendingAdd(tp0))
        assertFalse(transactionManager.isPartitionPendingAdd(tp1))
        assertFalse(transactionManager.isPartitionAdded(tp0))
        assertFalse(transactionManager.isPartitionAdded(tp1))
        assertFalse(transactionManager.hasPartitionsToAdd)

        val exception = transactionManager.lastError() as TopicAuthorizationException
        assertEquals(setOf(tp0.topic), exception.unauthorizedTopics)
        assertAbortableError(TopicAuthorizationException::class.java)
        sender.runOnce()

        assertFutureThrows(
            future = firstPartitionAppend!!,
            exceptionCauseClass = KafkaException::class.java
        )
        assertFutureThrows(
            future = secondPartitionAppend!!,
            exceptionCauseClass = KafkaException::class.java
        )
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCommitWithTopicAuthorizationFailureInAddPartitionsInFlight() {
        val tp0 = TopicPartition("foo", 0)
        val tp1 = TopicPartition("bar", 0)
        doInitTransactions()

        // Begin a transaction, send two records, and begin commit
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        transactionManager.maybeAddPartition(tp1)

        val firstPartitionAppend = appendToAccumulator(tp0)
        val secondPartitionAppend = appendToAccumulator(tp1)
        val commitResult = transactionManager.beginCommit()

        // We send the AddPartitionsToTxn request in the first sender call
        sender.runOnce()
        assertFalse(transactionManager.hasError)
        assertFalse(commitResult.isCompleted)
        assertFalse(firstPartitionAppend!!.isDone())

        // The AddPartitionsToTxn response returns in the next call with the error
        val errors = mapOf(
            tp0 to Errors.TOPIC_AUTHORIZATION_FAILED,
            tp1 to Errors.OPERATION_NOT_ATTEMPTED,
        )
        val result = AddPartitionsToTxnResponse.resultForTransaction(
            transactionalId = AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID,
            errors = errors,
        )
        val data = AddPartitionsToTxnResponseData()
            .setResultsByTopicV3AndBelow(result.topicResults)
            .setThrottleTimeMs(0)
        client.respond(
            matcher = { body ->
                val request = body as AddPartitionsToTxnRequest
                assertEquals(errors.keys.toSet(), getPartitionsFromV3Request(request).toSet())
                true
            },
            response = AddPartitionsToTxnResponse(data),
        )
        sender.runOnce()

        assertTrue(transactionManager.hasError)
        assertFalse(commitResult.isCompleted)
        assertFalse(firstPartitionAppend.isDone())
        assertFalse(secondPartitionAppend!!.isDone())

        // The next call aborts the records, which have not yet been sent. It should
        // not block because there are no requests pending and we still need to cancel
        // the pending transaction commit.
        sender.runOnce()
        assertTrue(commitResult.isCompleted)
        assertFutureThrows(
            future = firstPartitionAppend,
            exceptionCauseClass = KafkaException::class.java,
        )
        assertFutureThrows(
            future = secondPartitionAppend,
            exceptionCauseClass = KafkaException::class.java,
        )
        assertIs<TopicAuthorizationException>(commitResult.error())
    }

    @Test
    @Throws(Exception::class)
    fun testRecoveryFromAbortableErrorTransactionNotStarted() {
        val unauthorizedPartition = TopicPartition("foo", 0)
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(unauthorizedPartition)
        var responseFuture = appendToAccumulator(unauthorizedPartition)!!
        prepareAddPartitionsToTxn(
            mapOf(unauthorizedPartition to Errors.TOPIC_AUTHORIZATION_FAILED),
        )
        runUntil { !client.hasPendingResponses() }
        assertTrue(transactionManager.hasAbortableError)
        val abortResult = transactionManager.beginAbort()
        runUntil { responseFuture.isDone }
        assertProduceFutureFailed(responseFuture)

        // No partitions added, so no need to prepare EndTxn response
        runUntil(transactionManager::isReady)
        assertFalse(transactionManager.hasPartitionsToAdd)
        assertFalse(accumulator.hasIncomplete())
        assertTrue(abortResult.isSuccessful)
        abortResult.await()

        // ensure we can now start a new transaction
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        responseFuture = appendToAccumulator(tp0)!!
        prepareAddPartitionsToTxn(mapOf(tp0 to Errors.NONE))
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertFalse(transactionManager.hasPartitionsToAdd)
        transactionManager.beginCommit()
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture.isDone() }
        assertNotNull(responseFuture.get())
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(transactionManager::isReady)
    }

    @Test
    @Throws(Exception::class)
    fun testRetryAbortTransactionAfterTimeout() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        appendToAccumulator(tp0)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        val result = transactionManager.beginAbort()
        assertFailsWith<TimeoutException> { result.await(0, TimeUnit.MILLISECONDS) }
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil(transactionManager::isReady)
        assertTrue(result.isSuccessful)
        assertFalse(result.isAcked)
        assertFalse(transactionManager.hasOngoingTransaction)
        assertFailsWith<IllegalStateException> { transactionManager.initializeTransactions() }
        assertFailsWith<IllegalStateException> { transactionManager.beginTransaction() }
        assertFailsWith<IllegalStateException> { transactionManager.beginCommit() }
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
        assertSame(result, transactionManager.beginAbort())
        result.await()
        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)
    }

    @Test
    @Throws(Exception::class)
    fun testRetryCommitTransactionAfterTimeout() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        appendToAccumulator(tp0)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        val result = transactionManager.beginCommit()
        assertFailsWith<TimeoutException> { result.await(0, TimeUnit.MILLISECONDS) }
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(transactionManager::isReady)
        assertTrue(result.isSuccessful)
        assertFalse(result.isAcked)
        assertFalse(transactionManager.hasOngoingTransaction)
        assertFailsWith<IllegalStateException> { transactionManager.initializeTransactions() }
        assertFailsWith<IllegalStateException> { transactionManager.beginTransaction() }
        assertFailsWith<IllegalStateException> { transactionManager.beginAbort() }
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
        assertSame(result, transactionManager.beginCommit())
        result.await()
        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)
    }

    @Test
    fun testRetryInitTransactionsAfterTimeout() {
        val result = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
        assertFailsWith<TimeoutException> { result.await(0, TimeUnit.MILLISECONDS) }
        prepareInitPidResponse(Errors.NONE, false, producerId, epoch)
        runUntil { transactionManager.hasProducerId }
        assertTrue(result.isSuccessful)
        assertFalse(result.isAcked)

        // At this point, the InitProducerId call has returned, but the user has yet
        // to complete the call to `initTransactions`. Other transitions should be
        // rejected until they do.
        assertFailsWith<IllegalStateException> { transactionManager.beginTransaction() }
        assertFailsWith<IllegalStateException> { transactionManager.beginAbort() }
        assertFailsWith<IllegalStateException> { transactionManager.beginCommit() }
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
        assertSame(result, transactionManager.initializeTransactions())
        result.await()
        assertTrue(result.isAcked)
        assertFailsWith<IllegalStateException> { transactionManager.initializeTransactions() }
        transactionManager.beginTransaction()
        assertTrue(transactionManager.hasOngoingTransaction)
    }

    @Test
    @Throws(Exception::class)
    fun testRecoveryFromAbortableErrorTransactionStarted() {
        val unauthorizedPartition = TopicPartition("foo", 0)
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        val authorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        transactionManager.maybeAddPartition(unauthorizedPartition)
        val unauthorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition)
        prepareAddPartitionsToTxn(
            mapOf(unauthorizedPartition to Errors.TOPIC_AUTHORIZATION_FAILED),
        )
        runUntil { transactionManager.hasAbortableError }
        assertTrue(transactionManager.isPartitionAdded(tp0))
        assertFalse(transactionManager.isPartitionAdded(unauthorizedPartition))
        assertFalse(authorizedTopicProduceFuture!!.isDone)
        assertFalse(unauthorizedTopicProduceFuture!!.isDone)
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        val result = transactionManager.beginAbort()
        runUntil(transactionManager::isReady)

        // neither produce request has been sent, so they should both be failed immediately
        assertProduceFutureFailed(authorizedTopicProduceFuture)
        assertProduceFutureFailed(unauthorizedTopicProduceFuture)
        assertFalse(transactionManager.hasPartitionsToAdd)
        assertFalse(accumulator.hasIncomplete())
        assertTrue(result.isSuccessful)
        result.await()

        // ensure we can now start a new transaction
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val nextTransactionFuture = appendToAccumulator(tp0)!!
        prepareAddPartitionsToTxn(mapOf(tp0 to Errors.NONE))
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertFalse(transactionManager.hasPartitionsToAdd)
        transactionManager.beginCommit()
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { nextTransactionFuture.isDone() }
        assertNotNull(nextTransactionFuture.get())
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(transactionManager::isReady)
    }

    @Test
    @Throws(Exception::class)
    fun testRecoveryFromAbortableErrorProduceRequestInRetry() {
        val unauthorizedPartition = TopicPartition("foo", 0)
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        val authorizedTopicProduceFuture = appendToAccumulator(tp0)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        accumulator.beginFlush()
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, producerId, epoch)
        runUntil { !client.hasPendingResponses() }
        assertFalse(authorizedTopicProduceFuture!!.isDone)
        assertTrue(accumulator.hasIncomplete())
        transactionManager.maybeAddPartition(unauthorizedPartition)
        val unauthorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition)!!
        prepareAddPartitionsToTxn(
            mapOf(unauthorizedPartition to Errors.TOPIC_AUTHORIZATION_FAILED),
        )
        runUntil { transactionManager.hasAbortableError }
        assertTrue(transactionManager.isPartitionAdded(tp0))
        assertFalse(transactionManager.isPartitionAdded(unauthorizedPartition))
        assertFalse(authorizedTopicProduceFuture.isDone)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { authorizedTopicProduceFuture.isDone }
        assertProduceFutureFailed(unauthorizedTopicProduceFuture)
        assertNotNull(authorizedTopicProduceFuture.get())
        assertTrue(authorizedTopicProduceFuture.isDone)
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        val abortResult = transactionManager.beginAbort()
        runUntil(transactionManager::isReady)
        // neither produce request has been sent, so they should both be failed immediately
        assertTrue(transactionManager.isReady)
        assertFalse(transactionManager.hasPartitionsToAdd)
        assertFalse(accumulator.hasIncomplete())
        assertTrue(abortResult.isSuccessful)
        abortResult.await()

        // ensure we can now start a new transaction
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val nextTransactionFuture = appendToAccumulator(tp0)
        prepareAddPartitionsToTxn(mapOf(tp0 to Errors.NONE))
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertFalse(transactionManager.hasPartitionsToAdd)
        transactionManager.beginCommit()
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { nextTransactionFuture!!.isDone() }
        assertNotNull(nextTransactionFuture!!.get())
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(transactionManager::isReady)
    }

    @Test
    fun testTransactionalIdAuthorizationFailureInAddPartitions() {
        val tp = TopicPartition(topic = "foo", partition = 0)
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp)
        prepareAddPartitionsToTxn(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        runUntil { transactionManager.hasError }
        assertIs<TransactionalIdAuthorizationException>(transactionManager.lastError())
        assertFatalError(TransactionalIdAuthorizationException::class.java)
    }

    @Test
    fun testInvalidTxnStateInAddPartitions() {
        val tp = TopicPartition("foo", 0)
        
        doInitTransactions()
        
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp)
        
        prepareAddPartitionsToTxn(tp, Errors.INVALID_TXN_STATE)
        runUntil { transactionManager.hasError }
        assertIs<InvalidTxnStateException>(transactionManager.lastError())

        assertFatalError(InvalidTxnStateException::class.java)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testFlushPendingPartitionsOnCommit() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        val commitResult = transactionManager.beginCommit()

        // we have an append, an add partitions request, and now also an endtxn.
        // The order should be:
        //  1. Add Partitions
        //  2. Produce
        //  3. EndTxn.
        assertFalse(transactionManager.transactionContainsPartition(tp0))
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        assertFalse(responseFuture.isDone)
        assertFalse(commitResult.isCompleted)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture.isDone }
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        assertFalse(commitResult.isCompleted)
        assertTrue(transactionManager.hasOngoingTransaction)
        assertTrue(transactionManager.isCompleting)
        runUntil(commitResult::isCompleted)
        assertFalse(transactionManager.hasOngoingTransaction)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testMultipleAddPartitionsPerForOneProduce() {
        doInitTransactions()
        transactionManager.beginTransaction()
        // User does one producer.send
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        assertFalse(transactionManager.transactionContainsPartition(tp0))

        // Sender flushes one add partitions. The produce goes next.
        runUntil { transactionManager.transactionContainsPartition(tp0) }

        // In the mean time, the user does a second produce to a different partition
        transactionManager.maybeAddPartition(tp1)
        val secondResponseFuture = appendToAccumulator(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp1, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        assertFalse(transactionManager.transactionContainsPartition(tp1))
        assertFalse(responseFuture.isDone)
        assertFalse(secondResponseFuture!!.isDone)

        // The second add partitions should go out here.
        runUntil { transactionManager.transactionContainsPartition(tp1) }
        assertFalse(responseFuture.isDone)
        assertFalse(secondResponseFuture.isDone)

        // Finally we get to the produce.
        runUntil { responseFuture.isDone }
        assertTrue(secondResponseFuture.isDone)
    }

    @ParameterizedTest
    @EnumSource(names = ["UNKNOWN_TOPIC_OR_PARTITION", "REQUEST_TIMED_OUT", "COORDINATOR_LOAD_IN_PROGRESS", "CONCURRENT_TRANSACTIONS"])
    fun testRetriableErrors2(error: Errors) {
        // Ensure FindCoordinator retries.
        val result = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(error, false, CoordinatorType.TRANSACTION, transactionalId)
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )

        // Ensure InitPid retries.
        prepareInitPidResponse(
            error = error,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )
        prepareInitPidResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil { transactionManager.hasProducerId }
        result.await()
        transactionManager.beginTransaction()

        // Ensure AddPartitionsToTxn retries. Since CONCURRENT_TRANSACTIONS is handled differently here, we substitute.
        val addPartitionsToTxnError =
            if (error == Errors.CONCURRENT_TRANSACTIONS) Errors.COORDINATOR_LOAD_IN_PROGRESS
            else error
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(addPartitionsToTxnError, tp0, epoch, producerId)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        runUntil { transactionManager.transactionContainsPartition(tp0) }

        // Ensure txnOffsetCommit retries is tested in testRetriableErrorInTxnOffsetCommit.

        // Ensure EndTxn retries.
        val abortResult = transactionManager.beginCommit()
        prepareEndTxnResponse(error, TransactionResult.COMMIT, producerId, epoch)
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
    }

    @Test
    fun testCoordinatorNotAvailable() {
        // Ensure FindCoordinator with COORDINATOR_NOT_AVAILABLE error retries.
        val result = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.COORDINATOR_NOT_AVAILABLE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
        prepareInitPidResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil { transactionManager.hasProducerId }
        result.await()
    }

    @Test
    fun testProducerFencedExceptionInInitProducerId() {
        verifyProducerFencedForInitProducerId(Errors.PRODUCER_FENCED)
    }

    @Test
    fun testInvalidProducerEpochConvertToProducerFencedInInitProducerId() {
        verifyProducerFencedForInitProducerId(Errors.INVALID_PRODUCER_EPOCH)
    }

    private fun verifyProducerFencedForInitProducerId(error: Errors) {
        val result = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
        prepareInitPidResponse(error, false, producerId, epoch)
        runUntil { transactionManager.hasError }
        assertFailsWith<ProducerFencedException> { result.await() }
        assertFailsWith<ProducerFencedException> { transactionManager.beginTransaction() }
        assertFailsWith<ProducerFencedException> { transactionManager.beginCommit() }
        assertFailsWith<ProducerFencedException> { transactionManager.beginAbort() }
        assertFailsWith<ProducerFencedException> {
            transactionManager.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata("dummyId"),
            )
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testProducerFencedInAddPartitionToTxn() {
        verifyProducerFencedForAddPartitionsToTxn(Errors.PRODUCER_FENCED)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testInvalidProducerEpochConvertToProducerFencedInAddPartitionToTxn() {
        verifyProducerFencedForAddPartitionsToTxn(Errors.INVALID_PRODUCER_EPOCH)
    }

    @Throws(InterruptedException::class)
    private fun verifyProducerFencedForAddPartitionsToTxn(error: Errors) {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(error, tp0, epoch, producerId)
        verifyProducerFenced(responseFuture)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testProducerFencedInAddOffSetsToTxn() {
        verifyProducerFencedForAddOffsetsToTxn(Errors.INVALID_PRODUCER_EPOCH)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testInvalidProducerEpochConvertToProducerFencedInAddOffSetsToTxn() {
        verifyProducerFencedForAddOffsetsToTxn(Errors.INVALID_PRODUCER_EPOCH)
    }

    @Suppress("SameParameterValue")
    @Throws(InterruptedException::class)
    private fun verifyProducerFencedForAddOffsetsToTxn(error: Errors) {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.sendOffsetsToTransaction(
            offsets = emptyMap(),
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        prepareAddOffsetsToTxnResponse(error, consumerGroupId, producerId, epoch)
        verifyProducerFenced(responseFuture)
    }

    @Throws(InterruptedException::class)
    private fun verifyProducerFenced(responseFuture: Future<RecordMetadata>) {
        runUntil { responseFuture.isDone }
        assertTrue(transactionManager.hasError)
        try {
            // make sure the produce was expired.
            responseFuture.get()
            fail("Expected to get a ExecutionException from the response")
        } catch (e: ExecutionException) {
            assertIs<ProducerFencedException>(e.cause)
        }

        // make sure the exception was thrown directly from the follow-up calls.
        assertFailsWith<ProducerFencedException> { transactionManager.beginTransaction() }
        assertFailsWith<ProducerFencedException> { transactionManager.beginCommit() }
        assertFailsWith<ProducerFencedException> { transactionManager.beginAbort() }
        assertFailsWith<ProducerFencedException> {
            transactionManager.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata("dummyId")
            )
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testInvalidProducerEpochConvertToProducerFencedInEndTxn() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val commitResult = transactionManager.beginCommit()
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        prepareEndTxnResponse(
            error = Errors.INVALID_PRODUCER_EPOCH,
            result = TransactionResult.COMMIT,
            producerId = producerId,
            epoch = epoch,
        )
        runUntil(commitResult::isCompleted)
        runUntil { responseFuture.isDone }
        assertFailsWith<KafkaException> { commitResult.await() }
        assertFalse(commitResult.isSuccessful)
        assertTrue(commitResult.isAcked)

        // make sure the exception was thrown directly from the follow-up calls.
        assertFailsWith<KafkaException> { transactionManager.beginTransaction() }
        assertFailsWith<KafkaException> { transactionManager.beginCommit() }
        assertFailsWith<KafkaException> { transactionManager.beginAbort() }
        assertFailsWith<KafkaException> {
            transactionManager.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata("dummyId"),
            )
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testInvalidProducerEpochFromProduce() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.INVALID_PRODUCER_EPOCH, producerId, epoch)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        sender.runOnce()
        runUntil { responseFuture.isDone }
        assertTrue(transactionManager.hasError)
        transactionManager.beginAbort()
        var handler = transactionManager.nextRequest(false)

        // First we will get an EndTxn for abort.
        assertNotNull(handler)
        assertIs<EndTxnRequest.Builder>(handler.requestBuilder())
        handler = transactionManager.nextRequest(false)

        // Second we will see an InitPid for handling InvalidProducerEpoch.
        assertNotNull(handler)
        assertIs<InitProducerIdRequest.Builder>(handler.requestBuilder())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testDisallowCommitOnProduceFailure() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        val commitResult = transactionManager.beginCommit()
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, producerId, epoch)
        runUntil(commitResult::isCompleted) // commit should be cancelled with exception without being sent.
        assertFailsWith<KafkaException> { commitResult.await() }
        assertFutureThrows(responseFuture, OutOfOrderSequenceException::class.java)

        // Commit is not allowed, so let's abort and try again.
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, (epoch + 1).toShort())
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAllowAbortOnProduceFailure() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, producerId, epoch)

        // Because this is a failure that triggers an epoch bump, the abort will trigger an
        // InitProducerId call
        runUntil { transactionManager.hasAbortableError }
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, (epoch + 1).toShort())
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAbortableErrorWhileAbortInProgress() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        runUntil { !accumulator.hasUndrained() }
        val abortResult = transactionManager.beginAbort()
        assertTrue(transactionManager.isAborting)
        assertFalse(transactionManager.hasError)
        sendProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, producerId, epoch)
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil { responseFuture.isDone }

        // we do not transition to ABORTABLE_ERROR since we were already aborting
        assertTrue(transactionManager.isAborting)
        assertFalse(transactionManager.hasError)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
    }

    @Test
    @Throws(Exception::class)
    fun testCommitTransactionWithUnsentProduceRequest() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        runUntil { !client.hasPendingResponses() }
        assertTrue(accumulator.hasUndrained())

        // committing the transaction should cause the unsent batch to be flushed
        transactionManager.beginCommit()
        runUntil { !accumulator.hasUndrained() }
        assertTrue(accumulator.hasIncomplete())
        assertFalse(transactionManager.hasInFlightRequest)
        assertFalse(responseFuture.isDone)

        // until the produce future returns, we will not send EndTxn
        val numRuns = AtomicInteger(0)
        runUntil { numRuns.incrementAndGet() >= 4 }
        assertFalse(accumulator.hasUndrained())
        assertTrue(accumulator.hasIncomplete())
        assertFalse(transactionManager.hasInFlightRequest)
        assertFalse(responseFuture.isDone)

        // now the produce response returns
        sendProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture.isDone }
        assertFalse(accumulator.hasUndrained())
        assertFalse(accumulator.hasIncomplete())
        assertFalse(transactionManager.hasInFlightRequest)

        // now we send EndTxn
        runUntil { transactionManager.hasInFlightRequest }
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(transactionManager::isReady)
        assertFalse(transactionManager.hasInFlightRequest)
    }

    @Test
    @Throws(Exception::class)
    fun testCommitTransactionWithInFlightProduceRequest() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.NONE)
        runUntil { !transactionManager.hasPartitionsToAdd }
        assertTrue(accumulator.hasUndrained())
        accumulator.beginFlush()
        runUntil { !accumulator.hasUndrained() }
        assertFalse(accumulator.hasUndrained())
        assertTrue(accumulator.hasIncomplete())
        assertFalse(transactionManager.hasInFlightRequest)

        // now we begin the commit with the produce request still pending
        transactionManager.beginCommit()
        val numRuns = AtomicInteger(0)
        runUntil { numRuns.incrementAndGet() >= 4 }
        assertFalse(accumulator.hasUndrained())
        assertTrue(accumulator.hasIncomplete())
        assertFalse(transactionManager.hasInFlightRequest)
        assertFalse(responseFuture!!.isDone)

        // now the produce response returns
        sendProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture.isDone }
        assertFalse(accumulator.hasUndrained())
        assertFalse(accumulator.hasIncomplete())
        assertFalse(transactionManager.hasInFlightRequest)

        // now we send EndTxn
        runUntil { transactionManager.hasInFlightRequest }
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil(transactionManager::isReady)
        assertFalse(transactionManager.hasInFlightRequest)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testFindCoordinatorAllowedInAbortableErrorState() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        runUntil { transactionManager.hasInFlightRequest }
        transactionManager.transitionToAbortableError(KafkaException())
        sendAddPartitionsToTxnResponse(Errors.NOT_COORDINATOR, tp0, epoch, producerId)
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) == null }
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(
            expected = brokerNode,
            actual = transactionManager.coordinator(CoordinatorType.TRANSACTION),
        )
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testCancelUnsentAddPartitionsAndProduceOnAbort() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        val abortResult = transactionManager.beginAbort()
        // note since no partitions were added to the transaction, no EndTxn will be sent
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        assertFutureThrows(responseFuture, KafkaException::class.java)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAbortResendsAddPartitionErrorIfRetried() {
        doInitTransactions(producerId, epoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, epoch, producerId)
        val responseFuture = appendToAccumulator(tp0)!!
        runUntil { !client.hasPendingResponses() }
        assertFalse(responseFuture.isDone)
        val abortResult = transactionManager.beginAbort()

        // we should resend the AddPartitions
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        assertFutureThrows(
            responseFuture,
            KafkaException::class.java
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAbortResendsProduceRequestIfRetried() {
        doInitTransactions(producerId, epoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, producerId, epoch)
        val responseFuture = appendToAccumulator(tp0)
        runUntil { !client.hasPendingResponses() }
        assertFalse(responseFuture!!.isDone)
        val abortResult = transactionManager.beginAbort()

        // we should resend the ProduceRequest before aborting
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        val recordMetadata = responseFuture.get()
        assertEquals(tp0.topic, recordMetadata?.topic)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHandlingOfUnknownTopicPartitionErrorOnAddPartitions() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, epoch, producerId)
        runUntil { !client.hasPendingResponses() }
        assertFalse(transactionManager.transactionContainsPartition(tp0)) // The partition should not yet be added.
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        runUntil { responseFuture.isDone }
    }

    @Test
    fun testHandlingOfUnknownTopicPartitionErrorOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }

    @Test
    fun testHandlingOfCoordinatorLoadingErrorOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    }

    private fun testRetriableErrorInTxnOffsetCommit(error: Errors) {
        doInitTransactions()
        transactionManager.beginTransaction()
        val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        offsets[tp0] = OffsetAndMetadata(1)
        offsets[tp1] = OffsetAndMetadata(1)
        val addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, ConsumerGroupMetadata(consumerGroupId)
        )
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)
        runUntil { !client.hasPendingResponses() }
        assertFalse(addOffsetsResult.isCompleted) // The request should complete only after the TxnOffsetCommit completes.
        val txnOffsetCommitResponse: MutableMap<TopicPartition, Errors> = HashMap()
        txnOffsetCommitResponse[tp0] = Errors.NONE
        txnOffsetCommitResponse[tp1] = error
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId)
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse)
        assertNull(transactionManager.coordinator(CoordinatorType.GROUP))
        runUntil {
            transactionManager.coordinator(
                CoordinatorType.GROUP
            ) != null
        }
        assertTrue(transactionManager.hasPendingOffsetCommits)
        runUntil { transactionManager.hasPendingOffsetCommits } // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted) // We should only be done after both RPCs complete successfully.
        txnOffsetCommitResponse[tp1] = Errors.NONE
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse)
        runUntil(addOffsetsResult::isCompleted)
        assertTrue(addOffsetsResult.isSuccessful)
    }

    @Test
    fun testHandlingOfProducerFencedErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.PRODUCER_FENCED)
    }

    @Test
    fun testHandlingOfTransactionalIdAuthorizationFailedErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
    }

    @Test
    fun testHandlingOfInvalidProducerEpochErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.INVALID_PRODUCER_EPOCH, Errors.PRODUCER_FENCED)
    }

    @Test
    fun testHandlingOfUnsupportedForMessageFormatErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT)
    }

    private fun testFatalErrorInTxnOffsetCommit(triggeredError: Errors, resultingError: Errors = triggeredError) {
        doInitTransactions()
        transactionManager.beginTransaction()
        val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        offsets[tp0] = OffsetAndMetadata(1)
        offsets[tp1] = OffsetAndMetadata(1)
        val addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, ConsumerGroupMetadata(consumerGroupId)
        )
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)
        runUntil { !client.hasPendingResponses() }
        assertFalse(addOffsetsResult.isCompleted) // The request should complete only after the TxnOffsetCommit completes.
        val txnOffsetCommitResponse: MutableMap<TopicPartition, Errors> = HashMap()
        txnOffsetCommitResponse[tp0] = Errors.NONE
        txnOffsetCommitResponse[tp1] = triggeredError
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse)
        runUntil(addOffsetsResult::isCompleted)
        assertFalse(addOffsetsResult.isSuccessful)
        assertEquals(resultingError.exception!!::class.java, addOffsetsResult.error()!!::class.java)
    }

    @Test
    @Throws(Exception::class)
    fun shouldNotAddPartitionsToTransactionWhenTopicAuthorizationFailed() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxn(tp0, Errors.TOPIC_AUTHORIZATION_FAILED)
        runUntil { transactionManager.hasError }
        assertFalse(transactionManager.transactionContainsPartition(tp0))
    }

    @Test
    fun shouldNotSendAbortTxnRequestWhenOnlyAddPartitionsRequestFailed() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.TOPIC_AUTHORIZATION_FAILED, tp0, epoch, producerId)
        runUntil { !client.hasPendingResponses() }
        val abortResult = transactionManager.beginAbort()
        assertFalse(abortResult.isCompleted)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
    }

    @Test
    fun shouldNotSendAbortTxnRequestWhenOnlyAddOffsetsRequestFailed() {
        doInitTransactions()
        transactionManager.beginTransaction()
        val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        offsets[tp1] = OffsetAndMetadata(1)
        transactionManager.sendOffsetsToTransaction(
            offsets = offsets,
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )
        val abortResult = transactionManager.beginAbort()
        prepareAddOffsetsToTxnResponse(
            error = Errors.GROUP_AUTHORIZATION_FAILED,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil(abortResult::isCompleted)
        assertTrue(transactionManager.isReady)
        assertTrue(abortResult.isCompleted)
        assertTrue(abortResult.isSuccessful)
    }

    @Test
    fun shouldFailAbortIfAddOffsetsFailsWithFatalError() {
        doInitTransactions()
        transactionManager.beginTransaction()
        val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        offsets[tp1] = OffsetAndMetadata(1)
        transactionManager.sendOffsetsToTransaction(
            offsets = offsets,
            groupMetadata = ConsumerGroupMetadata(consumerGroupId)
        )
        val abortResult = transactionManager.beginAbort()
        prepareAddOffsetsToTxnResponse(
            error = Errors.UNKNOWN_SERVER_ERROR,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
        )
        runUntil(abortResult::isCompleted)
        assertFalse(abortResult.isSuccessful)
        assertTrue(transactionManager.hasFatalError)
    }

    @Test
    fun testSendOffsetsWithGroupMetadata() {
        val txnOffsetCommitResponse: MutableMap<TopicPartition, Errors> = HashMap()
        txnOffsetCommitResponse[tp0] = Errors.NONE
        txnOffsetCommitResponse[tp1] = Errors.COORDINATOR_LOAD_IN_PROGRESS
        val addOffsetsResult = prepareGroupMetadataCommit {
            prepareTxnOffsetCommitResponse(
                consumerGroupId = consumerGroupId,
                producerId = producerId,
                producerEpoch = epoch,
                groupInstanceId = groupInstanceId,
                memberId = memberId,
                generationId = generationId,
                txnOffsetCommitResponse = txnOffsetCommitResponse,
            )
        }
        sender.runOnce() // Send TxnOffsetCommitRequest request.
        assertTrue(transactionManager.hasPendingOffsetCommits) // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted) // We should only be done after both RPCs complete successfully.
        txnOffsetCommitResponse[tp1] = Errors.NONE
        prepareTxnOffsetCommitResponse(
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = epoch,
            groupInstanceId = groupInstanceId,
            memberId = memberId,
            generationId = generationId,
            txnOffsetCommitResponse = txnOffsetCommitResponse,
        )
        sender.runOnce() // Send TxnOffsetCommitRequest again.
        assertTrue(addOffsetsResult.isCompleted)
        assertTrue(addOffsetsResult.isSuccessful)
    }

    @Test
    fun testSendOffsetWithGroupMetadataFailAsAutoDowngradeTxnCommitNotEnabled() {
        client.setNodeApiVersions(
            NodeApiVersions.create(
                apiKey = ApiKeys.TXN_OFFSET_COMMIT.id,
                minVersion = 0,
                maxVersion = 2,
            )
        )
        val txnOffsetCommitResponse = mapOf(
            tp0 to Errors.NONE,
            tp1 to Errors.COORDINATOR_LOAD_IN_PROGRESS,
        )
        val addOffsetsResult = prepareGroupMetadataCommit {
            prepareTxnOffsetCommitResponse(
                consumerGroupId = consumerGroupId,
                producerId = producerId,
                producerEpoch = epoch,
                txnOffsetCommitResponse = txnOffsetCommitResponse
            )
        }
        sender.runOnce()
        assertTrue(addOffsetsResult.isCompleted)
        assertFalse(addOffsetsResult.isSuccessful)
        assertIs<UnsupportedVersionException>(addOffsetsResult.error())
        assertFatalError(UnsupportedVersionException::class.java)
    }

    private fun prepareGroupMetadataCommit(prepareTxnCommitResponse: Runnable): TransactionalRequestResult {
        doInitTransactions()
        transactionManager.beginTransaction()
        val offsets = mapOf(
            tp0 to OffsetAndMetadata(1),
            tp1 to OffsetAndMetadata(1),
        )
        val addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets = offsets,
            groupMetadata = ConsumerGroupMetadata(
                groupId = consumerGroupId,
                generationId = generationId,
                memberId = memberId,
                groupInstanceId = groupInstanceId,
            )
        )
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch)
        sender.runOnce() // send AddOffsetsToTxnResult
        assertFalse(addOffsetsResult.isCompleted) // The request should complete only after the TxnOffsetCommit completes
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.GROUP,
            coordinatorKey = consumerGroupId,
        )
        prepareTxnCommitResponse.run()
        assertNull(transactionManager.coordinator(CoordinatorType.GROUP))
        sender.runOnce() // try to send TxnOffsetCommitRequest, but find we don't have a group coordinator
        sender.runOnce() // send find coordinator for group request
        assertNotNull(transactionManager.coordinator(CoordinatorType.GROUP))
        assertTrue(transactionManager.hasPendingOffsetCommits)
        return addOffsetsResult
    }

    @Test
    @Throws(InterruptedException::class)
    fun testNoDrainWhenPartitionsPending() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        appendToAccumulator(tp0)
        transactionManager.maybeAddPartition(tp1)
        appendToAccumulator(tp1)
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        assertFalse(transactionManager.isSendToPartitionAllowed(tp1))
        val node1 = Node(0, "localhost", 1111)
        val node2 = Node(1, "localhost", 1112)
        val part1 = PartitionInfo(
            topic = topic,
            partition = 0,
            leader = node1,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val part2 = PartitionInfo(
            topic = topic,
            partition = 1,
            leader = node2,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val cluster = Cluster(
            clusterId = null,
            nodes = listOf(node1, node2),
            partitions = listOf(part1, part2),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val nodes = setOf(node1, node2)
        val drainedBatches = accumulator.drain(
            cluster = cluster,
            nodes = nodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id))
        assertTrue(drainedBatches[node1.id]!!.isEmpty())
        assertTrue(drainedBatches.containsKey(node2.id))
        assertTrue(drainedBatches[node2.id]!!.isEmpty())
        assertFalse(transactionManager.hasError)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAllowDrainInAbortableErrorState() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp1)
        prepareAddPartitionsToTxn(tp1, Errors.NONE)
        runUntil { transactionManager.transactionContainsPartition(tp1) }
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxn(tp0, Errors.TOPIC_AUTHORIZATION_FAILED)
        runUntil { transactionManager.hasAbortableError }
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1))

        // Try to drain a message destined for tp1, it should get drained.
        val node1 = Node(id = 1, host = "localhost", port = 1112)
        val part1 = PartitionInfo(
            topic = topic,
            partition = 1,
            leader = node1,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val cluster = Cluster(
            clusterId = null,
            nodes = listOf(node1),
            partitions = listOf(part1),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        appendToAccumulator(tp1)
        val drainedBatches = accumulator.drain(
            cluster = cluster,
            nodes = setOf(node1),
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        // We should drain the appended record since we are in abortable state and the partition has already been
        // added to the transaction.
        assertTrue(drainedBatches.containsKey(node1.id))
        assertEquals(1, drainedBatches[node1.id]!!.size)
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRaiseErrorWhenNoPartitionsPendingOnDrain() {
        doInitTransactions()
        transactionManager.beginTransaction()
        // Don't execute transactionManager.maybeAddPartitionToTransaction(tp0). This should result in an error on drain.
        appendToAccumulator(tp0)
        val node1 = Node(id = 0, host = "localhost", port = 1111)
        val part1 = PartitionInfo(
            topic = topic,
            partition = 0,
            leader = node1,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val cluster = Cluster(
            clusterId = null,
            nodes = listOf(node1),
            partitions = listOf(part1),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val nodes: MutableSet<Node> = HashSet()
        nodes.add(node1)
        val drainedBatches = accumulator.drain(
            cluster = cluster,
            nodes = nodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id))
        assertTrue(drainedBatches[node1.id]!!.isEmpty())
    }

    @Test
    @Throws(Exception::class)
    fun resendFailedProduceRequestAfterAbortableError() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.NOT_LEADER_OR_FOLLOWER, producerId, epoch)
        runUntil { !client.hasPendingResponses() }
        assertFalse(responseFuture.isDone)
        transactionManager.transitionToAbortableError(KafkaException())
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture.isDone }
        assertNotNull(responseFuture.get()) // should throw the exception which caused the transaction to be aborted.
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTransitionToAbortableErrorOnBatchExpiry() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)!!
        assertFalse(responseFuture.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        // Check that only addPartitions was sent.
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0))
        assertFalse(responseFuture.isDone)

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they
        // can't be drained.
        time.sleep(10000)
        // Disconnect the target node for the pending produce request. This will ensure that sender
        // will try to expire the batch.
        val clusterNode = metadata.fetch().nodes[0]
        client.disconnect(clusterNode.idString())
        client.backoff(clusterNode, 100)
        runUntil { responseFuture.isDone }
        val error = assertFailsWith<ExecutionException>(
            message = "Expected to get a TimeoutException since the queued ProducerBatch should have been expired",
        ){
            // make sure the produce was expired.
            responseFuture.get()
        }
        assertIs<TimeoutException>(error.cause)
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTransitionToAbortableErrorOnMultipleBatchExpiry() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        transactionManager.maybeAddPartition(tp1)
        val firstBatchResponse = appendToAccumulator(tp0)
        val secondBatchResponse = appendToAccumulator(tp1)
        assertFalse(firstBatchResponse!!.isDone)
        assertFalse(secondBatchResponse!!.isDone)
        val partitionErrors: MutableMap<TopicPartition, Errors> = HashMap()
        partitionErrors[tp0] = Errors.NONE
        partitionErrors[tp1] = Errors.NONE
        prepareAddPartitionsToTxn(partitionErrors)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        // Check that only addPartitions was sent.
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        assertTrue(transactionManager.transactionContainsPartition(tp1))
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1))
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1))
        assertFalse(firstBatchResponse.isDone)
        assertFalse(secondBatchResponse.isDone)

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they
        // can't be drained.
        time.sleep(10000)
        // Disconnect the target node for the pending produce request. This will ensure that sender
        // will try to expire the batch.
        val clusterNode = metadata.fetch().nodes[0]
        client.disconnect(clusterNode.idString())
        client.backoff(clusterNode, 100)
        runUntil { firstBatchResponse.isDone }
        runUntil { secondBatchResponse.isDone }
        try {
            // make sure the produce was expired.
            firstBatchResponse.get()
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired")
        } catch (e: ExecutionException) {
            assertIs<TimeoutException>(e.cause)
        }
        try {
            // make sure the produce was expired.
            secondBatchResponse.get()
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired")
        } catch (e: ExecutionException) {
            assertIs<TimeoutException>(e.cause)
        }
        assertTrue(transactionManager.hasAbortableError)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testDropCommitOnBatchExpiry() {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        // Check that only addPartitions was sent.
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0))
        assertFalse(responseFuture.isDone)
        val commitResult = transactionManager.beginCommit()

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they
        // can't be drained.
        time.sleep(10000)
        // Disconnect the target node for the pending produce request. This will ensure that sender
        // will try to expire the batch.
        val clusterNode = metadata.fetch().nodes[0]
        client.disconnect(clusterNode.idString())
        runUntil { responseFuture.isDone } // We should try to flush the produce, but expire it
        // instead without sending anything.
        val error = assertFailsWith<ExecutionException>(
            message = "Expected to get a TimeoutException since the queued ProducerBatch should have been expired",
        ) {
            // make sure the produce was expired.
            responseFuture.get()
        }
        assertIs<TimeoutException>(error.cause)
        runUntil(commitResult::isCompleted) // the commit shouldn't be completed without being sent since the produce request failed.
        assertFalse(commitResult.isSuccessful) // the commit shouldn't succeed since the produce request failed.
        assertFailsWith<TimeoutException> { commitResult.await() }
        assertTrue(transactionManager.hasAbortableError)
        assertTrue(transactionManager.hasOngoingTransaction)
        assertFalse(transactionManager.isCompleting)
        assertTrue(transactionManager.transactionContainsPartition(tp0))
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, (epoch + 1).toShort())
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        assertFalse(transactionManager.hasOngoingTransaction)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTransitionToFatalErrorWhenRetriedBatchIsExpired() {
        apiVersions.update(
            nodeId = "0",
            nodeApiVersions = NodeApiVersions.create(
                listOf(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                        .setMinVersion(0.toShort())
                        .setMaxVersion(1.toShort()),
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.PRODUCE.id)
                        .setMinVersion(0.toShort())
                        .setMaxVersion(7.toShort())
                )
            )
        )
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        assertFalse(transactionManager.transactionContainsPartition(tp0))
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0))
        // Check that only addPartitions was sent.
        runUntil { transactionManager.transactionContainsPartition(tp0) }
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0))
        prepareProduceResponse(Errors.NOT_LEADER_OR_FOLLOWER, producerId, epoch)
        runUntil { !client.hasPendingResponses() }
        assertFalse(responseFuture.isDone)
        val commitResult = transactionManager.beginCommit()

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they
        // can't be drained.
        time.sleep(10000)
        // Disconnect the target node for the pending produce request. This will ensure that sender
        // will try to expire the batch.
        val clusterNode = metadata.fetch().nodes[0]
        client.disconnect(clusterNode.idString())
        client.backoff(clusterNode, 100)
        runUntil { responseFuture.isDone } // We should try to flush the produce, but expire it instead without sending anything.

        val error = assertFailsWith<ExecutionException>(
            message = "Expected to get a TimeoutException since the queued ProducerBatch should have been expired",
        ) {
            // make sure the produce was expired.
            responseFuture.get()
        }
        assertIs<TimeoutException>(error.cause)

        runUntil(commitResult::isCompleted)
        assertFalse(commitResult.isSuccessful) // the commit should have been dropped.
        assertTrue(transactionManager.hasFatalError)
        assertFalse(transactionManager.hasOngoingTransaction)
    }

    @Test
    fun testBumpEpochAfterTimeoutWithoutPendingInflightRequests() {
        initializeTransactionManager(null)
        val producerId = 15L
        val epoch: Short = 5
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId, epoch)
        initializeIdempotentProducerId(producerId, epoch)

        // Nothing to resolve, so no reset is needed
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
        val tp0 = TopicPartition("foo", 0)
        assertEquals(0, transactionManager.sequenceNumber(tp0))
        val b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        assertEquals(1, transactionManager.sequenceNumber(tp0))
        transactionManager.handleCompletedBatch(
            batch = b1,
            response = ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 500,
                logAppendTime = time.milliseconds(),
                logStartOffset = 0,
            ),
        )
        assertEquals(0, transactionManager.lastAckedSequence(tp0))

        // Marking sequence numbers unresolved without inflight requests is basically a no-op.
        transactionManager.markSequenceUnresolved(b1)
        transactionManager.maybeResolveSequences()
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
        assertFalse(transactionManager.hasUnresolvedSequences())

        // We have a new batch which fails with a timeout
        val b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        assertEquals(2, transactionManager.sequenceNumber(tp0))
        transactionManager.markSequenceUnresolved(b2)
        transactionManager.handleFailedBatch(b2, TimeoutException(), false)
        assertTrue(transactionManager.hasUnresolvedSequences())

        // We only had one inflight batch, so we should be able to clear the unresolved status
        // and bump the epoch
        transactionManager.maybeResolveSequences()
        assertFalse(transactionManager.hasUnresolvedSequences())

        // Run sender loop to trigger epoch bump
        runUntil { transactionManager.producerIdAndEpoch.epoch.toInt() == 6 }
    }

    @Test
    fun testNoProducerIdResetAfterLastInFlightBatchSucceeds() {
        initializeTransactionManager(null)
        val producerId = 15L
        val epoch: Short = 5
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId, epoch)
        initializeIdempotentProducerId(producerId, epoch)
        val tp0 = TopicPartition("foo", 0)
        val b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        val b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        val b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3")
        assertEquals(3, transactionManager.sequenceNumber(tp0))

        // The first batch fails with a timeout
        transactionManager.markSequenceUnresolved(b1)
        transactionManager.handleFailedBatch(b1, TimeoutException(), false)
        assertTrue(transactionManager.hasUnresolvedSequences())

        // The reset should not occur until sequence numbers have been resolved
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
        assertTrue(transactionManager.hasUnresolvedSequences())

        // The second batch fails as well with a timeout
        transactionManager.handleFailedBatch(b2, TimeoutException(), false)
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
        assertTrue(transactionManager.hasUnresolvedSequences())

        // The third batch succeeds, which should resolve the sequence number without
        // requiring a producerId reset.
        transactionManager.handleCompletedBatch(
            batch = b3,
            response = ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 500,
                logAppendTime = time.milliseconds(),
                logStartOffset = 0,
            ),
        )
        transactionManager.maybeResolveSequences()
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
        assertFalse(transactionManager.hasUnresolvedSequences())
        assertEquals(3, transactionManager.sequenceNumber(tp0))
    }

    @Test
    fun testEpochBumpAfterLastInflightBatchFails() {
        initializeTransactionManager(null)
        val producerIdAndEpoch = ProducerIdAndEpoch(producerId, epoch)
        initializeIdempotentProducerId(producerId, epoch)
        val tp0 = TopicPartition("foo", 0)
        val b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        val b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        val b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3")
        assertEquals(3, transactionManager.sequenceNumber(tp0))

        // The first batch fails with a timeout
        transactionManager.markSequenceUnresolved(b1)
        transactionManager.handleFailedBatch(b1, TimeoutException(), false)
        assertTrue(transactionManager.hasUnresolvedSequences())

        // The second batch succeeds, but sequence numbers are still not resolved
        transactionManager.handleCompletedBatch(
            batch = b2,
            response = ProduceResponse.PartitionResponse(
                error = Errors.NONE,
                baseOffset = 500,
                logAppendTime = time.milliseconds(),
                logStartOffset = 0,
            ),
        )
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch)
        assertTrue(transactionManager.hasUnresolvedSequences())

        // When the last inflight batch fails, we have to bump the epoch
        transactionManager.handleFailedBatch(b3, TimeoutException(), false)

        // Run sender loop to trigger epoch bump
        runUntil { transactionManager.producerIdAndEpoch.epoch.toInt() == 2 }
        assertFalse(transactionManager.hasUnresolvedSequences())
        assertEquals(0, transactionManager.sequenceNumber(tp0))
    }

    @Test
    fun testNoFailedBatchHandlingWhenTxnManagerIsInFatalError() {
        initializeTransactionManager(null)
        val producerId = 15L
        val epoch: Short = 5
        initializeIdempotentProducerId(producerId, epoch)
        val tp0 = TopicPartition("foo", 0)
        val b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        // Handling b1 should bump the epoch after OutOfOrderSequenceException
        transactionManager.handleFailedBatch(
            batch = b1,
            exception = OutOfOrderSequenceException("out of sequence"),
            adjustSequenceNumbers = false,
        )
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        val idAndEpochAfterFirstBatch = ProducerIdAndEpoch(producerId, (epoch + 1).toShort())
        assertEquals(
            expected = idAndEpochAfterFirstBatch,
            actual = transactionManager.producerIdAndEpoch,
        )
        transactionManager.transitionToFatalError(KafkaException())

        // The second batch should not bump the epoch as txn manager is already in fatal error state
        val b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        transactionManager.handleFailedBatch(b2, TimeoutException(), true)
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
        assertEquals(
            expected = idAndEpochAfterFirstBatch,
            actual = transactionManager.producerIdAndEpoch,
        )
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAbortTransactionAndReuseSequenceNumberOnError() {
        apiVersions.update(
            nodeId = "0",
            nodeApiVersions = NodeApiVersions.create(
                listOf(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                        .setMinVersion(0)
                        .setMaxVersion(1),
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.PRODUCE.id)
                        .setMinVersion(0)
                        .setMaxVersion(7),
                )
            )
        )
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture0 = appendToAccumulator(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { transactionManager.isPartitionAdded(tp0) } // Send AddPartitionsRequest
        runUntil { responseFuture0!!.isDone }
        val responseFuture1 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture1!!.isDone }
        val responseFuture2 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.TOPIC_AUTHORIZATION_FAILED, producerId, epoch)
        runUntil { responseFuture2!!.isDone } // Receive abortable error
        assertTrue(transactionManager.hasAbortableError)
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        abortResult.await()
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) } // Send AddPartitionsRequest
        assertEquals(2, transactionManager.sequenceNumber(tp0))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testAbortTransactionAndResetSequenceNumberOnUnknownProducerId() {
        // Set the InitProducerId version such that bumping the epoch number is not supported. This will test the case
        // where the sequence number is reset on an UnknownProducerId error, allowing subsequent transactions to
        // append to the log successfully
        apiVersions.update(
            nodeId = "0",
            nodeApiVersions = NodeApiVersions.create(
                listOf(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                        .setMinVersion(0)
                        .setMaxVersion(1),
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.PRODUCE.id)
                        .setMinVersion(0)
                        .setMaxVersion(7),
                )
            )
        )
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp1)
        val successPartitionResponseFuture = appendToAccumulator(tp1)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp1, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch, tp1)
        runUntil { successPartitionResponseFuture!!.isDone }
        assertTrue(transactionManager.isPartitionAdded(tp1))
        transactionManager.maybeAddPartition(tp0)
        val responseFuture0 = appendToAccumulator(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture0!!.isDone }
        assertTrue(transactionManager.isPartitionAdded(tp0))
        val responseFuture1 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { responseFuture1!!.isDone }
        val responseFuture2 = appendToAccumulator(tp0)
        client.prepareResponse(
            matcher = produceRequestMatcher(producerId, epoch, tp0),
            response = produceResponse(
                tp = tp0,
                offset = 0,
                error = Errors.UNKNOWN_PRODUCER_ID,
                throttleTimeMs = 0,
                logStartOffset = 0,
            )
        )
        runUntil { responseFuture2!!.isDone }
        assertTrue(transactionManager.hasAbortableError)
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch)
        runUntil(abortResult::isCompleted)
        assertTrue(abortResult.isSuccessful)
        abortResult.await()
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertEquals(0, transactionManager.sequenceNumber(tp0))
        assertEquals(1, transactionManager.sequenceNumber(tp1))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testBumpTransactionalEpochOnAbortableError() {
        val initialEpoch: Short = 1
        val bumpedEpoch = (initialEpoch + 1).toShort()
        doInitTransactions(producerId, initialEpoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        val responseFuture0 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture0!!.isDone }
        val responseFuture1 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture1!!.isDone }
        val responseFuture2 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.TOPIC_AUTHORIZATION_FAILED, producerId, initialEpoch)
        runUntil { responseFuture2!!.isDone }
        assertTrue(transactionManager.hasAbortableError)
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch)
        runUntil { transactionManager.producerIdAndEpoch.epoch == bumpedEpoch }
        assertTrue(abortResult.isCompleted)
        assertTrue(abortResult.isSuccessful)
        abortResult.await()
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, bumpedEpoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertEquals(0, transactionManager.sequenceNumber(tp0))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testBumpTransactionalEpochOnUnknownProducerIdError() {
        val initialEpoch: Short = 1
        val bumpedEpoch: Short = 2
        doInitTransactions(producerId, initialEpoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        val responseFuture0 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture0!!.isDone }
        val responseFuture1 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture1!!.isDone }
        val responseFuture2 = appendToAccumulator(tp0)
        client.prepareResponse(
            matcher = produceRequestMatcher(producerId, initialEpoch, tp0),
            response = produceResponse(
                tp = tp0,
                offset = 0,
                error = Errors.UNKNOWN_PRODUCER_ID,
                throttleTimeMs = 0,
                logStartOffset = 0,
            )
        )
        runUntil { responseFuture2!!.isDone }
        assertTrue(transactionManager.hasAbortableError)
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch)
        runUntil { transactionManager.producerIdAndEpoch.epoch == bumpedEpoch }
        assertTrue(abortResult.isCompleted)
        assertTrue(abortResult.isSuccessful)
        abortResult.await()
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, bumpedEpoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertEquals(0, transactionManager.sequenceNumber(tp0))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testBumpTransactionalEpochOnTimeout() {
        val initialEpoch: Short = 1
        val bumpedEpoch: Short = 2
        doInitTransactions(producerId, initialEpoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        val responseFuture0 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture0!!.isDone }
        val responseFuture1 = appendToAccumulator(tp0)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture1!!.isDone }
        val responseFuture2 = appendToAccumulator(tp0)
        runUntil { client.hasInFlightRequests() } // Send Produce Request

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they
        // can't be drained.
        time.sleep(10000)
        // Disconnect the target node for the pending produce request. This will ensure that sender
        // will try to expire the batch.
        val clusterNode = metadata.fetch().nodes[0]
        client.disconnect(clusterNode.idString())
        client.backoff(clusterNode, 100)
        runUntil { responseFuture2!!.isDone } // We should try to flush the produce, but expire it instead without sending anything.
        assertTrue(transactionManager.hasAbortableError)
        val abortResult = transactionManager.beginAbort()
        sender.runOnce() // handle the abort
        time.sleep(110) // Sleep to make sure the node backoff period has passed
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch)
        runUntil { transactionManager.producerIdAndEpoch.epoch == bumpedEpoch }
        assertTrue(abortResult.isCompleted)
        assertTrue(abortResult.isSuccessful)
        abortResult.await()
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, bumpedEpoch, producerId)
        runUntil { transactionManager.isPartitionAdded(tp0) }
        assertEquals(0, transactionManager.sequenceNumber(tp0))
    }

    @Test
    fun testBumpTransactionalEpochOnRecoverableAddPartitionRequestError() {
        val initialEpoch: Short = 1
        val bumpedEpoch: Short = 2
        doInitTransactions(producerId, initialEpoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        prepareAddPartitionsToTxnResponse(
            error = Errors.INVALID_PRODUCER_ID_MAPPING,
            topicPartition = tp0,
            epoch = initialEpoch,
            producerId = producerId,
        )
        runUntil { transactionManager.hasAbortableError }
        val abortResult = transactionManager.beginAbort()
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch)
        runUntil(abortResult::isCompleted)
        assertEquals(bumpedEpoch, transactionManager.producerIdAndEpoch.epoch)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
    }

    @Test
    @Throws(InterruptedException::class)
    fun testBumpTransactionalEpochOnRecoverableAddOffsetsRequestError() {
        val initialEpoch: Short = 1
        val bumpedEpoch: Short = 2
        doInitTransactions(producerId, initialEpoch)
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        val responseFuture = appendToAccumulator(tp0)
        assertFalse(responseFuture!!.isDone)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch)
        runUntil { responseFuture.isDone }
        val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        offsets[tp0] = OffsetAndMetadata(1)
        transactionManager.sendOffsetsToTransaction(
            offsets = offsets,
            groupMetadata = ConsumerGroupMetadata(consumerGroupId),
        )
        assertFalse(transactionManager.hasPendingOffsetCommits)
        prepareAddOffsetsToTxnResponse(
            error = Errors.INVALID_PRODUCER_ID_MAPPING,
            consumerGroupId = consumerGroupId,
            producerId = producerId,
            producerEpoch = initialEpoch,
        )
        runUntil { transactionManager.hasAbortableError } // Send AddOffsetsRequest
        val abortResult = transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch)
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch)
        runUntil(abortResult::isCompleted)
        assertEquals(bumpedEpoch, transactionManager.producerIdAndEpoch.epoch)
        assertTrue(abortResult.isSuccessful)
        assertTrue(transactionManager.isReady) // make sure we are ready for a transaction now.
    }

    @Test
    @Throws(InterruptedException::class)
    fun testHealthyPartitionRetriesDuringEpochBump() {
        // Use a custom Sender to allow multiple inflight requests
        initializeTransactionManager(null)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = false,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = MAX_RETRIES,
            metricsRegistry = SenderMetricsRegistry(Metrics(time = time)),
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = 50,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        initializeIdempotentProducerId(producerId, epoch)
        val tp0b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        val tp0b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        writeIdempotentBatchWithValue(transactionManager, tp0, "3")
        val tp1b1 = writeIdempotentBatchWithValue(transactionManager, tp1, "4")
        val tp1b2 = writeIdempotentBatchWithValue(transactionManager, tp1, "5")
        assertEquals(3, transactionManager.sequenceNumber(tp0))
        assertEquals(2, transactionManager.sequenceNumber(tp1))

        // First batch of each partition succeeds
        val b1AppendTime = time.milliseconds()
        val t0b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500L,
            logAppendTime = b1AppendTime,
            logStartOffset = 0L
        )
        tp0b1.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp0b1, t0b1Response)
        val t1b1Response = ProduceResponse.PartitionResponse(
            Errors.NONE, 500L, b1AppendTime, 0L
        )
        tp1b1.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp1b1, t1b1Response)

        // We bump the epoch and set sequence numbers back to 0
        val t0b2Response = ProduceResponse.PartitionResponse(
            error = Errors.UNKNOWN_PRODUCER_ID,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 500,
        )
        assertTrue(transactionManager.canRetry(t0b2Response, tp0b2))

        // Run sender loop to trigger epoch bump
        runUntil { transactionManager.producerIdAndEpoch.epoch.toInt() == 2 }

        // tp0 batches should have had sequence and epoch rewritten, but tp1 batches should not
        assertEquals(tp0b2, transactionManager.nextBatchBySequence(tp0))
        assertEquals(0, transactionManager.firstInFlightSequence(tp0))
        assertEquals(0, tp0b2.baseSequence)
        assertTrue(tp0b2.sequenceHasBeenReset)
        assertEquals(2, tp0b2.producerEpoch.toInt())
        assertEquals(tp1b2, transactionManager.nextBatchBySequence(tp1))
        assertEquals(1, transactionManager.firstInFlightSequence(tp1))
        assertEquals(1, tp1b2.baseSequence)
        assertFalse(tp1b2.sequenceHasBeenReset)
        assertEquals(1, tp1b2.producerEpoch.toInt())

        // New tp1 batches should not be drained from the accumulator while tp1 has in-flight
        // requests using the old epoch
        appendToAccumulator(tp1)
        sender.runOnce()
        assertEquals(1, accumulator.getDeque(tp1)!!.size)

        // Partition failover occurs and tp1 returns a NOT_LEADER_OR_FOLLOWER error
        // Despite having the old epoch, the batch should retry
        var t1b2Response = ProduceResponse.PartitionResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, -1, -1, 600L
        )
        assertTrue(transactionManager.canRetry(t1b2Response, tp1b2))
        accumulator.reenqueue(tp1b2, time.milliseconds())

        // The batch with the old epoch should be successfully drained, leaving the new one in the queue
        sender.runOnce()
        assertEquals(1, accumulator.getDeque(tp1)!!.size)
        assertNotEquals(tp1b2, accumulator.getDeque(tp1)!!.peek())
        assertEquals(epoch, tp1b2.producerEpoch)

        // After successfully retrying, there should be no in-flight batches for tp1 and the
        // sequence should be 0
        t1b2Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        tp1b2.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp1b2, t1b2Response)
        transactionManager.maybeUpdateProducerIdAndEpoch(tp1)
        assertFalse(transactionManager.hasInflightBatches(tp1))
        assertEquals(0, transactionManager.sequenceNumber(tp1))

        // The last batch should now be drained and sent
        runUntil { transactionManager.hasInflightBatches(tp1) }
        assertTrue(accumulator.getDeque(tp1)!!.isEmpty())
        val tp1b3 = transactionManager.nextBatchBySequence(tp1)
        assertEquals(epoch + 1, tp1b3!!.producerEpoch.toInt())
        val t1b3Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        tp1b3.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp1b3, t1b3Response)
        transactionManager.maybeUpdateProducerIdAndEpoch(tp1)
        assertFalse(transactionManager.hasInflightBatches(tp1))
        assertEquals(1, transactionManager.sequenceNumber(tp1))
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRetryAbortTransaction() {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.ABORT, TransactionResult.ABORT)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRetryCommitTransaction() {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.COMMIT, TransactionResult.COMMIT)
    }

    @Test
    fun testRetryAbortTransactionAfterCommitTimeout() {
        assertFailsWith<IllegalStateException> {
            verifyCommitOrAbortTransactionRetriable(
                TransactionResult.COMMIT,
                TransactionResult.ABORT,
            )
        }
    }

    @Test
    fun testRetryCommitTransactionAfterAbortTimeout() {
        assertFailsWith<IllegalStateException> {
            verifyCommitOrAbortTransactionRetriable(
                TransactionResult.ABORT,
                TransactionResult.COMMIT,
            )
        }
    }

    @Test
    fun testCanBumpEpochDuringCoordinatorDisconnect() {
        doInitTransactions(0, 0.toShort())
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertTrue(transactionManager.canBumpEpoch)
        apiVersions.remove(transactionManager.coordinator(CoordinatorType.TRANSACTION)!!.idString())
        assertTrue(transactionManager.canBumpEpoch)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testFailedInflightBatchAfterEpochBump() {
        // Use a custom Sender to allow multiple inflight requests
        initializeTransactionManager(null)
        val sender = Sender(
            logContext = logContext,
            client = client,
            metadata = metadata,
            accumulator = accumulator,
            guaranteeMessageOrder = false,
            maxRequestSize = MAX_REQUEST_SIZE,
            acks = ACKS_ALL,
            retries = MAX_RETRIES,
            metricsRegistry = SenderMetricsRegistry(Metrics(time = time)),
            time = time,
            requestTimeoutMs = REQUEST_TIMEOUT,
            retryBackoffMs = 50,
            transactionManager = transactionManager,
            apiVersions = apiVersions,
        )
        initializeIdempotentProducerId(producerId, epoch)
        val tp0b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1")
        val tp0b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2")
        writeIdempotentBatchWithValue(transactionManager, tp0, "3")
        val tp1b1 = writeIdempotentBatchWithValue(transactionManager, tp1, "4")
        val tp1b2 = writeIdempotentBatchWithValue(transactionManager, tp1, "5")
        assertEquals(3, transactionManager.sequenceNumber(tp0))
        assertEquals(2, transactionManager.sequenceNumber(tp1))

        // First batch of each partition succeeds
        val b1AppendTime = time.milliseconds()
        val t0b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        tp0b1.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp0b1, t0b1Response)
        val t1b1Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        tp1b1.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp1b1, t1b1Response)

        // We bump the epoch and set sequence numbers back to 0
        val t0b2Response = ProduceResponse.PartitionResponse(
            error = Errors.UNKNOWN_PRODUCER_ID,
            baseOffset = -1,
            logAppendTime = -1,
            logStartOffset = 500,
        )
        assertTrue(transactionManager.canRetry(t0b2Response, tp0b2))

        // Run sender loop to trigger epoch bump
        runUntil { transactionManager.producerIdAndEpoch.epoch.toInt() == 2 }

        // tp0 batches should have had sequence and epoch rewritten, but tp1 batches should not
        assertEquals(tp0b2, transactionManager.nextBatchBySequence(tp0))
        assertEquals(0, transactionManager.firstInFlightSequence(tp0))
        assertEquals(0, tp0b2.baseSequence)
        assertTrue(tp0b2.sequenceHasBeenReset)
        assertEquals(2, tp0b2.producerEpoch.toInt())
        assertEquals(tp1b2, transactionManager.nextBatchBySequence(tp1))
        assertEquals(1, transactionManager.firstInFlightSequence(tp1))
        assertEquals(1, tp1b2.baseSequence)
        assertFalse(tp1b2.sequenceHasBeenReset)
        assertEquals(1, tp1b2.producerEpoch.toInt())

        // New tp1 batches should not be drained from the accumulator while tp1 has in-flight requests using the old epoch
        appendToAccumulator(tp1)
        sender.runOnce()
        assertEquals(1, accumulator.getDeque(tp1)!!.size)

        // Partition failover occurs and tp1 returns a NOT_LEADER_OR_FOLLOWER error
        // Despite having the old epoch, the batch should retry
        var t1b2Response = ProduceResponse.PartitionResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, -1, -1, 600L
        )
        assertTrue(transactionManager.canRetry(t1b2Response, tp1b2))
        accumulator.reenqueue(tp1b2, time.milliseconds())

        // The batch with the old epoch should be successfully drained, leaving the new one in the queue
        sender.runOnce()
        assertEquals(1, accumulator.getDeque(tp1)!!.size)
        assertNotEquals(tp1b2, accumulator.getDeque(tp1)!!.peek())
        assertEquals(epoch, tp1b2.producerEpoch)

        // After successfully retrying, there should be no in-flight batches for tp1 and the sequence should be 0
        t1b2Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        tp1b2.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp1b2, t1b2Response)
        transactionManager.maybeUpdateProducerIdAndEpoch(tp1)
        assertFalse(transactionManager.hasInflightBatches(tp1))
        assertEquals(0, transactionManager.sequenceNumber(tp1))

        // The last batch should now be drained and sent
        runUntil { transactionManager.hasInflightBatches(tp1) }
        assertTrue(accumulator.getDeque(tp1)!!.isEmpty())
        val tp1b3 = transactionManager.nextBatchBySequence(tp1)
        assertEquals(epoch + 1, tp1b3!!.producerEpoch.toInt())
        val t1b3Response = ProduceResponse.PartitionResponse(
            error = Errors.NONE,
            baseOffset = 500,
            logAppendTime = b1AppendTime,
            logStartOffset = 0,
        )
        tp1b3.complete(500L, b1AppendTime)
        transactionManager.handleCompletedBatch(tp1b3, t1b3Response)
        assertFalse(transactionManager.hasInflightBatches(tp1))
        assertEquals(1, transactionManager.sequenceNumber(tp1))
    }

    @Test
    fun testBackgroundInvalidStateTransitionIsFatal() {
        doInitTransactions()
        assertTrue(transactionManager.isTransactional)

        transactionManager.setPoisonStateOnInvalidTransition(true)

        // Intentionally perform an operation that will cause an invalid state transition. The detection of this
        // will result in a poisoning of the transaction manager for all subsequent transactional operations since
        // it was performed in the background.
        assertFailsWith<IllegalStateException> {
            transactionManager.handleFailedBatch(batchWithValue(tp0, "test"), KafkaException(), false)
        }
        assertTrue(transactionManager.hasFatalError)

        // Validate that all of these operations will fail after the invalid state transition attempt above.
        assertFailsWith<IllegalStateException> { transactionManager.beginTransaction() }
        assertFailsWith<IllegalStateException> { transactionManager.beginAbort() }
        assertFailsWith<IllegalStateException> { transactionManager.beginCommit() }
        assertFailsWith<IllegalStateException> { transactionManager.maybeAddPartition(tp0) }
        assertFailsWith<IllegalStateException> { transactionManager.initializeTransactions() }
        assertFailsWith<IllegalStateException> {
            transactionManager.sendOffsetsToTransaction(emptyMap(), ConsumerGroupMetadata("fake-group-id"))
        }
    }

    @Test
    fun testForegroundInvalidStateTransitionIsRecoverable() {
        // Intentionally perform an operation that will cause an invalid state transition. The detection of this
        // will not poison the transaction manager since it was performed in the foreground.
        assertFailsWith<IllegalStateException> { transactionManager.beginAbort() }
        assertFalse(transactionManager.hasFatalError)

        // Validate that the transactions can still run after the invalid state transition attempt above.
        doInitTransactions()
        assertTrue(transactionManager.isTransactional)

        transactionManager.beginTransaction()
        assertFalse(transactionManager.hasFatalError)

        transactionManager.maybeAddPartition(tp1)
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareAddPartitionsToTxn(tp1, Errors.NONE)
        runUntil { transactionManager.isPartitionAdded(tp1) }

        val retryResult = transactionManager.beginCommit()
        assertTrue(transactionManager.hasOngoingTransaction)

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch)
        runUntil { !transactionManager.hasOngoingTransaction }
        runUntil(retryResult::isCompleted)
        retryResult.await()
        runUntil(retryResult::isAcked)
        assertFalse(transactionManager.hasOngoingTransaction)
    }

    @Throws(InterruptedException::class)
    private fun appendToAccumulator(tp: TopicPartition): FutureRecordMetadata? {
        val nowMs = time.milliseconds()
        return accumulator.append(
            topic = tp.topic,
            partition = tp.partition,
            timestamp = nowMs,
            key = "key".toByteArray(),
            value = "value".toByteArray(),
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = MAX_BLOCK_TIMEOUT.toLong(),
            abortOnNewBatch = false,
            nowMs = nowMs,
            cluster = singletonCluster(),
        ).future
    }

    @Throws(InterruptedException::class)
    private fun verifyCommitOrAbortTransactionRetriable(
        firstTransactionResult: TransactionResult,
        retryTransactionResult: TransactionResult,
    ) {
        doInitTransactions()
        transactionManager.beginTransaction()
        transactionManager.maybeAddPartition(tp0)
        appendToAccumulator(tp0)
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId)
        prepareProduceResponse(Errors.NONE, producerId, epoch)
        runUntil { !client.hasPendingResponses() }
        val result =
            if (firstTransactionResult === TransactionResult.COMMIT) transactionManager.beginCommit()
            else transactionManager.beginAbort()
        prepareEndTxnResponse(Errors.NONE, firstTransactionResult, producerId, epoch, true)
        runUntil { !client.hasPendingResponses() }
        assertFalse(result.isCompleted)
        assertFailsWith<TimeoutException> {
            result.await(timeout = MAX_BLOCK_TIMEOUT.toLong(), unit = TimeUnit.MILLISECONDS)
        }
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { !client.hasPendingResponses() }
        val retryResult =
            if (retryTransactionResult === TransactionResult.COMMIT) transactionManager.beginCommit()
            else transactionManager.beginAbort()
        assertEquals(retryResult, result) // check if cached result is reused.
        prepareEndTxnResponse(Errors.NONE, retryTransactionResult, producerId, epoch, false)
        runUntil(retryResult::isCompleted)
        assertFalse(transactionManager.hasOngoingTransaction)
    }

    private fun prepareAddPartitionsToTxn(errors: Map<TopicPartition, Errors>) {
        val result = AddPartitionsToTxnResponse.resultForTransaction(
            transactionalId = AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID,
            errors = errors,
        )
        val data = AddPartitionsToTxnResponseData()
            .setResultsByTopicV3AndBelow(result.topicResults)
            .setThrottleTimeMs(0)

        client.prepareResponse(
            matcher = { body ->
                val request = body as AddPartitionsToTxnRequest
                assertEquals(errors.keys.toSet(), getPartitionsFromV3Request(request).toSet())
                true
            },
            response = AddPartitionsToTxnResponse(data),
        )
    }

    private fun prepareAddPartitionsToTxn(tp: TopicPartition, error: Errors) {
        prepareAddPartitionsToTxn(mapOf(tp to error))
    }

    private fun prepareFindCoordinatorResponse(
        error: Errors, shouldDisconnect: Boolean,
        coordinatorType: CoordinatorType,
        coordinatorKey: String,
    ) {
        client.prepareResponse(
            matcher = { body ->
                val findCoordinatorRequest = (body as FindCoordinatorRequest).data()

                assertEquals(
                    coordinatorType,
                    CoordinatorType.forId(findCoordinatorRequest.keyType)
                )

                val key =
                    if (findCoordinatorRequest.coordinatorKeys.isEmpty()) findCoordinatorRequest.key
                    else findCoordinatorRequest.coordinatorKeys[0]

                assertEquals(coordinatorKey, key)
                true
            },
            response = FindCoordinatorResponse.prepareResponse(error, coordinatorKey, brokerNode),
            disconnected = shouldDisconnect,
        )
    }

    private fun prepareInitPidResponse(
        error: Errors,
        shouldDisconnect: Boolean,
        producerId: Long,
        producerEpoch: Short,
    ) {
        val responseData = InitProducerIdResponseData()
            .setErrorCode(error.code)
            .setProducerEpoch(producerEpoch)
            .setProducerId(producerId)
            .setThrottleTimeMs(0)
        client.prepareResponse(
            matcher = { body ->
                val initProducerIdRequest = (body as InitProducerIdRequest).data()
                assertEquals(transactionalId, initProducerIdRequest.transactionalId)
                assertEquals(transactionTimeoutMs, initProducerIdRequest.transactionTimeoutMs)
                true
            },
            response = InitProducerIdResponse(responseData),
            disconnected = shouldDisconnect,
        )
    }

    private fun sendProduceResponse(
        error: Errors,
        producerId: Long,
        producerEpoch: Short,
        tp: TopicPartition = tp0,
    ) {
        client.respond(
            matcher = produceRequestMatcher(producerId, producerEpoch, tp),
            response = produceResponse(tp = tp, offset = 0, error = error, throttleTimeMs = 0),
        )
    }

    private fun prepareProduceResponse(
        error: Errors,
        producerId: Long,
        producerEpoch: Short,
        tp: TopicPartition = tp0,
    ) {
        client.prepareResponse(
            matcher = produceRequestMatcher(producerId, producerEpoch, tp),
            response = produceResponse(tp = tp, offset = 0, error = error, throttleTimeMs = 0),
        )
    }

    private fun produceRequestMatcher(
        producerId: Long,
        epoch: Short,
        tp: TopicPartition,
    ): RequestMatcher {
        return RequestMatcher { body ->
            val produceRequest = body as ProduceRequest
            val records = produceRequest.data().topicData
                .find { it.name == tp.topic }!!
                .partitionData
                .filter { it.index == tp.partition }
                .firstNotNullOf { it.records as? MemoryRecords }
            assertNotNull(records)
            val batchIterator = records.batches().iterator()
            assertTrue(batchIterator.hasNext())
            val batch = batchIterator.next()
            assertFalse(batchIterator.hasNext())
            assertTrue(batch.isTransactional)
            assertEquals(producerId, batch.producerId())
            assertEquals(epoch, batch.producerEpoch())
            assertEquals(transactionalId, produceRequest.transactionalId)
            true
        }
    }

    @Suppress("SameParameterValue")
    private fun prepareAddPartitionsToTxnResponse(
        error: Errors,
        topicPartition: TopicPartition,
        epoch: Short,
        producerId: Long,
    ) {
        val result = AddPartitionsToTxnResponse.resultForTransaction(
            transactionalId = AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID,
            errors = Collections.singletonMap(topicPartition, error),
        )

        client.prepareResponse(
            matcher = addPartitionsRequestMatcher(topicPartition, epoch, producerId),
            response = AddPartitionsToTxnResponse(
                AddPartitionsToTxnResponseData()
                    .setThrottleTimeMs(0)
                    .setResultsByTopicV3AndBelow(result.topicResults),
            )
        )
    }

    @Suppress("SameParameterValue")
    private fun sendAddPartitionsToTxnResponse(
        error: Errors,
        topicPartition: TopicPartition,
        epoch: Short,
        producerId: Long,
    ) {
        val result = AddPartitionsToTxnResponse.resultForTransaction(
            transactionalId = AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID,
            errors = Collections.singletonMap(topicPartition, error),
        )
        client.respond(
            matcher = addPartitionsRequestMatcher(topicPartition, epoch, producerId),
            response = AddPartitionsToTxnResponse(
                AddPartitionsToTxnResponseData()
                    .setThrottleTimeMs(0)
                    .setResultsByTopicV3AndBelow(result.topicResults)
            ),
        )
    }

    private fun addPartitionsRequestMatcher(
        topicPartition: TopicPartition,
        epoch: Short,
        producerId: Long,
    ): RequestMatcher {
        return RequestMatcher { body ->
            val addPartitionsToTxnRequest = body as AddPartitionsToTxnRequest
            assertEquals(producerId, addPartitionsToTxnRequest.data().v3AndBelowProducerId)
            assertEquals(epoch, addPartitionsToTxnRequest.data().v3AndBelowProducerEpoch)
            assertEquals(listOf(topicPartition), getPartitionsFromV3Request(addPartitionsToTxnRequest))
            assertEquals(transactionalId, addPartitionsToTxnRequest.data().v3AndBelowTransactionalId)
            true
        }
    }

    private fun getPartitionsFromV3Request(request: AddPartitionsToTxnRequest): List<TopicPartition> {
        return AddPartitionsToTxnRequest.getPartitions(request.data().v3AndBelowTopics)
    }

    private fun prepareEndTxnResponse(
        error: Errors,
        result: TransactionResult,
        producerId: Long,
        epoch: Short,
        shouldDisconnect: Boolean = false,
    ) {
        client.prepareResponse(
            matcher = endTxnMatcher(result, producerId, epoch),
            response = EndTxnResponse(
                EndTxnResponseData()
                    .setErrorCode(error.code)
                    .setThrottleTimeMs(0)
            ),
            disconnected = shouldDisconnect,
        )
    }

    @Suppress("SameParameterValue")
    private fun sendEndTxnResponse(
        error: Errors,
        result: TransactionResult,
        producerId: Long,
        epoch: Short,
    ) {
        client.respond(
            endTxnMatcher(result, producerId, epoch), EndTxnResponse(
                EndTxnResponseData()
                    .setErrorCode(error.code)
                    .setThrottleTimeMs(0)
            )
        )
    }

    private fun endTxnMatcher(
        result: TransactionResult,
        producerId: Long,
        epoch: Short,
    ): RequestMatcher {
        return RequestMatcher { body ->
            val endTxnRequest = body as EndTxnRequest
            assertEquals(transactionalId, endTxnRequest.data().transactionalId)
            assertEquals(producerId, endTxnRequest.data().producerId)
            assertEquals(epoch, endTxnRequest.data().producerEpoch)
            assertEquals(result, endTxnRequest.result)
            true
        }
    }

    @Suppress("SameParameterValue")
    private fun prepareAddOffsetsToTxnResponse(
        error: Errors,
        consumerGroupId: String,
        producerId: Long,
        producerEpoch: Short,
    ) {
        client.prepareResponse(
            matcher = { body ->
                val addOffsetsToTxnRequest = (body as AddOffsetsToTxnRequest).data()
                assertEquals(consumerGroupId, addOffsetsToTxnRequest.groupId)
                assertEquals(transactionalId, addOffsetsToTxnRequest.transactionalId)
                assertEquals(producerId, addOffsetsToTxnRequest.producerId)
                assertEquals(producerEpoch, addOffsetsToTxnRequest.producerEpoch)
                true
            },
            response = AddOffsetsToTxnResponse(
                AddOffsetsToTxnResponseData().setErrorCode(error.code)
            ),
        )
    }

    @Suppress("SameParameterValue")
    private fun prepareTxnOffsetCommitResponse(
        consumerGroupId: String,
        producerId: Long,
        producerEpoch: Short,
        txnOffsetCommitResponse: Map<TopicPartition, Errors>,
    ) {
        client.prepareResponse(
            matcher = { request ->
                val txnOffsetCommitRequest = (request as TxnOffsetCommitRequest).data()
                assertEquals(consumerGroupId, txnOffsetCommitRequest.groupId)
                assertEquals(producerId, txnOffsetCommitRequest.producerId)
                assertEquals(producerEpoch, txnOffsetCommitRequest.producerEpoch)
                true
            },
            response = TxnOffsetCommitResponse(0, txnOffsetCommitResponse),
        )
    }

    @Suppress("SameParameterValue")
    private fun prepareTxnOffsetCommitResponse(
        consumerGroupId: String,
        producerId: Long,
        producerEpoch: Short,
        groupInstanceId: String,
        memberId: String,
        generationId: Int,
        txnOffsetCommitResponse: Map<TopicPartition, Errors>,
    ) {
        client.prepareResponse(
            matcher = { request ->
                val txnOffsetCommitRequest = (request as TxnOffsetCommitRequest).data()
                assertEquals(consumerGroupId, txnOffsetCommitRequest.groupId)
                assertEquals(producerId, txnOffsetCommitRequest.producerId)
                assertEquals(producerEpoch, txnOffsetCommitRequest.producerEpoch)
                assertEquals(groupInstanceId, txnOffsetCommitRequest.groupInstanceId)
                assertEquals(memberId, txnOffsetCommitRequest.memberId)
                assertEquals(generationId, txnOffsetCommitRequest.generationId)
                true
            },
            response = TxnOffsetCommitResponse(0, txnOffsetCommitResponse),
        )
    }

    @Suppress("SameParameterValue")
    private fun produceResponse(
        tp: TopicPartition,
        offset: Long,
        error: Errors,
        throttleTimeMs: Int,
    ): ProduceResponse = produceResponse(
        tp = tp,
        offset = offset,
        error = error,
        throttleTimeMs = throttleTimeMs,
        logStartOffset = 10
    )

    @Suppress("Deprecation")
    private fun produceResponse(
        tp: TopicPartition,
        offset: Long,
        error: Errors,
        throttleTimeMs: Int,
        logStartOffset: Int,
    ): ProduceResponse {
        val resp = ProduceResponse.PartitionResponse(
            error, offset, RecordBatch.NO_TIMESTAMP,
            logStartOffset.toLong()
        )
        val partResp = mapOf(tp to resp)
        return ProduceResponse(partResp, throttleTimeMs)
    }

    private fun initializeIdempotentProducerId(producerId: Long, epoch: Short) {
        val responseData = InitProducerIdResponseData()
            .setErrorCode(Errors.NONE.code)
            .setProducerEpoch(epoch)
            .setProducerId(producerId)
            .setThrottleTimeMs(0)
        client.prepareResponse(
            matcher = { body ->
                val initProducerIdRequest = body as InitProducerIdRequest
                assertNull(initProducerIdRequest.data().transactionalId)
                true
            },
            response = InitProducerIdResponse(responseData),
            disconnected = false,
        )
        runUntil { transactionManager.hasProducerId }
    }

    private fun doInitTransactions(producerId: Long = this.producerId, epoch: Short = this.epoch) {
        val result = transactionManager.initializeTransactions()
        prepareFindCoordinatorResponse(
            error = Errors.NONE,
            shouldDisconnect = false,
            coordinatorType = CoordinatorType.TRANSACTION,
            coordinatorKey = transactionalId,
        )
        runUntil { transactionManager.coordinator(CoordinatorType.TRANSACTION) != null }
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION))
        prepareInitPidResponse(Errors.NONE, false, producerId, epoch)
        runUntil { transactionManager.hasProducerId }
        result.await()
        assertTrue(result.isSuccessful)
        assertTrue(result.isAcked)
    }

    private fun assertAbortableError(cause: Class<out RuntimeException?>) {
        try {
            transactionManager.beginCommit()
            fail("Should have raised " + cause.getSimpleName())
        } catch (e: KafkaException) {
            assertTrue(cause.isAssignableFrom(e.cause!!.javaClass))
            assertTrue(transactionManager.hasError)
        }
        assertTrue(transactionManager.hasError)
        transactionManager.beginAbort()
        assertFalse(transactionManager.hasError)
    }

    private fun assertFatalError(cause: Class<out RuntimeException?>) {
        assertTrue(transactionManager.hasError)
        try {
            transactionManager.beginAbort()
            fail("Should have raised " + cause.getSimpleName())
        } catch (e: KafkaException) {
            assertTrue(cause.isAssignableFrom(e.cause!!.javaClass))
            assertTrue(transactionManager.hasError)
        }

        // Transaction abort cannot clear fatal error state
        try {
            transactionManager.beginAbort()
            fail("Should have raised " + cause.getSimpleName())
        } catch (e: KafkaException) {
            assertTrue(cause.isAssignableFrom(e.cause!!.javaClass))
            assertTrue(transactionManager.hasError)
        }
    }

    @Throws(InterruptedException::class)
    private fun assertProduceFutureFailed(future: Future<RecordMetadata>) {
        assertTrue(future.isDone)
        try {
            future.get()
            fail("Expected produce future to throw")
        } catch (e: ExecutionException) {
            // expected
        }
    }

    private fun runUntil(condition: () -> Boolean) {
        ProducerTestUtils.runUntil(sender, condition)
    }

    companion object {

        private const val MAX_REQUEST_SIZE = 1024 * 1024

        private const val ACKS_ALL: Short = -1

        private const val MAX_RETRIES = Int.MAX_VALUE

        private const val MAX_BLOCK_TIMEOUT = 1000

        private const val REQUEST_TIMEOUT = 1000

        private const val DEFAULT_RETRY_BACKOFF_MS = 100L
    }
}
