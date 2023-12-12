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

import java.nio.ByteBuffer
import java.util.UUID
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordDeserializationException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.ControlRecordType
import org.apache.kafka.common.record.EndTransactionMarker
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.UUIDDeserializer
import org.apache.kafka.common.serialization.UUIDSerializer
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CompletedFetchTest {

    @Test
    fun testSimple() {
        val fetchOffset: Long = 5
        val startingOffset = 10
        val numRecords = 11 // Records for 10-20
        val partitionData = FetchResponseData.PartitionData()
            .setRecords(newRecords(startingOffset.toLong(), numRecords, fetchOffset))

        val completedFetch = newCompletedFetch(fetchOffset, partitionData)

        var records = completedFetch.fetchRecords(maxRecords = 10)
        assertEquals(10, records.size)
        var record = records[0]
        assertEquals(10, record.offset)

        records = completedFetch.fetchRecords(maxRecords = 10)
        assertEquals(1, records.size)
        record = records[0]
        assertEquals(20, record.offset)

        records = completedFetch.fetchRecords(maxRecords = 10)
        assertEquals(0, records.size)
    }

    @Test
    fun testAbortedTransactionRecordsRemoved() {
        val numRecords = 10
        val rawRecords = newTranscactionalRecords(ControlRecordType.ABORT, numRecords)

        val partitionData = FetchResponseData.PartitionData()
            .setRecords(rawRecords)
            .setAbortedTransactions(newAbortedTransactions())

        var completedFetch = newCompletedFetch(
            isolationLevel = IsolationLevel.READ_COMMITTED,
            offsetResetStrategy = OffsetResetStrategy.NONE,
            checkCrcs = true,
            fetchOffset = 0,
            partitionData = partitionData
        )

        var records = completedFetch.fetchRecords(maxRecords = 10)
        assertEquals(0, records.size)

        completedFetch = newCompletedFetch(
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
            offsetResetStrategy = OffsetResetStrategy.NONE,
            checkCrcs = true,
            fetchOffset = 0,
            partitionData = partitionData,
        )
        records = completedFetch.fetchRecords(maxRecords = 10)
        assertEquals(numRecords, records.size)
    }

    @Test
    fun testCommittedTransactionRecordsIncluded() {
        val numRecords = 10
        val rawRecords = newTranscactionalRecords(ControlRecordType.COMMIT, numRecords)
        val partitionData = FetchResponseData.PartitionData()
            .setRecords(rawRecords)

        val completedFetch = newCompletedFetch(
            isolationLevel = IsolationLevel.READ_COMMITTED,
            offsetResetStrategy = OffsetResetStrategy.NONE,
            checkCrcs = true,
            fetchOffset = 0,
            partitionData = partitionData
        )

        val records = completedFetch.fetchRecords(10)
        assertEquals(10, records.size)
    }

    @Test
    fun testNegativeFetchCount() {
        val fetchOffset: Long = 0
        val startingOffset = 0
        val numRecords = 10
        val partitionData = FetchResponseData.PartitionData()
            .setRecords(newRecords(startingOffset.toLong(), numRecords, fetchOffset))

        val completedFetch = newCompletedFetch(fetchOffset, partitionData)

        val records = completedFetch.fetchRecords(-10)
        assertEquals(0, records.size)
    }

    @Test
    fun testNoRecordsInFetch() {
        val partitionData = FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(10)
            .setLastStableOffset(20)
            .setLogStartOffset(0)

        val completedFetch = newCompletedFetch(
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
            offsetResetStrategy = OffsetResetStrategy.NONE,
            checkCrcs = false,
            fetchOffset = 1,
            partitionData = partitionData,
        )

        val records = completedFetch.fetchRecords(10)
        assertEquals(0, records.size)
    }

    @Test
    fun testCorruptedMessage() {
        // Create one good record and then one "corrupted" record.
        val builder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0,
        )
        builder.append(SimpleRecord(value = UUIDSerializer().serialize(TOPIC_NAME, UUID.randomUUID())))
        builder.append(timestamp = 0L, key = "key".toByteArray(), value = "value".toByteArray())
        val records: Records = builder.build()
        val partitionData = FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(10)
            .setLastStableOffset(20)
            .setLogStartOffset(0)
            .setRecords(records)
        val completedFetch = newCompletedFetch(
            keyDeserializer = UUIDDeserializer(),
            valueDeserializer = UUIDDeserializer(),
            isolationLevel = IsolationLevel.READ_COMMITTED,
            offsetResetStrategy = OffsetResetStrategy.NONE,
            checkCrcs = false,
            fetchOffset = 0,
            partitionData = partitionData,
        )
        completedFetch.fetchRecords(10)
        assertFailsWith<RecordDeserializationException> { completedFetch.fetchRecords(10) }
    }

    private fun newCompletedFetch(
        fetchOffset: Long,
        partitionData: FetchResponseData.PartitionData,
    ): CompletedFetch<String, String> = newCompletedFetch(
        isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        offsetResetStrategy = OffsetResetStrategy.NONE,
        checkCrcs = true,
        fetchOffset = fetchOffset,
        partitionData = partitionData,
    )

    private fun newCompletedFetch(
        isolationLevel: IsolationLevel,
        offsetResetStrategy: OffsetResetStrategy,
        checkCrcs: Boolean,
        fetchOffset: Long,
        partitionData: FetchResponseData.PartitionData,
    ): CompletedFetch<String, String> = newCompletedFetch(
        keyDeserializer = StringDeserializer(),
        valueDeserializer = StringDeserializer(),
        isolationLevel = isolationLevel,
        offsetResetStrategy = offsetResetStrategy,
        checkCrcs = checkCrcs,
        fetchOffset = fetchOffset,
        partitionData = partitionData,
    )

    private fun <K, V> newCompletedFetch(
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
        isolationLevel: IsolationLevel,
        offsetResetStrategy: OffsetResetStrategy,
        checkCrcs: Boolean,
        fetchOffset: Long,
        partitionData: FetchResponseData.PartitionData,
    ): CompletedFetch<K, V> {
        val logContext = LogContext()
        val subscriptions = SubscriptionState(logContext, offsetResetStrategy)
        val metricsRegistry = FetchMetricsRegistry()
        val metrics = FetchMetricsManager(Metrics(), metricsRegistry)
        val metricAggregator = FetchMetricsAggregator(metrics, setOf(TP))
        val fetchConfig = FetchConfig(
            minBytes = ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            maxBytes = ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            maxWaitMs = ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            fetchSize = ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            maxPollRecords = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
            checkCrcs = checkCrcs,
            clientRackId = ConsumerConfig.DEFAULT_CLIENT_RACK,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            isolationLevel = isolationLevel,
        )
        return CompletedFetch(
            logContext = logContext,
            subscriptions = subscriptions,
            fetchConfig = fetchConfig,
            decompressionBufferSupplier = BufferSupplier.create(),
            partition = TP,
            partitionData = partitionData,
            metricAggregator = metricAggregator,
            fetchOffset = fetchOffset,
            requestVersion = ApiKeys.FETCH.latestVersion(),
        )
    }

    private fun newRecords(baseOffset: Long, count: Int, firstMessageId: Long): Records {
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = ByteBuffer.allocate(1024),
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = baseOffset,
        )
        for (i in 0..<count) builder.append(
            timestamp = 0L,
            key = "key".toByteArray(),
            value = ("value-" + (firstMessageId + i)).toByteArray(),
        )
        return builder.build()
    }

    private fun newTranscactionalRecords(controlRecordType: ControlRecordType, numRecords: Int): Records {
        val time: Time = MockTime()
        val buffer = ByteBuffer.allocate(1024)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0,
            logAppendTime = time.milliseconds(),
            producerId = PRODUCER_ID,
            producerEpoch = PRODUCER_EPOCH,
            baseSequence = 0,
            isTransactional = true,
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
        )
        for (i in 0..<numRecords) builder.append(
            SimpleRecord(
                timestamp = time.milliseconds(),
                key = "key".toByteArray(),
                value = "value".toByteArray(),
            )
        )
        builder.build()
        writeTransactionMarker(
            buffer = buffer,
            controlRecordType = controlRecordType,
            offset = numRecords,
            time = time,
        )
        buffer.flip()
        return MemoryRecords.readableRecords(buffer)
    }

    private fun writeTransactionMarker(
        buffer: ByteBuffer,
        controlRecordType: ControlRecordType,
        offset: Int,
        time: Time,
    ) = MemoryRecords.writeEndTransactionalMarker(
        buffer = buffer,
        initialOffset = offset.toLong(),
        timestamp = time.milliseconds(),
        partitionLeaderEpoch = 0,
        producerId = PRODUCER_ID,
        producerEpoch = PRODUCER_EPOCH,
        marker = EndTransactionMarker(controlRecordType, 0),
    )

    private fun newAbortedTransactions(): List<AbortedTransaction> {
        val abortedTransaction = AbortedTransaction()
        abortedTransaction.setFirstOffset(0)
        abortedTransaction.setProducerId(PRODUCER_ID)
        return listOf(abortedTransaction)
    }

    companion object {

        private const val TOPIC_NAME = "test"

        private val TP = TopicPartition(TOPIC_NAME, 0)

        private const val PRODUCER_ID = 1000L

        private const val PRODUCER_EPOCH: Short = 0
    }
}
