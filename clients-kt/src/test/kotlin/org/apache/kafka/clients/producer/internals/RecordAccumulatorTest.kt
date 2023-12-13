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
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.internals.RecordAccumulator.PartitionerConfig
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.CompressionRatioEstimator.resetEstimation
import org.apache.kafka.common.record.CompressionRatioEstimator.setEstimation
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.DefaultRecord
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class RecordAccumulatorTest {

    private val topic = "test"

    private val partition1 = 0

    private val partition2 = 1

    private val partition3 = 2

    private val node1 = Node(0, "localhost", 1111)

    private val node2 = Node(1, "localhost", 1112)

    private val tp1 = TopicPartition(topic, partition1)

    private val tp2 = TopicPartition(topic, partition2)

    private val tp3 = TopicPartition(topic, partition3)

    private val part1 = PartitionInfo(
        topic = topic,
        partition = partition1,
        leader = node1,
        replicas = emptyList(),
        inSyncReplicas = emptyList(),
    )

    private val part2 = PartitionInfo(
        topic = topic,
        partition = partition2,
        leader = node1,
        replicas = emptyList(),
        inSyncReplicas = emptyList(),
    )

    private val part3 = PartitionInfo(
        topic = topic,
        partition = partition3,
        leader = node2,
        replicas = emptyList(),
        inSyncReplicas = emptyList(),
    )

    private val time = MockTime()

    private val key = "key".toByteArray()

    private val value = "value".toByteArray()

    private val msgSize = DefaultRecord.sizeInBytes(
        offsetDelta = 0,
        timestampDelta = 0,
        keySize = key.size,
        valueSize = value.size,
        headers = Record.EMPTY_HEADERS,
    )

    private val cluster = Cluster(
        clusterId = null,
        nodes = listOf(node1, node2),
        partitions = listOf(part1, part2, part3),
        unauthorizedTopics = emptySet(),
        invalidTopics = emptySet(),
    )

    private val metrics = Metrics(time = time)

    private val maxBlockTimeMs: Long = 1000

    private val logContext = LogContext()

    @AfterEach
    fun teardown() {
        metrics.close()
    }

    @Test
    @Throws(Exception::class)
    fun testDrainBatches() {
        // test case: node1(tp1,tp2) , node2(tp3,tp4)
        // add tp-4
        val partition4 = 3
        val tp4 = TopicPartition(topic, partition4)
        val part4 = PartitionInfo(
            topic = topic,
            partition = partition4,
            leader = node2,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val batchSize = (value.size + DefaultRecordBatch.RECORD_BATCH_OVERHEAD).toLong()
        val accum = createTestRecordAccumulator(
            batchSize = batchSize.toInt(),
            totalSize = Int.MAX_VALUE.toLong(),
            type = CompressionType.NONE,
            lingerMs = 10,
        )
        val cluster = Cluster(
            clusterId = null,
            nodes = listOf(node1, node2),
            partitions = listOf(part1, part2, part3, part4),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )

        //  initial data
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition2,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition3,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition4,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )

        // drain batches from 2 nodes: node1 => tp1, node2 => tp3, because the max request size is full after the first batch drained
        val batches1 = accum.drain(
            cluster = cluster,
            nodes = setOf(node1, node2),
            maxSize = batchSize.toInt(),
            now = 0,
        )
        verifyTopicPartitionInBatches(batches1, tp1, tp3)

        // add record for tp1, tp3
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition3,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )

        // drain batches from 2 nodes: node1 => tp2, node2 => tp4, because the max request size is full after the first batch drained
        // The drain index should start from next topic partition, that is, node1 => tp2, node2 => tp4
        val batches2 = accum.drain(
            cluster = cluster,
            nodes = setOf(node1, node2),
            maxSize = batchSize.toInt(),
            now = 0,
        )
        verifyTopicPartitionInBatches(batches2, tp2, tp4)

        // make sure in next run, the drain index will start from the beginning
        val batches3 = accum.drain(
            cluster = cluster,
            nodes = setOf(node1, node2),
            maxSize = batchSize.toInt(),
            now = 0,
        )
        verifyTopicPartitionInBatches(batches3, tp1, tp3)

        // add record for tp2, tp3, tp4 and mute the tp4
        accum.append(
            topic = topic,
            partition = partition2,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition3,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition4,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.mutePartition(tp4)
        // drain batches from 2 nodes: node1 => tp2, node2 => tp3 (because tp4 is muted)
        val batches4 = accum.drain(
            cluster = cluster,
            nodes = setOf(node1, node2),
            maxSize = batchSize.toInt(),
            now = 0,
        )
        verifyTopicPartitionInBatches(batches4, tp2, tp3)

        // add record for tp1, tp2, tp3, and unmute tp4
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition2,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition3,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.unmutePartition(tp4)
        // set maxSize as a max value, so that the all partitions in 2 nodes should be drained: node1 => [tp1, tp2], node2 => [tp3, tp4]
        val batches5 = accum.drain(
            cluster = cluster,
            nodes = setOf(node1, node2),
            maxSize = Int.MAX_VALUE,
            now = 0,
        )
        verifyTopicPartitionInBatches(batches5, tp1, tp2, tp3, tp4)
    }

    private fun verifyTopicPartitionInBatches(
        nodeBatches: Map<Int, List<ProducerBatch>>,
        vararg tp: TopicPartition,
    ) {
        val allTpBatchCount = nodeBatches.values.flatten().size

        assertEquals(tp.size, allTpBatchCount)

        val topicPartitionsInBatch = mutableListOf<TopicPartition>()
        for (tpBatchList in nodeBatches.values) {
            val tpList = tpBatchList.map { producerBatch -> producerBatch.topicPartition }
            topicPartitionsInBatch.addAll(tpList)
        }
        for (i in tp.indices) assertEquals(tp[i], topicPartitionsInBatch[i])
    }

    @Test
    @Throws(Exception::class)
    fun testFull() {
        val now = time.milliseconds()

        // test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = 10L * batchSize,
            type = CompressionType.NONE,
            lingerMs = 10,
        )
        val appends = expectedNumAppends(batchSize)
        for (i in 0 until appends) {
            // append to the first batch
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            val partitionBatches = accum.getDeque(tp1)

            assertEquals(1, partitionBatches!!.size)

            val batch = partitionBatches.peekFirst()

            assertTrue(batch.isWritable)
            assertEquals(
                expected = 0,
                actual = accum.ready(cluster, now).readyNodes.size,
                message = "No partitions should be ready.",
            )
        }

        // this append doesn't fit in the first batch, so a new batch is created and the first batch is closed
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        val partitionBatches = accum.getDeque(tp1)

        assertEquals(2, partitionBatches!!.size)

        val partitionBatchesIterator = partitionBatches.iterator()

        assertTrue(partitionBatchesIterator.next().isWritable)
        assertEquals(
            expected = setOf(node1),
            actual = accum.ready(cluster, time.milliseconds()).readyNodes,
            message = "Our partition's leader should be ready",
        )
        val batches = accum.drain(
            cluster = cluster,
            nodes = setOf(node1),
            maxSize = Int.MAX_VALUE,
            now = 0,
        )[node1.id]!!

        assertEquals(1, batches.size)

        val batch = batches[0]
        val iter = batch.records().records().iterator()
        repeat(appends) {
            val record = iter.next()
            assertEquals(ByteBuffer.wrap(key), record.key(), "Keys should match")
            assertEquals(ByteBuffer.wrap(value), record.value(), "Values should match")
        }

        assertFalse(iter.hasNext(), "No more records")
    }

    @Test
    @Throws(Exception::class)
    fun testAppendLargeCompressed() = testAppendLarge(CompressionType.GZIP)

    @Test
    @Throws(Exception::class)
    fun testAppendLargeNonCompressed() = testAppendLarge(CompressionType.NONE)

    @Throws(Exception::class)
    private fun testAppendLarge(compressionType: CompressionType) {
        val batchSize = 512
        val value = ByteArray(2 * batchSize)
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * 1024).toLong(),
            type = compressionType,
            lingerMs = 0,
        )
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )

        assertEquals(
            expected = setOf(node1),
            actual = accum.ready(cluster, time.milliseconds()).readyNodes,
            message = "Our partition's leader should be ready",
        )

        val batches = accum.getDeque(tp1)

        assertEquals(1, batches!!.size)

        val producerBatch = batches.peek()
        val recordBatches = producerBatch.records().batches().toList()

        assertEquals(1, recordBatches.size)

        val recordBatch = recordBatches[0]

        assertEquals(0L, recordBatch.baseOffset())

        val records = recordBatch.toList()

        assertEquals(1, records.size)

        val record = records[0]

        assertEquals(0L, record.offset())
        assertEquals(ByteBuffer.wrap(key), record.key())
        assertEquals(ByteBuffer.wrap(value), record.value())
        assertEquals(0L, record.timestamp())
    }

    @Test
    @Throws(Exception::class)
    fun testAppendLargeOldMessageFormatCompressed() =
        testAppendLargeOldMessageFormat(CompressionType.GZIP)

    @Test
    @Throws(Exception::class)
    fun testAppendLargeOldMessageFormatNonCompressed() =
        testAppendLargeOldMessageFormat(CompressionType.NONE)

    @Throws(Exception::class)
    private fun testAppendLargeOldMessageFormat(compressionType: CompressionType) {
        val batchSize = 512
        val value = ByteArray(2 * batchSize)
        val apiVersions = ApiVersions()
        apiVersions.update(
            nodeId = node1.idString(),
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.PRODUCE.id,
                minVersion = 0,
                maxVersion = 2,
            ),
        )
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * 1024).toLong(),
            type = compressionType,
            lingerMs = 0,
        )
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster
        )

        assertEquals(
            expected = setOf(node1),
            actual = accum.ready(cluster, time.milliseconds()).readyNodes,
            message = "Our partition's leader should be ready"
        )

        val batches = accum.getDeque(tp1)

        assertEquals(1, batches!!.size)

        val producerBatch = batches.peek()
        val recordBatches = producerBatch.records().batches().toList()

        assertEquals(1, recordBatches.size)

        val recordBatch = recordBatches[0]

        assertEquals(0L, recordBatch.baseOffset())

        val records = recordBatch.toList()

        assertEquals(1, records.size)

        val record = records[0]

        assertEquals(0L, record.offset())
        assertEquals(ByteBuffer.wrap(key), record.key())
        assertEquals(ByteBuffer.wrap(value), record.value())
        assertEquals(0L, record.timestamp())
    }

    @Test
    @Throws(Exception::class)
    fun testLinger() {
        val lingerMs = 10
        val accum = createTestRecordAccumulator(
            batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * 1024).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )

        assertEquals(
            expected = 0,
            actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
            message = "No partitions should be ready",
        )

        time.sleep(10)

        assertEquals(
            expected = setOf(node1),
            actual = accum.ready(cluster, time.milliseconds()).readyNodes,
            message = "Our partition's leader should be ready",
        )

        val batches = accum.drain(
            cluster = cluster,
            nodes = setOf(node1),
            maxSize = Int.MAX_VALUE,
            now = 0,
        )[node1.id]!!

        assertEquals(1, batches.size)

        val batch = batches[0]
        val iter = batch.records().records().iterator()
        val record = iter.next()

        assertEquals(ByteBuffer.wrap(key), record.key(), "Keys should match")
        assertEquals(ByteBuffer.wrap(value), record.value(), "Values should match")
        assertFalse(iter.hasNext(), "No more records")
    }

    @Test
    @Throws(Exception::class)
    fun testPartialDrain() {
        val accum = createTestRecordAccumulator(
            batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * 1024).toLong(),
            type = CompressionType.NONE,
            lingerMs = 10,
        )
        val appends = 1024 / msgSize + 1
        val partitions = listOf(tp1, tp2)
        for (tp in partitions) {
            repeat(appends) {
                accum.append(
                    topic = tp.topic,
                    partition = tp.partition,
                    timestamp = 0L,
                    key = key,
                    value = value,
                    headers = Record.EMPTY_HEADERS,
                    callbacks = null,
                    maxTimeToBlock = maxBlockTimeMs,
                    abortOnNewBatch = false,
                    nowMs = time.milliseconds(),
                    cluster = cluster,
                )
            }
        }

        assertEquals(
            expected = setOf(node1),
            actual = accum.ready(cluster, time.milliseconds()).readyNodes,
            message = "Partition's leader should be ready",
        )

        val batches = accum.drain(
            cluster = cluster,
            nodes = setOf(node1),
            maxSize = 1024,
            now = 0,
        )[node1.id]!!

        assertEquals(
            expected = 1,
            actual = batches.size,
            message = "But due to size bound only one partition should have been retrieved",
        )
    }

    @Suppress("unused")
    @Test
    @Throws(Exception::class)
    fun testStressfulSituation() {
        val numThreads = 5
        val msgs = 10000
        val numParts = 2
        val accum = createTestRecordAccumulator(
            batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * 1024).toLong(),
            type = CompressionType.NONE,
            lingerMs = 0,
        )
        val threads = List(numThreads) {
            Thread {
                repeat(msgs) { i ->
                    try {
                        accum.append(
                            topic = topic,
                            partition = i % numParts,
                            timestamp = 0L,
                            key = key,
                            value = value,
                            headers = Record.EMPTY_HEADERS,
                            callbacks = null,
                            maxTimeToBlock = maxBlockTimeMs,
                            abortOnNewBatch = false,
                            nowMs = time.milliseconds(),
                            cluster = cluster,
                        )
                    } catch (e: Exception) {
                        e.printStackTrace()
                    }
                }
            }
        }
        for (t in threads) t.start()
        var read = 0
        val now = time.milliseconds()
        while (read < numThreads * msgs) {
            val nodes = accum.ready(cluster, now).readyNodes
            val batches = accum.drain(cluster, nodes, 5 * 1024, 0)[node1.id]
            if (batches != null) {
                for (batch in batches) {
                    for (record in batch.records().records()) read++
                    accum.deallocate(batch)
                }
            }
        }
        for (thread in threads) thread.join()
    }

    @Test
    @Throws(Exception::class)
    fun testNextReadyCheckDelay() {
        // Next check time will use lingerMs since this test won't trigger any retries/backoff
        val lingerMs = 10

        // test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * batchSize).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )
        // Just short of going over the limit so we trigger linger time
        val appends = expectedNumAppends(batchSize)

        // Partition on node1 only
        repeat(appends) {
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
        }
        var result = accum.ready(cluster, time.milliseconds())
        assertEquals(0, result.readyNodes.size, "No nodes should be ready.")
        assertEquals(
            expected = lingerMs.toLong(),
            actual = result.nextReadyCheckDelayMs,
            message = "Next check time should be the linger time"
        )
        time.sleep((lingerMs / 2).toLong())

        // Add partition on node2 only
        repeat(appends) {
            accum.append(
                topic = topic,
                partition = partition3,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
        }
        result = accum.ready(cluster, time.milliseconds())
        assertEquals(0, result.readyNodes.size, "No nodes should be ready.")
        assertEquals(
            expected = (lingerMs / 2).toLong(),
            actual = result.nextReadyCheckDelayMs,
            message = "Next check time should be defined by node1, half remaining linger time",
        )

        // Add data for another partition on node1, enough to make data sendable immediately
        repeat(appends + 1) {
            accum.append(
                topic = topic,
                partition = partition2,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster
            )
        }

        result = accum.ready(cluster, time.milliseconds())
        assertEquals(setOf(node1), result.readyNodes, "Node1 should be ready")
        // Note this can actually be < linger time because it may use delays from partitions that aren't sendable
        // but have leaders with other sendable data.
        assertTrue(
            result.nextReadyCheckDelayMs <= lingerMs,
            "Next check time should be defined by node2, at most linger time"
        )
    }

    @Test
    @Throws(Exception::class)
    fun testRetryBackoff() {
        val lingerMs = Int.MAX_VALUE / 16
        val retryBackoffMs = (Int.MAX_VALUE / 8).toLong()
        val deliveryTimeoutMs = Int.MAX_VALUE
        val totalSize = (10 * 1024).toLong()
        val batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD
        val metricGrpName = "producer-metrics"
        val accum = RecordAccumulator(
            logContext = logContext,
            batchSize = batchSize,
            compression = CompressionType.NONE,
            lingerMs = lingerMs,
            retryBackoffMs = retryBackoffMs,
            deliveryTimeoutMs = deliveryTimeoutMs,
            metrics = metrics,
            metricGrpName = metricGrpName,
            time = time,
            apiVersions = ApiVersions(),
            transactionManager = null,
            free = BufferPool(
                totalMemory = totalSize,
                poolableSize = batchSize,
                metrics = metrics,
                time = time,
                metricGrpName = metricGrpName,
            ),
        )
        var now = time.milliseconds()
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        var result = accum.ready(cluster, now + lingerMs + 1)
        assertEquals(setOf(node1), result.readyNodes, "Node1 should be ready")
        var batches = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = now + lingerMs + 1,
        )
        assertEquals(1, batches.size, "Node1 should be the only ready node.")
        assertEquals(1, batches[0]!!.size, "Partition 0 should only have one batch drained.")

        // Reenqueue the batch
        now = time.milliseconds()
        accum.reenqueue(batches[0]!![0], now)

        // Put message for partition 1 into accumulator
        accum.append(
            topic = topic,
            partition = partition2,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        result = accum.ready(cluster, now + lingerMs + 1)
        assertEquals(setOf(node1), result.readyNodes, "Node1 should be ready")

        // tp1 should backoff while tp2 should not
        batches = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = now + lingerMs + 1,
        )
        assertEquals(1, batches.size, "Node1 should be the only ready node.")
        assertEquals(1, batches[0]!!.size, "Node1 should only have one batch drained.")
        assertEquals(
            expected = tp2,
            actual = batches[0]!![0].topicPartition,
            message = "Node1 should only have one batch for partition 1.",
        )

        // Partition 0 can be drained after retry backoff
        result = accum.ready(cluster, now + retryBackoffMs + 1)
        assertEquals(setOf(node1), result.readyNodes, "Node1 should be ready")
        batches = accum.drain(cluster, result.readyNodes, Int.MAX_VALUE, now + retryBackoffMs + 1)
        assertEquals(1, batches.size, "Node1 should be the only ready node.")
        assertEquals(1, batches[0]!!.size, "Node1 should only have one batch drained.")
        assertEquals(
            expected = tp1,
            actual = batches[0]!![0].topicPartition,
            message = "Node1 should only have one batch for partition 0.",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testFlush() {
        val lingerMs = Int.MAX_VALUE
        val accum = createTestRecordAccumulator(
            batchSize = 4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (64 * 1024).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )
        repeat(100) { i ->
            accum.append(
                topic = topic,
                partition = i % 3,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            assertTrue(accum.hasIncomplete())
        }
        var result = accum.ready(cluster, time.milliseconds())
        assertEquals(0, result.readyNodes.size, "No nodes should be ready.")
        accum.beginFlush()
        result = accum.ready(cluster, time.milliseconds())

        // drain and deallocate all batches
        val results = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertTrue(accum.hasIncomplete())

        for (batches in results.values)
            for (batch in batches) accum.deallocate(batch)

        // should be complete with no unsent records.
        accum.awaitFlushCompletion()
        assertFalse(accum.hasUndrained())
        assertFalse(accum.hasIncomplete())
    }

    private fun delayedInterrupt(thread: Thread, delayMs: Long) {
        val t = Thread {
            Time.SYSTEM.sleep(delayMs)
            thread.interrupt()
        }
        t.start()
    }

    @Test
    @Throws(Exception::class)
    fun testAwaitFlushComplete() {
        val accum = createTestRecordAccumulator(
            batchSize = 4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = 64 * 1024,
            type = CompressionType.NONE,
            lingerMs = Int.MAX_VALUE,
        )
        accum.append(
            topic = topic,
            partition = 0,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accum.beginFlush()
        assertTrue(accum.flushInProgress())
        delayedInterrupt(Thread.currentThread(), 1000L)

        assertFailsWith<InterruptedException>(
            message = "awaitFlushCompletion should throw InterruptException",
        ) { accum.awaitFlushCompletion() }
        assertFalse(
            accum.flushInProgress(),
            "flushInProgress count should be decremented even if thread is interrupted"
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAbortIncompleteBatches() {
        val lingerMs = Int.MAX_VALUE
        val numRecords = 100
        val numExceptionReceivedInCallback = AtomicInteger(0)
        val accum = createTestRecordAccumulator(
            batchSize = 128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = 64 * 1024,
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )

        class TestCallback : RecordAccumulator.AppendCallbacks {

            override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                assertEquals("Producer is closed forcefully.", exception!!.message)
                numExceptionReceivedInCallback.incrementAndGet()
            }

            override fun setPartition(partition: Int) = Unit
        }
        repeat(numRecords) { i ->
            accum.append(
                topic = topic,
                partition = i % 3,
                timestamp = 0L,
                key = key,
                value = value,
                headers = null,
                callbacks = TestCallback(),
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster
            )
        }
        val result = accum.ready(cluster, time.milliseconds())

        assertFalse(result.readyNodes.isEmpty())

        val drained = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertTrue(accum.hasUndrained())
        assertTrue(accum.hasIncomplete())

        var numDrainedRecords = 0
        for (drainedEntry in drained) {
            for (batch in drainedEntry.value) {
                assertTrue(batch.isClosed)
                assertFalse(batch.produceFuture.completed)

                numDrainedRecords += batch.recordCount
            }
        }

        assertTrue(numDrainedRecords in 1 until numRecords)

        accum.abortIncompleteBatches()

        assertEquals(numRecords, numExceptionReceivedInCallback.get())
        assertFalse(accum.hasUndrained())
        assertFalse(accum.hasIncomplete())
    }

    @Test
    @Throws(Exception::class)
    fun testAbortUnsentBatches() {
        val lingerMs = Int.MAX_VALUE
        val numRecords = 100
        val numExceptionReceivedInCallback = AtomicInteger(0)
        val accum = createTestRecordAccumulator(
            batchSize = 128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (64 * 1024).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )
        val cause = KafkaException()

        class TestCallback : RecordAccumulator.AppendCallbacks {

            override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                assertEquals(cause, exception)
                numExceptionReceivedInCallback.incrementAndGet()
            }

            override fun setPartition(partition: Int) {}
        }

        repeat(numRecords) { i ->
            accum.append(
                topic = topic,
                partition = i % 3,
                timestamp = 0L,
                key = key,
                value = value,
                headers = null,
                callbacks = TestCallback(),
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
        }
        val result = accum.ready(cluster, time.milliseconds())

        assertFalse(result.readyNodes.isEmpty())

        val drained = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertTrue(accum.hasUndrained())
        assertTrue(accum.hasIncomplete())

        accum.abortUndrainedBatches(cause)
        var numDrainedRecords = 0
        for (drainedEntry in drained) {
            for (batch in drainedEntry.value) {
                assertTrue(batch.isClosed)
                assertFalse(batch.produceFuture.completed)

                numDrainedRecords += batch.recordCount
            }
        }

        assertTrue(numDrainedRecords > 0)
        assertTrue(numExceptionReceivedInCallback.get() > 0)
        assertEquals(numRecords, numExceptionReceivedInCallback.get() + numDrainedRecords)
        assertFalse(accum.hasUndrained())
        assertTrue(accum.hasIncomplete())
    }

    @Throws(InterruptedException::class)
    private fun doExpireBatchSingle(deliveryTimeoutMs: Int) {
        val lingerMs = 300
        val muteStates = listOf(false, true)
        var readyNodes: Set<Node?>
        var expiredBatches: List<ProducerBatch?>
        // test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            deliveryTimeoutMs = deliveryTimeoutMs,
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * batchSize).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs
        )

        // Make the batches ready due to linger. These batches are not in retry
        for (mute in muteStates) {
            if (time.milliseconds() < System.currentTimeMillis())
                time.setCurrentTimeMs(System.currentTimeMillis())

            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )

            assertEquals(
                expected = 0,
                actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
                message = "No partition should be ready.",
            )

            time.sleep(lingerMs.toLong())
            readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes

            assertEquals(setOf(node1), readyNodes, "Our partition's leader should be ready")

            expiredBatches = accum.expiredBatches(time.milliseconds())

            assertEquals(0, expiredBatches.size, "The batch should not expire when just linger has passed")

            if (mute) accum.mutePartition(tp1) else accum.unmutePartition(tp1)

            // Advance the clock to expire the batch.
            time.sleep((deliveryTimeoutMs - lingerMs).toLong())
            expiredBatches = accum.expiredBatches(time.milliseconds())

            assertEquals(
                expected = 1,
                actual = expiredBatches.size,
                message = "The batch may expire when the partition is muted",
            )
            assertEquals(
                expected = 0,
                actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
                message = "No partitions should be ready.",
            )
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testExpiredBatchSingle() = doExpireBatchSingle(3200)

    @Test
    @Throws(InterruptedException::class)
    fun testExpiredBatchSingleMaxValue() = doExpireBatchSingle(Int.MAX_VALUE)

    @Test
    @Throws(InterruptedException::class)
    fun testExpiredBatches() {
        val retryBackoffMs = 100L
        val lingerMs = 30
        val requestTimeout = 60
        val deliveryTimeoutMs = 3200

        // test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            deliveryTimeoutMs = deliveryTimeoutMs,
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * batchSize).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )
        val appends = expectedNumAppends(batchSize)

        // Test batches not in retry
        repeat(appends) {
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            assertEquals(
                expected = 0,
                actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
                message = "No partitions should be ready.",
            )
        }
        // Make the batches ready due to batch full
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = 0,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        var readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes

        assertEquals(setOf(node1), readyNodes, "Our partition's leader should be ready")

        // Advance the clock to expire the batch.
        time.sleep((deliveryTimeoutMs + 1).toLong())
        accum.mutePartition(tp1)
        var expiredBatches: List<ProducerBatch?> = accum.expiredBatches(time.milliseconds())

        assertEquals(
            expected = 2,
            actual = expiredBatches.size,
            message = "The batches will be muted no matter if the partition is muted or not",
        )

        accum.unmutePartition(tp1)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "All batches should have been expired earlier")
        assertEquals(
            expected = 0,
            actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
            message = "No partitions should be ready.",
        )

        // Advance the clock to make the next batch ready due to linger.ms
        time.sleep(lingerMs.toLong())

        assertEquals(setOf(node1), readyNodes, "Our partition's leader should be ready")

        time.sleep((requestTimeout + 1).toLong())
        accum.mutePartition(tp1)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(
            expected = 0,
            actual = expiredBatches.size,
            message = "The batch should not be expired when metadata is still available and partition is muted",
        )

        accum.unmutePartition(tp1)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "All batches should have been expired")
        assertEquals(
            expected = 0,
            actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
            message = "No partitions should be ready.",
        )

        // Test batches in retry.
        // Create a retried batch
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = 0,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        time.sleep(lingerMs.toLong())
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes

        assertEquals(setOf(node1), readyNodes, "Our partition's leader should be ready")

        val drained = accum.drain(cluster, readyNodes, Int.MAX_VALUE, time.milliseconds())

        assertEquals(drained[node1.id]!!.size, 1, "There should be only one batch.")

        time.sleep(1000L)
        accum.reenqueue(drained[node1.id]!![0], time.milliseconds())

        // test expiration.
        time.sleep(requestTimeout + retryBackoffMs)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "The batch should not be expired.")

        time.sleep(1L)
        accum.mutePartition(tp1)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "The batch should not be expired when the partition is muted")

        accum.unmutePartition(tp1)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "All batches should have been expired.")

        // Test that when being throttled muted batches are expired before the throttle time is over.
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = 0,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        time.sleep(lingerMs.toLong())
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes

        assertEquals(setOf(node1), readyNodes, "Our partition's leader should be ready")

        // Advance the clock to expire the batch.
        time.sleep((requestTimeout + 1).toLong())
        accum.mutePartition(tp1)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "The batch should not be expired when the partition is muted")

        val throttleTimeMs = 100L
        accum.unmutePartition(tp1)
        // The batch shouldn't be expired yet.
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "The batch should not be expired when the partition is muted")

        // Once the throttle time is over, the batch can be expired.
        time.sleep(throttleTimeMs)
        expiredBatches = accum.expiredBatches(time.milliseconds())

        assertEquals(0, expiredBatches.size, "All batches should have been expired earlier")
        assertEquals(
            expected = 1,
            actual = accum.ready(cluster, time.milliseconds()).readyNodes.size,
            message = "No partitions should be ready.",
        )
    }

    @Test
    @Throws(InterruptedException::class)
    fun testMutedPartitions() {
        val now = time.milliseconds()
        // test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * batchSize).toLong(),
            type = CompressionType.NONE,
            lingerMs = 10,
        )
        val appends = expectedNumAppends(batchSize)
        repeat(appends) {
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster
            )

            assertEquals(
                expected = 0,
                actual = accum.ready(cluster, now).readyNodes.size,
                message = "No partitions should be ready.",
            )
        }
        time.sleep(2000)

        // Test ready with muted partition
        accum.mutePartition(tp1)
        var result = accum.ready(cluster, time.milliseconds())

        assertEquals(0, result.readyNodes.size, "No node should be ready")

        // Test ready without muted partition
        accum.unmutePartition(tp1)
        result = accum.ready(cluster, time.milliseconds())

        assertTrue(result.readyNodes.isNotEmpty(), "The batch should be ready")

        // Test drain with muted partition
        accum.mutePartition(tp1)
        var drained = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertEquals(0, drained[node1.id]!!.size, "No batch should have been drained")

        // Test drain without muted partition.
        accum.unmutePartition(tp1)
        drained = accum.drain(cluster, result.readyNodes, Int.MAX_VALUE, time.milliseconds())

        assertTrue(drained[node1.id]!!.isNotEmpty(), "The batch should have been drained.")
    }

    @Test
    fun testIdempotenceWithOldMagic() {
        // Simulate talking to an older broker, ie. one which supports a lower magic.
        val apiVersions = ApiVersions()
        val batchSize = 1025
        val deliveryTimeoutMs = 3200
        val lingerMs = 10
        val retryBackoffMs = 100L
        val totalSize = (10 * batchSize).toLong()
        val metricGrpName = "producer-metrics"
        apiVersions.update(
            nodeId = "foobar",
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.PRODUCE.id,
                minVersion = 0,
                maxVersion = 2,
            )
        )
        val transactionManager = TransactionManager(
            logContext = LogContext(),
            transactionalId = null,
            transactionTimeoutMs = 0,
            retryBackoffMs = retryBackoffMs,
            apiVersions = apiVersions,
        )
        val accum = RecordAccumulator(
            logContext = logContext,
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            compression = CompressionType.NONE,
            lingerMs = lingerMs,
            retryBackoffMs = retryBackoffMs,
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
        assertFailsWith<UnsupportedVersionException> {
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = 0,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRecordsDrainedWhenTransactionCompleting() {
        val batchSize = 1025
        val deliveryTimeoutMs = 3200
        val lingerMs = 10
        val totalSize = (10 * batchSize).toLong()
        val transactionManager = mock<TransactionManager>()
        val accumulator = createTestRecordAccumulator(
            txnManager = transactionManager,
            deliveryTimeoutMs = deliveryTimeoutMs,
            batchSize = batchSize,
            totalSize = totalSize,
            type = CompressionType.NONE,
            lingerMs = lingerMs
        )
        val producerIdAndEpoch = ProducerIdAndEpoch(12345L, 5.toShort())
        whenever(transactionManager.producerIdAndEpoch).thenReturn(producerIdAndEpoch)
        whenever(transactionManager.isSendToPartitionAllowed(tp1)).thenReturn(true)
        whenever(transactionManager.isPartitionAdded(tp1)).thenReturn(true)
        whenever(transactionManager.firstInFlightSequence(tp1)).thenReturn(0)

        // Initially, the transaction is still in progress, so we should respect the linger.
        whenever(transactionManager.isCompleting).thenReturn(false)
        accumulator.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        accumulator.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )

        assertTrue(accumulator.hasUndrained())

        val firstResult = accumulator.ready(cluster, time.milliseconds())

        assertEquals(0, firstResult.readyNodes.size)

        val firstDrained = accumulator.drain(
            cluster = cluster,
            nodes = firstResult.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertEquals(0, firstDrained.size)

        // Once the transaction begins completion, then the batch should be drained immediately.
        whenever(transactionManager.isCompleting).thenReturn(true)

        val secondResult = accumulator.ready(cluster, time.milliseconds())

        assertEquals(1, secondResult.readyNodes.size)

        val readyNode = secondResult.readyNodes.first()
        val secondDrained = accumulator.drain(
            cluster = cluster,
            nodes = secondResult.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertEquals(setOf(readyNode.id), secondDrained.keys)

        val batches = secondDrained[readyNode.id]!!

        assertEquals(1, batches.size)
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testSplitAndReenqueue() {
        val now = time.milliseconds()
        val accum = createTestRecordAccumulator(
            batchSize = 1024,
            totalSize = (10 * 1024).toLong(),
            type = CompressionType.GZIP,
            lingerMs = 10,
        )

        // Create a big batch
        val buffer = ByteBuffer.allocate(4096)
        val builder: MemoryRecordsBuilder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        val batch = ProducerBatch(
            topicPartition = tp1,
            recordsBuilder = builder,
            createdMs = now,
            isSplitBatch = true,
        )
        val value = ByteArray(1024)
        val acked = AtomicInteger(0)

        val cb = Callback { _, _ -> acked.incrementAndGet() }
        // Append two messages so the batch is too big.
        val future1 = batch.tryAppend(
            timestamp = now,
            key = null,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callback = cb,
            now = now,
        )
        val future2 = batch.tryAppend(
            timestamp = now,
            key = null,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callback = cb,
            now = now,
        )

        assertNotNull(future1)
        assertNotNull(future2)

        batch.close()
        // Enqueue the batch to the accumulator as if the batch was created by the accumulator.
        accum.reenqueue(batch, now)
        time.sleep(101L)
        // Drain the batch.
        val result = accum.ready(cluster, time.milliseconds())

        assertTrue(result.readyNodes.isNotEmpty(), "The batch should be ready")

        var drained = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertEquals(1, drained.size, "Only node1 should be drained")
        assertEquals(1, drained[node1.id]!!.size, "Only one batch should be drained")

        // Split and reenqueue the batch.
        accum.splitAndReenqueue(drained[node1.id]!![0])
        time.sleep(101L)
        drained = accum.drain(cluster, result.readyNodes, Int.MAX_VALUE, time.milliseconds())

        assertFalse(drained.isEmpty())
        assertFalse(drained[node1.id]!!.isEmpty())

        drained[node1.id]!![0].complete(acked.get().toLong(), 100L)

        assertEquals(1, acked.get(), "The first message should have been acked.")
        assertTrue(future1.isDone)
        assertEquals(0, future1.get()?.offset)

        drained = accum.drain(cluster, result.readyNodes, Int.MAX_VALUE, time.milliseconds())

        assertFalse(drained.isEmpty())
        assertFalse(drained[node1.id]!!.isEmpty())

        drained[node1.id]!![0].complete(acked.get().toLong(), 100L)

        assertEquals(2, acked.get(), "Both message should have been acked.")
        assertTrue(future2.isDone)
        assertEquals(1, future2.get()?.offset)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSplitBatchOffAccumulator() {
        val seed = System.currentTimeMillis()
        val batchSize = 1024
        val bufferCapacity = 3 * 1024

        // First set the compression ratio estimation to be good.
        setEstimation(tp1.topic, CompressionType.GZIP, 0.1f)
        val accum = createTestRecordAccumulator(
            batchSize = batchSize,
            totalSize = bufferCapacity.toLong(),
            type = CompressionType.GZIP,
            lingerMs = 0,
        )
        val numSplitBatches = prepareSplitBatches(
            accum = accum,
            seed = seed,
            recordSize = 100,
            numRecords = 20,
        )

        assertTrue(numSplitBatches > 0, "There should be some split batches")

        // Drain all the split batches.
        val result = accum.ready(cluster, time.milliseconds())
        for (i in 0 until numSplitBatches) {
            val drained = accum.drain(cluster, result.readyNodes, Int.MAX_VALUE, time.milliseconds())
            assertFalse(drained.isEmpty())
            assertFalse(drained[node1.id]!!.isEmpty())
        }
        assertTrue(
            accum.ready(cluster, time.milliseconds()).readyNodes.isEmpty(),
            "All the batches should have been drained."
        )
        assertEquals(
            bufferCapacity.toLong(), accum.bufferPoolAvailableMemory(),
            "The split batches should be allocated off the accumulator"
        )
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSplitFrequency() {
        val seed = System.currentTimeMillis()
        val random = Random(seed)
        val batchSize = 1024
        val numMessages = 1000
        val accum = createTestRecordAccumulator(
            batchSize = batchSize,
            totalSize = (3 * 1024).toLong(),
            type = CompressionType.GZIP,
            lingerMs = 10,
        )
        // Adjust the high and low compression ratio message percentage
        for (goodCompRatioPercentage in 1..99) {
            var numSplit = 0
            var numBatches = 0
            resetEstimation(topic)

            for (i in 0 until numMessages) {
                val dice = random.nextInt(100)
                val value =
                    if (dice < goodCompRatioPercentage) bytesWithGoodCompression(random)
                    else bytesWithPoorCompression(random, 100)

                accum.append(
                    topic = topic,
                    partition = partition1,
                    timestamp = 0L,
                    key = null,
                    value = value,
                    headers = Record.EMPTY_HEADERS,
                    callbacks = null,
                    maxTimeToBlock = 0,
                    abortOnNewBatch = false,
                    nowMs = time.milliseconds(),
                    cluster = cluster,
                )
                val result = completeOrSplitBatches(accum, batchSize)
                numSplit += result.numSplit
                numBatches += result.numBatches
            }
            time.sleep(10)
            val result = completeOrSplitBatches(accum, batchSize)
            numSplit += result.numSplit
            numBatches += result.numBatches

            assertTrue(
                actual = numSplit.toDouble() / numBatches < 0.1f,
                message = "Total num batches = $numBatches, split batches = $numSplit, more than 10% of the " +
                        "batch splits. Random seed is $seed",
            )
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun testSoonToExpireBatchesArePickedUpForExpiry() {
        val lingerMs = 500
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * batchSize).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )
        accum.append(
            topic = topic,
            partition = partition1,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        var readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes
        var drained = accum.drain(
            cluster = cluster,
            nodes = readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertTrue(drained.isEmpty())
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // advanced clock and send one batch out but it should not be included in soon to expire inflight
        // batches because batch's expiry is quite far.
        time.sleep((lingerMs + 1).toLong())
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes
        drained = accum.drain(
            cluster = cluster,
            nodes = readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertEquals(1, drained.size, "A batch did not drain after linger")
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // Queue another batch and advance clock such that batch expiry time is earlier than request timeout.
        accum.append(
            topic = topic,
            partition = partition2,
            timestamp = 0L,
            key = key,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        time.sleep((lingerMs * 4).toLong())

        // Now drain and check that accumulator picked up the drained batch because its expiry is soon.
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes
        drained = accum.drain(cluster, readyNodes, Int.MAX_VALUE, time.milliseconds())

        assertEquals(1, drained.size, "A batch did not drain after linger")
    }

    @Test
    @Throws(InterruptedException::class)
    fun testExpiredBatchesRetry() {
        val lingerMs = 3000
        val rtt = 1000
        val deliveryTimeoutMs = 3200
        var readyNodes: Set<Node?>
        var expiredBatches: List<ProducerBatch?>
        val muteStates = listOf(false, true)

        // test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val accum = createTestRecordAccumulator(
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = (10 * batchSize).toLong(),
            type = CompressionType.NONE,
            lingerMs = lingerMs,
        )

        // Test batches in retry.
        for (mute: Boolean in muteStates) {
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = key,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = 0,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            time.sleep(lingerMs.toLong())
            readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes

            assertEquals(setOf(node1), readyNodes, "Our partition's leader should be ready")

            val drained = accum.drain(
                cluster = cluster,
                nodes = readyNodes,
                maxSize = Int.MAX_VALUE,
                now = time.milliseconds(),
            )

            assertEquals(1, drained[node1.id]!!.size, "There should be only one batch.")

            time.sleep(rtt.toLong())
            accum.reenqueue(drained[node1.id]!![0], time.milliseconds())
            if (mute) accum.mutePartition(tp1) else accum.unmutePartition(tp1)

            // test expiration
            time.sleep((deliveryTimeoutMs - rtt).toLong())
            accum.drain(
                cluster = cluster,
                nodes = setOf(node1),
                maxSize = Int.MAX_VALUE,
                now = time.milliseconds(),
            )
            expiredBatches = accum.expiredBatches(time.milliseconds())

            assertEquals(
                expected = if (mute) 1 else 0,
                actual = expiredBatches.size,
                message = "RecordAccumulator has expired batches if the partition is not muted",
            )
        }
    }

    @Suppress("Deprecation")
    @Test
    @Throws(Exception::class)
    fun testStickyBatches() {
        val now = time.milliseconds()

        // Test case assumes that the records do not fill the batch completely
        val batchSize = 1025
        val partitioner = DefaultPartitioner()
        val accum = createTestRecordAccumulator(
            deliveryTimeoutMs = 3200,
            batchSize = batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            totalSize = 10L * batchSize,
            type = CompressionType.NONE,
            lingerMs = 10,
        )
        val expectedAppends = expectedNumAppendsNoKey(batchSize)

        // Create first batch
        var partition = partitioner.partition(
            topic = topic,
            key = null,
            keyBytes = null,
            value = "value",
            valueBytes = value,
            cluster = cluster,
        )
        accum.append(
            topic = topic,
            partition = partition,
            timestamp = 0L,
            key = null,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        var appends = 1
        var switchPartition = false
        while (!switchPartition) {
            // Append to the first batch
            partition = partitioner.partition(
                topic = topic,
                key = null,
                keyBytes = null,
                value = "value",
                valueBytes = value,
                cluster = cluster,
            )
            val result = accum.append(
                topic = topic,
                partition = partition,
                timestamp = 0L,
                key = null,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = true,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            val partitionBatches1 = accum.getDeque(tp1)
            val partitionBatches2 = accum.getDeque(tp2)
            val partitionBatches3 = accum.getDeque(tp3)
            val numBatches = (partitionBatches1?.size ?: 0) +
                    (partitionBatches2?.size ?: 0) +
                    (partitionBatches3?.size ?: 0)

            // Only one batch is created because the partition is sticky.
            assertEquals(1, numBatches)

            switchPartition = result.abortForNewBatch
            // We only appended if we do not retry.
            if (!switchPartition) {
                appends++
                assertEquals(
                    expected = 0,
                    actual = accum.ready(cluster, now).readyNodes.size,
                    message = "No partitions should be ready.",
                )
            }
        }

        // Batch should be full.
        assertEquals(1, accum.ready(cluster, time.milliseconds()).readyNodes.size)
        assertEquals(appends, expectedAppends)
        switchPartition = false

        // KafkaProducer would call this method in this case, make second batch
        partitioner.onNewBatch(topic, cluster, partition)
        partition = partitioner.partition(topic, null, null, "value", value, cluster)
        accum.append(
            topic = topic,
            partition = partition,
            timestamp = 0L,
            key = null,
            value = value,
            headers = Record.EMPTY_HEADERS,
            callbacks = null,
            maxTimeToBlock = maxBlockTimeMs,
            abortOnNewBatch = false,
            nowMs = time.milliseconds(),
            cluster = cluster,
        )
        appends++

        // These appends all go into the second batch
        while (!switchPartition) {
            partition = partitioner.partition(topic, null, null, "value", value, cluster)
            val result = accum.append(
                topic = topic,
                partition = partition,
                timestamp = 0L,
                key = null,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = true,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            val partitionBatches1 = accum.getDeque(tp1)
            val partitionBatches2 = accum.getDeque(tp2)
            val partitionBatches3 = accum.getDeque(tp3)
            val numBatches = (partitionBatches1?.size ?: 0) +
                    (partitionBatches2?.size ?: 0) +
                    (partitionBatches3?.size ?: 0)

            // Only two batches because the new partition is also sticky.
            assertEquals(2, numBatches)
            switchPartition = result.abortForNewBatch

            // We only appended if we do not retry.
            if (!switchPartition) appends++
        }

        // There should be two full batches now.
        assertEquals(appends, 2 * expectedAppends)
    }

    @Test
    @Throws(Exception::class)
    fun testUniformBuiltInPartitioner() {
        try {
            // Mock random number generator with just sequential integer.
            val mockRandom = AtomicInteger()
            BuiltInPartitioner.mockRandom = Supplier { mockRandom.getAndAdd(1) }
            val totalSize = (1024 * 1024).toLong()
            val batchSize = 1024 // note that this is also a "sticky" limit for the partitioner
            val accum = createTestRecordAccumulator(
                batchSize = batchSize,
                totalSize = totalSize,
                type = CompressionType.NONE,
                lingerMs = 0,
            )

            // Set up callbacks so that we know what partition is chosen.
            val p = AtomicInteger(RecordMetadata.UNKNOWN_PARTITION)
            val callbacks = object : RecordAccumulator.AppendCallbacks {

                override fun setPartition(partition: Int) = p.set(partition)

                override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) = Unit
            }

            // Produce small record, we should switch to first partition.
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            assertEquals(partition1, p.get())
            assertEquals(1, mockRandom.get())

            // Produce large record, we should exceed "sticky" limit, but produce to this partition
            // as we try to switch after the "sticky" limit is exceeded.  The switch is disabled
            // because of incomplete batch.
            val largeValue = ByteArray(batchSize)
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = largeValue,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            assertEquals(partition1, p.get())
            assertEquals(1, mockRandom.get())

            // Produce large record, we should switch to next partition as we complete
            // previous batch and exceeded sticky limit.
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = largeValue,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            assertEquals(partition2, p.get())
            assertEquals(2, mockRandom.get())

            // Produce large record, we should switch to next partition as we complete
            // previous batch and exceeded sticky limit.
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = largeValue,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )

            assertEquals(partition3, p.get())
            assertEquals(3, mockRandom.get())

            // Produce large record, we should switch to next partition as we complete
            // previous batch and exceeded sticky limit.
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = largeValue,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )

            assertEquals(partition1, p.get())
            assertEquals(4, mockRandom.get())
        } finally {
            BuiltInPartitioner.mockRandom = null
        }
    }

    @Test
    @Throws(Exception::class)
    fun testAdaptiveBuiltInPartitioner() {
        try {
            // Mock random number generator with just sequential integer.
            val mockRandom = AtomicInteger()
            BuiltInPartitioner.mockRandom = Supplier { mockRandom.getAndAdd(1) }

            // Create accumulator with partitioner config to enable adaptive partitioning.
            val config = PartitionerConfig(
                enableAdaptivePartitioning = true,
                partitionAvailabilityTimeoutMs = 100,
            )
            val totalSize = (1024 * 1024).toLong()
            val batchSize = 128
            val accum = RecordAccumulator(
                logContext = logContext,
                batchSize = batchSize,
                compression = CompressionType.NONE,
                lingerMs = 0,
                retryBackoffMs = 0L,
                deliveryTimeoutMs = 3200,
                partitionerConfig = config,
                metrics = metrics,
                metricGrpName = "producer-metrics",
                time = time,
                apiVersions = ApiVersions(),
                transactionManager = null,
                free = BufferPool(
                    totalMemory = totalSize,
                    poolableSize = batchSize,
                    metrics = metrics,
                    time = time,
                    metricGrpName = "producer-internal-metrics",
                ),
            )
            val largeValue = ByteArray(batchSize)
            val queueSizes = intArrayOf(1, 7, 2)
            val expectedFrequencies = IntArray(queueSizes.size)
            for (i in queueSizes.indices) {
                expectedFrequencies[i] = 8 - queueSizes[i] // 8 is max(queueSizes) + 1
                var c = queueSizes[i]
                while (c-- > 0) {

                    // Add large records to each partition, so that each record creates a batch.
                    accum.append(
                        topic = topic,
                        partition = i,
                        timestamp = 0L,
                        key = null,
                        value = largeValue,
                        headers = Record.EMPTY_HEADERS,
                        callbacks = null,
                        maxTimeToBlock = maxBlockTimeMs,
                        abortOnNewBatch = false,
                        nowMs = time.milliseconds(),
                        cluster = cluster,
                    )
                }
                assertEquals(queueSizes[i], accum.getDeque(TopicPartition(topic, i))!!.size)
            }

            // Let the accumulator generate the probability tables.
            accum.ready(cluster, time.milliseconds())

            // Set up callbacks so that we know what partition is chosen.
            val p = AtomicInteger(RecordMetadata.UNKNOWN_PARTITION)
            val callbacks = object : RecordAccumulator.AppendCallbacks {

                override fun setPartition(partition: Int) = p.set(partition)

                override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) = Unit
            }

            // Prime built-in partitioner so that it'd switch on every record, as switching only
            // happens after the "sticky" limit is exceeded.
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = largeValue,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )

            // Issue a certain number of partition calls to validate that the partitions would be
            // distributed with frequencies that are reciprocal to the queue sizes.  The number of
            // iterations is defined by the last element of the cumulative frequency table which is
            // the sum of all frequencies.  We do 2 cycles, just so it's more than 1.
            val numberOfCycles = 2
            val numberOfIterations = accum.getBuiltInPartitioner(topic).loadStatsRangeEnd() * numberOfCycles
            val frequencies = IntArray(queueSizes.size)
            for (i in 0 until numberOfIterations) {
                accum.append(
                    topic = topic,
                    partition = RecordMetadata.UNKNOWN_PARTITION,
                    timestamp = 0L,
                    key = null,
                    value = largeValue,
                    headers = Record.EMPTY_HEADERS,
                    callbacks = callbacks,
                    maxTimeToBlock = maxBlockTimeMs,
                    abortOnNewBatch = false,
                    nowMs = time.milliseconds(),
                    cluster = cluster,
                )
                ++frequencies[p.get()]
            }

            // Verify that frequencies are reciprocal of queue sizes.
            for (i in frequencies.indices) assertEquals(
                expected = expectedFrequencies[i] * numberOfCycles,
                actual = frequencies[i],
                message = "Partition " + i + " was chosen " + frequencies[i] + " times",
            )

            // Test that partitions residing on high-latency nodes don't get switched to.
            accum.updateNodeLatencyStats(0, time.milliseconds() - 200, true)
            accum.updateNodeLatencyStats(0, time.milliseconds(), false)
            accum.ready(cluster, time.milliseconds())

            // Do one append, because partition gets switched after append.
            accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0L,
                key = null,
                value = largeValue,
                headers = Record.EMPTY_HEADERS,
                callbacks = callbacks,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster,
            )
            var c = 10
            while (c-- > 0) {
                accum.append(
                    topic = topic,
                    partition = RecordMetadata.UNKNOWN_PARTITION,
                    timestamp = 0L,
                    key = null,
                    value = largeValue,
                    headers = Record.EMPTY_HEADERS,
                    callbacks = callbacks,
                    maxTimeToBlock = maxBlockTimeMs,
                    abortOnNewBatch = false,
                    nowMs = time.milliseconds(),
                    cluster = cluster
                )
                assertEquals(partition3, p.get())
            }
        } finally {
            BuiltInPartitioner.mockRandom = null
        }
    }

    @Test
    @Throws(Exception::class)
    fun testBuiltInPartitionerFractionalBatches() {
        // Test how we avoid creating fractional batches with high linger.ms (see
        // BuiltInPartitioner.updatePartitionInfo).
        val totalSize = (1024 * 1024).toLong()
        val batchSize = 512 // note that this is also a "sticky" limit for the partitioner
        val valSize = 32
        val accum = createTestRecordAccumulator(
            batchSize = batchSize,
            totalSize = totalSize,
            type = CompressionType.NONE,
            lingerMs = 10,
        )
        val value = ByteArray(valSize)
        var c = 10
        while (c-- > 0) {

            // Produce about 2/3 of the batch size.
            var recCount = (batchSize * 2) / 3 / valSize
            while (recCount-- > 0) accum.append(
                topic = topic,
                partition = RecordMetadata.UNKNOWN_PARTITION,
                timestamp = 0,
                key = null,
                value = value,
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = maxBlockTimeMs,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster
            )

            // Advance the time to make the batch ready.
            time.sleep(10)

            // We should have one batch ready.
            val nodes = accum.ready(cluster, time.milliseconds()).readyNodes

            assertEquals(1, nodes.size, "Should have 1 leader ready")

            val batches = accum.drain(
                cluster = cluster,
                nodes = nodes,
                maxSize = Int.MAX_VALUE,
                now = 0,
            ).entries.first().value

            assertEquals(1, batches.size, "Should have 1 batch ready")

            val actualBatchSize = batches[0].records().sizeInBytes()

            assertTrue(actualBatchSize > batchSize / 2, "Batch must be greater than half batch.size")
            assertTrue(actualBatchSize < batchSize, "Batch must be less than batch.size")
        }
    }

    @Throws(InterruptedException::class)
    private fun prepareSplitBatches(accum: RecordAccumulator, seed: Long, recordSize: Int, numRecords: Int): Int {
        val random = Random(seed)

        // First set the compression ratio estimation to be good.
        setEstimation(tp1.topic, CompressionType.GZIP, 0.1f)
        // Append 20 records of 100 bytes size with poor compression ratio should make the batch too big.
        repeat(numRecords) {
            accum.append(
                topic = topic,
                partition = partition1,
                timestamp = 0L,
                key = null,
                value = bytesWithPoorCompression(random, recordSize),
                headers = Record.EMPTY_HEADERS,
                callbacks = null,
                maxTimeToBlock = 0,
                abortOnNewBatch = false,
                nowMs = time.milliseconds(),
                cluster = cluster
            )
        }
        val result = accum.ready(cluster, time.milliseconds())

        assertFalse(result.readyNodes.isEmpty())

        val batches = accum.drain(
            cluster = cluster,
            nodes = result.readyNodes,
            maxSize = Int.MAX_VALUE,
            now = time.milliseconds(),
        )

        assertEquals(1, batches.size)
        assertEquals(1, batches.values.first().size)

        val batch = batches.values.first()[0]
        val numSplitBatches = accum.splitAndReenqueue(batch)
        accum.deallocate(batch)

        return numSplitBatches
    }

    private fun completeOrSplitBatches(accum: RecordAccumulator, batchSize: Int): BatchDrainedResult {
        var numSplit = 0
        var numBatches = 0
        var batchDrained: Boolean
        do {
            batchDrained = false
            val result = accum.ready(cluster, time.milliseconds())
            val batches = accum.drain(
                cluster = cluster,
                nodes = result.readyNodes,
                maxSize = Int.MAX_VALUE,
                now = time.milliseconds(),
            )
            for (batchList in batches.values) {
                for (batch in batchList) {
                    batchDrained = true
                    numBatches++
                    if (batch.estimatedSizeInBytes > batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD) {
                        accum.splitAndReenqueue(batch)
                        // release the resource of the original big batch.
                        numSplit++
                    } else batch.complete(baseOffset = 0L, logAppendTime = 0L)
                    accum.deallocate(batch)
                }
            }
        } while (batchDrained)
        return BatchDrainedResult(numSplit, numBatches)
    }

    /**
     * Generates the compression ratio at about 0.6
     */
    private fun bytesWithGoodCompression(random: Random): ByteArray {
        val value = ByteArray(100)
        val buffer = ByteBuffer.wrap(value)
        while (buffer.remaining() > 0) buffer.putInt(random.nextInt(1000))
        return value
    }

    /**
     * Generates the compression ratio at about 0.9
     */
    private fun bytesWithPoorCompression(random: Random, size: Int): ByteArray {
        val value = ByteArray(size)
        random.nextBytes(value)
        return value
    }

    private data class BatchDrainedResult(val numSplit: Int, val numBatches: Int)

    /**
     * Return the offset delta.
     */
    private fun expectedNumAppends(batchSize: Int): Int {
        var size = 0
        var offsetDelta = 0
        while (true) {
            val recordSize = DefaultRecord.sizeInBytes(
                offsetDelta = offsetDelta,
                timestampDelta = 0,
                keySize = key.size,
                valueSize = value.size,
                headers = Record.EMPTY_HEADERS,
            )
            if (size + recordSize > batchSize) return offsetDelta
            offsetDelta += 1
            size += recordSize
        }
    }

    /**
     * Return the offset delta when there is no key.
     */
    private fun expectedNumAppendsNoKey(batchSize: Int): Int {
        var size = 0
        var offsetDelta = 0
        while (true) {
            val recordSize = DefaultRecord.sizeInBytes(
                offsetDelta = offsetDelta,
                timestampDelta = 0,
                keySize = 0,
                valueSize = value.size,
                headers = Record.EMPTY_HEADERS,
            )
            if (size + recordSize > batchSize) return offsetDelta
            offsetDelta += 1
            size += recordSize
        }
    }

    /**
     * Return a test RecordAccumulator instance
     */
    private fun createTestRecordAccumulator(
        txnManager: TransactionManager? = null,
        deliveryTimeoutMs: Int = 3200,
        batchSize: Int,
        totalSize: Long,
        type: CompressionType,
        lingerMs: Int,
    ): RecordAccumulator {
        val retryBackoffMs = 100L
        val metricGrpName = "producer-metrics"
        return RecordAccumulator(
            logContext = logContext,
            batchSize = batchSize,
            compression = type,
            lingerMs = lingerMs,
            retryBackoffMs = retryBackoffMs,
            deliveryTimeoutMs = deliveryTimeoutMs,
            metrics = metrics,
            metricGrpName = metricGrpName,
            time = time,
            apiVersions = ApiVersions(),
            transactionManager = txnManager,
            free = BufferPool(
                totalMemory = totalSize,
                poolableSize = batchSize,
                metrics = metrics,
                time = time,
                metricGrpName = metricGrpName,
            ),
        )
    }
}
