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

package org.apache.kafka.clients.producer

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.clients.producer.internals.ProduceRequestResult
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.utils.Time
import java.time.Duration
import java.util.*
import java.util.concurrent.Future

/**
 * A mock of the producer interface you can use for testing code that uses Kafka.
 *
 * By default, this mock will synchronously complete each send call successfully. However, it can be
 * configured to allow the user to control the completion of the call and supply an optional error
 * for the producer to throw.
 *
 * @property cluster The cluster holding metadata for this producer
 * @property autoComplete If true automatically complete all requests successfully and execute the
 * callback. Otherwise, the user must call [completeNext] or [errorNext] after [send] to
 * complete the call and unblock the [Future<RecordMetadata>][java.util.concurrent.Future] that
 * is returned.
 * @property partitioner The partition strategy
 * @property keySerializer The serializer for key that implements [Serializer].
 * @property valueSerializer The serializer for value that implements [Serializer].
 */
class MockProducer<K, V>(
    private val cluster: Cluster = Cluster.empty(),
    private val autoComplete: Boolean = false,
    private val partitioner: Partitioner? = null,
    private val keySerializer: Serializer<K>? = null,
    private val valueSerializer: Serializer<V>? = null,
) : Producer<K, V> {

    private val sent = mutableListOf<ProducerRecord<K, V>>()

    private val uncommittedSends = mutableListOf<ProducerRecord<K, V>>()

    private val completions: Deque<Completion> = ArrayDeque()

    private val offsets = mutableMapOf<TopicPartition, Long>()

    private val consumerGroupOffsets =
        mutableListOf<Map<String, MutableMap<TopicPartition, OffsetAndMetadata>>>()

    private var uncommittedConsumerGroupOffsets =
        mutableMapOf<String, MutableMap<TopicPartition, OffsetAndMetadata>>()

    var closed = false
        private set

    var transactionInitialized = false
        private set

    var transactionInFlight = false
        private set

    var transactionCommitted = false
        private set

    var transactionAborted = false
        private set

    private var producerFenced = false

    var sentOffsets = false
        private set

    var commitCount = 0L
        private set

    private val mockMetrics = mutableMapOf<MetricName, Metric>()

    var initTransactionException: RuntimeException? = null

    var beginTransactionException: RuntimeException? = null

    var sendOffsetsToTransactionException: RuntimeException? = null

    var commitTransactionException: RuntimeException? = null

    var abortTransactionException: RuntimeException? = null

    var sendException: RuntimeException? = null

    var flushException: RuntimeException? = null

    var partitionsForException: RuntimeException? = null

    var closeException: RuntimeException? = null

    override fun initTransactions() {
        verifyProducerState()
        check(!transactionInitialized) {
            "MockProducer has already been initialized for transactions."
        }
        initTransactionException?.let { throw it }

        transactionInitialized = true
        transactionInFlight = false
        transactionCommitted = false
        transactionAborted = false
        sentOffsets = false
    }

    @Throws(ProducerFencedException::class)
    override fun beginTransaction() {
        verifyProducerState()
        verifyTransactionsInitialized()
        beginTransactionException?.let { throw it }
        check(!transactionInFlight) { "Transaction already started" }

        transactionInFlight = true
        transactionCommitted = false
        transactionAborted = false
        sentOffsets = false
    }

    @Deprecated("")
    @Throws(ProducerFencedException::class)
    override fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        consumerGroupId: String,
    ) = sendOffsetsToTransaction(offsets, ConsumerGroupMetadata(consumerGroupId))

    @Throws(ProducerFencedException::class)
    override fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata,
    ) {
        verifyProducerState()
        verifyTransactionsInitialized()
        verifyTransactionInFlight()
        sendOffsetsToTransactionException?.let { throw it }
        if (offsets.isEmpty()) return

        val uncommittedOffsets = uncommittedConsumerGroupOffsets
            .computeIfAbsent(groupMetadata.groupId()) { mutableMapOf() }

        uncommittedOffsets.putAll(offsets)
        sentOffsets = true
    }

    @Throws(ProducerFencedException::class)
    override fun commitTransaction() {
        verifyProducerState()
        verifyTransactionsInitialized()
        verifyTransactionInFlight()
        commitTransactionException?.let { throw it }

        flush()
        sent.addAll(uncommittedSends)
        if (uncommittedConsumerGroupOffsets.isNotEmpty())
            consumerGroupOffsets.add(uncommittedConsumerGroupOffsets)

        uncommittedSends.clear()
        uncommittedConsumerGroupOffsets = HashMap()
        transactionCommitted = true
        transactionAborted = false
        transactionInFlight = false
        ++commitCount
    }

    @Throws(ProducerFencedException::class)
    override fun abortTransaction() {
        verifyProducerState()
        verifyTransactionsInitialized()
        verifyTransactionInFlight()
        abortTransactionException?.let { throw it }

        flush()
        uncommittedSends.clear()
        uncommittedConsumerGroupOffsets.clear()
        transactionCommitted = false
        transactionAborted = true
        transactionInFlight = false
    }

    @Synchronized
    private fun verifyProducerState() {
        check(!closed) { "MockProducer is already closed." }
        if (producerFenced) throw ProducerFencedException("MockProducer is fenced.")
    }

    private fun verifyTransactionsInitialized() =
        check(transactionInitialized) { "MockProducer hasn't been initialized for transactions." }

    private fun verifyTransactionInFlight() =
        check(transactionInFlight) { "There is no open transaction." }

    /**
     * Adds the record to the list of sent records. The [RecordMetadata] returned will be
     * immediately satisfied.
     *
     * @see history
     */
    @Synchronized
    override fun send(record: ProducerRecord<K, V>): Future<RecordMetadata> =
        send(record, null)

    /**
     * Adds the record to the list of sent records.
     *
     * @see .history
     */
    @Synchronized
    override fun send(record: ProducerRecord<K, V>, callback: Callback?): Future<RecordMetadata> {
        check(!closed) { "MockProducer is already closed." }
        if (producerFenced)
            throw KafkaException("MockProducer is fenced.", ProducerFencedException("Fenced"))
        sendException?.let { throw it }

        var partition = 0
        if (cluster.partitionsForTopic(record.topic).isNotEmpty())
            partition = partition(record, cluster)
        else {
            //just to throw ClassCastException if serializers are not the proper ones to serialize
            // key/value
            keySerializer!!.serialize(record.topic, record.key)
            valueSerializer!!.serialize(record.topic, record.value)
        }
        val topicPartition = TopicPartition(record.topic, partition)
        val result = ProduceRequestResult(topicPartition)
        val future = FutureRecordMetadata(
            result = result,
            batchIndex = 0,
            createTimestamp = RecordBatch.NO_TIMESTAMP,
            serializedKeySize = 0,
            serializedValueSize = 0,
            time = Time.SYSTEM,
        )
        val offset = nextOffset(topicPartition)
        val baseOffset = (offset - Int.MAX_VALUE).coerceAtLeast(0)
        val batchIndex: Int = offset.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

        val completion = Completion(
            offset = offset,
            metadata = RecordMetadata(
                topicPartition = topicPartition,
                baseOffset = baseOffset,
                batchIndex = batchIndex,
                timestamp = RecordBatch.NO_TIMESTAMP,
                serializedKeySize = 0,
                serializedValueSize = 0,
            ),
            result = result,
            callback = callback,
            tp = topicPartition,
        )

        if (!transactionInFlight) sent.add(record)
        else uncommittedSends.add(record)

        if (autoComplete) completion.complete(null)
        else completions.addLast(completion)

        return future
    }

    /**
     * Get the next offset for this topic/partition
     */
    private fun nextOffset(tp: TopicPartition): Long {
        val offset = offsets[tp]
        return if (offset == null) {
            offsets[tp] = 1L
            0L
        } else {
            val next = offset + 1
            offsets[tp] = next
            offset
        }
    }

    @Synchronized
    override fun flush() {
        verifyProducerState()
        flushException?.let { throw it }
        while (!completions.isEmpty()) completeNext()
    }

    override fun partitionsFor(topic: String): List<PartitionInfo> {
        partitionsForException?.let { throw it }
        return cluster.partitionsForTopic(topic)
    }

    override fun metrics(): Map<MetricName, Metric> = mockMetrics

    /**
     * Set a mock metric for testing purpose
     */
    fun setMockMetrics(name: MetricName, metric: Metric) {
        mockMetrics[name] = metric
    }

    override fun close() = close(Duration.ofMillis(0))

    override fun close(timeout: Duration) {
        closeException?.let { throw it }
        closed = true
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("closed"),
    )
    fun closed(): Boolean = closed

    @Synchronized
    fun fenceProducer() {
        verifyProducerState()
        verifyTransactionsInitialized()
        producerFenced = true
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionInitialized"),
    )
    fun transactionInitialized(): Boolean = transactionInitialized

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionInFlight"),
    )
    fun transactionInFlight(): Boolean = transactionInFlight

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionCommitted"),
    )
    fun transactionCommitted(): Boolean = transactionCommitted

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionAborted"),
    )
    fun transactionAborted(): Boolean = transactionAborted

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("flushed"),
    )
    fun flushed(): Boolean = completions.isEmpty()

    val flushed: Boolean
        get() =  completions.isEmpty()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("sentOffsets"),
    )
    fun sentOffsets(): Boolean = sentOffsets

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("commitCount"),
    )
    fun commitCount(): Long = commitCount

    /**
     * Get the list of sent records since the last call to [clear]
     */
    @Synchronized
    fun history(): List<ProducerRecord<K, V>> = sent.toList()

    @Synchronized
    fun uncommittedRecords(): List<ProducerRecord<K, V>> = uncommittedSends.toList()

    /**
     * Get the list of committed consumer group offsets since the last call to [clear]
     */
    @Synchronized
    fun consumerGroupOffsetsHistory(): List<Map<String, MutableMap<TopicPartition, OffsetAndMetadata>>> =
        consumerGroupOffsets.toList()

    @Synchronized
    fun uncommittedOffsets(): Map<String, MutableMap<TopicPartition, OffsetAndMetadata>> =
        uncommittedConsumerGroupOffsets.toMap()

    /**
     * Clear the stored history of sent records, consumer group offsets
     */
    @Synchronized
    fun clear() {
        sent.clear()
        uncommittedSends.clear()
        sentOffsets = false
        completions.clear()
        consumerGroupOffsets.clear()
        uncommittedConsumerGroupOffsets.clear()
    }

    /**
     * Complete the earliest uncompleted call successfully.
     *
     * @return true if there was an uncompleted call to complete
     */
    @Synchronized
    fun completeNext(): Boolean = errorNext(null)

    /**
     * Complete the earliest uncompleted call with the given error.
     *
     * @return true if there was an uncompleted call to complete
     */
    @Synchronized
    fun errorNext(e: RuntimeException?): Boolean {
        val completion = completions.pollFirst()
        return if (completion != null) {
            completion.complete(e)
            true
        } else false
    }

    /**
     * computes partition for given record.
     */
    private fun partition(record: ProducerRecord<K, V>, cluster: Cluster): Int {
        val partition = record.partition
        val topic = record.topic
        if (partition != null) {
            val partitions = cluster.partitionsForTopic(topic)
            val numPartitions = partitions.size
            // they have given us a partition, use it
            require(!(partition < 0 || partition >= numPartitions)) {
                ("Invalid partition given with record: " + partition
                        + " is not in the range [0..."
                        + numPartitions
                        + "].")
            }
            return partition
        }
        val keyBytes = keySerializer!!.serialize(topic, record.headers, record.key)
        val valueBytes = valueSerializer!!.serialize(topic, record.headers, record.value)

        return partitioner!!.partition(
            topic = topic,
            key = record.key,
            keyBytes = keyBytes,
            value = record.value,
            valueBytes = valueBytes,
            cluster = cluster,
        )
    }

    private class Completion(
        private val offset: Long,
        private val metadata: RecordMetadata,
        private val result: ProduceRequestResult,
        private val callback: Callback?,
        private val tp: TopicPartition,
    ) {
        fun complete(exception: RuntimeException?) {
            if (exception == null) result[offset, RecordBatch.NO_TIMESTAMP] = null
            else result[-1, RecordBatch.NO_TIMESTAMP] = { exception }

            if (callback != null) {
                if (exception == null) callback.onCompletion(metadata, null)
                else callback.onCompletion(
                    metadata = RecordMetadata(
                        topicPartition = tp,
                        baseOffset = -1,
                        batchIndex = -1,
                        timestamp = RecordBatch.NO_TIMESTAMP,
                        serializedKeySize = -1,
                        serializedValueSize = -1,
                    ),
                    exception = exception,
                )
            }
            result.done()
        }
    }
}
