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
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner.StickyPartitionInfo
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.AbstractRecords
import org.apache.kafka.common.record.CompressionRatioEstimator.setEstimation
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.CopyOnWriteMap
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger
import java.nio.ByteBuffer
import java.util.Deque
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

/**
 * This class acts as a queue that accumulates records into [MemoryRecords] instances to be sent to
 * the server.
 *
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is
 * exhausted, unless this behavior is explicitly disabled.
 *
 * Create a new record accumulator
 *
 * @property logContext The log context used for logging
 * @property batchSize The size to use when allocating [MemoryRecords] instances
 * @property compression The compression codec for the records
 * @property lingerMs An artificial delay time to add before declaring a records instance that isn't
 * full ready for sending. This allows time for more records to arrive. Setting a non-zero
 * lingerMs will trade off some latency for potentially better throughput due to more batching (and
 * hence fewer, larger requests).
 * @property retryBackoffMs An artificial delay time to retry the produce request upon receiving an
 * error. This avoids
 * exhausting all retries in a short period of time.
 * @property deliveryTimeoutMs An upper bound on the time to report success or failure on record
 * delivery
 * @param partitionerConfig Partitioner config.
 * @param metrics The metrics
 * @param metricGrpName The metric group name
 * @param time The time instance to use
 * @param apiVersions Request API versions for current connected brokers
 * @param transactionManager The shared transaction state object which tracks producer IDs, epochs,
 * and sequence numbers per partition.
 * @param free The buffer pool
 */
class RecordAccumulator(
    private val logContext: LogContext,
    private val batchSize: Int,
    private val compression: CompressionType,
    private val lingerMs: Int,
    private val retryBackoffMs: Long,
    val deliveryTimeoutMs: Int,
    partitionerConfig: PartitionerConfig = PartitionerConfig(),
    metrics: Metrics,
    metricGrpName: String,
    private val time: Time,
    private val apiVersions: ApiVersions,
    private val transactionManager: TransactionManager?,
    private val free: BufferPool,
) {

    private val log: Logger = logContext.logger(RecordAccumulator::class.java)

    @Volatile
    private var closed = false

    private val flushesInProgress: AtomicInteger = AtomicInteger(0)

    private val appendsInProgress: AtomicInteger = AtomicInteger(0)

    /**
     * Latency threshold for marking partition temporary unavailable
     */
    private val partitionAvailabilityTimeoutMs: Long =
        partitionerConfig.partitionAvailabilityTimeoutMs

    private val enableAdaptivePartitioning: Boolean = partitionerConfig.enableAdaptivePartitioning

    private val topicInfoMap: ConcurrentMap<String, TopicInfo> = CopyOnWriteMap()

    private val nodeStats: ConcurrentMap<Int, NodeLatencyStats> = CopyOnWriteMap()

    private val incomplete: IncompleteBatches = IncompleteBatches()

    /**
     * The following variables are only accessed by the sender thread, so we don't need to protect
     * them.
     */
    private val muted = mutableSetOf<TopicPartition>()

    private val nodesDrainIndex = mutableMapOf<String, Int>()

    /**
     * The earliest time (absolute) a batch will expire.
     */
    private var nextBatchExpiryTimeMs = Long.MAX_VALUE

    init {
        registerMetrics(metrics, metricGrpName)
    }

    private fun registerMetrics(metrics: Metrics, metricGrpName: String) {
        metrics.addMetric(
            metricName = metrics.metricName(
                name = "waiting-threads",
                group = metricGrpName,
                description = "The number of user threads blocked waiting for buffer memory to " +
                        "enqueue their records",
            ),
            metricValueProvider = Measurable { _, _ -> free.queued().toDouble() },
        )
        metrics.addMetric(
            metricName = metrics.metricName(
                name = "buffer-total-bytes",
                group = metricGrpName,
                description = "The maximum amount of buffer memory the client can use (whether " +
                        "or not it is currently used).",
            ),
            metricValueProvider = Measurable { _, _ -> free.totalMemory.toDouble() },
        )
        metrics.addMetric(
            metricName = metrics.metricName(
                name = "buffer-available-bytes",
                group = metricGrpName,
                description = "The total amount of buffer memory that is not being used (either " +
                        "unallocated or in the free list).",
            ),
            metricValueProvider = Measurable { _, _ -> free.availableMemory().toDouble() },
        )
    }

    private fun setPartition(callbacks: AppendCallbacks?, partition: Int) =
        callbacks?.setPartition(partition)

    /**
     * Check if partition concurrently changed, or we need to complete previously disabled partition
     * change.
     *
     * @param topic The topic
     * @param topicInfo The topic info
     * @param partitionInfo The built-in partitioner's partition info
     * @param deque The partition queue
     * @param nowMs The current time, in milliseconds
     * @param cluster THe cluster metadata
     * @return `true` if partition changed, and we need to get new partition info and retry, `false`
     * otherwise
     */
    private fun partitionChanged(
        topic: String,
        topicInfo: TopicInfo,
        partitionInfo: StickyPartitionInfo?,
        deque: Deque<ProducerBatch>,
        nowMs: Long,
        cluster: Cluster,
    ): Boolean {
        if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
            log.trace(
                "Partition {} for topic {} switched by a concurrent append, retrying",
                partitionInfo!!.partition(), topic
            )
            return true
        }

        // We might have disabled partition switch if the queue had incomplete batches.
        // Check if all batches are full now and switch .
        if (allBatchesFull(deque)) {
            topicInfo.builtInPartitioner.updatePartitionInfo(
                partitionInfo = partitionInfo,
                appendedBytes = 0,
                cluster = cluster,
                enableSwitch = true,
            )

            if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
                log.trace(
                    "Completed previously disabled switch for topic {} partition {}, retrying",
                    topic,
                    partitionInfo!!.partition(),
                )
                return true
            }
        }
        return false
    }

    /**
     * Add a record to the accumulator, return the append result
     *
     * The append result will contain the future metadata, and flag for whether the appended batch
     * is full or a new batch is created.
     *
     * @param topic The topic to which this record is being sent
     * @param partition The partition to which this record is being sent or
     * RecordMetadata.UNKNOWN_PARTITION if any partition could be used
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callbacks The callbacks to execute
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be
     * available
     * @param abortOnNewBatch A boolean that indicates returning before a new batch is created and
     * running the partitioner's onNewBatch method before trying to append again
     * @param nowMs The current time, in milliseconds
     * @param cluster The cluster metadata
     */
    @Throws(InterruptedException::class)
    fun append(
        topic: String,
        partition: Int,
        timestamp: Long,
        key: ByteArray?,
        value: ByteArray?,
        headers: Array<Header>?,
        callbacks: AppendCallbacks?,
        maxTimeToBlock: Long,
        abortOnNewBatch: Boolean,
        nowMs: Long,
        cluster: Cluster,
    ): RecordAppendResult {
        var nowMillis = nowMs
        val topicInfo = topicInfoMap.computeIfAbsent(topic) { t ->
            TopicInfo(
                logContext = logContext,
                topic = t,
                stickyBatchSize = batchSize
            )
        }

        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet()
        var buffer: ByteBuffer? = null
        val actualHeaders = headers ?: Record.EMPTY_HEADERS
        try {
            // Loop to retry in case we encounter partitioner's race conditions.
            while (true) {
                // If the message doesn't have any partition affinity, so we pick a partition based
                // on the broker availability and performance. Note, that here we peek current
                // partition before we hold the deque lock, so we'll need to make sure that it's not
                // changed while we were waiting for the deque lock.
                val partitionInfo: StickyPartitionInfo?
                val effectivePartition: Int
                if (partition == RecordMetadata.UNKNOWN_PARTITION) {
                    partitionInfo = topicInfo.builtInPartitioner.peekCurrentPartitionInfo(cluster)
                    effectivePartition = partitionInfo!!.partition()
                } else {
                    partitionInfo = null
                    effectivePartition = partition
                }

                // Now that we know the effective partition, let the caller know.
                setPartition(callbacks, effectivePartition)

                // check if we have an in-progress batch
                val dq = topicInfo.batches.computeIfAbsent(effectivePartition) { ArrayDeque() }

                // Current Kotlin workaround for break and continue inside lambdas
                // See https://youtrack.jetbrains.com/issue/KT-1436/Support-non-local-break-and-continue
                var continueFlag = false
                synchronized(dq) {

                    // After taking the lock, validate that the partition hasn't changed and retry.
                    if (
                        partitionChanged(
                            topic = topic,
                            topicInfo = topicInfo,
                            partitionInfo = partitionInfo,
                            deque = dq,
                            nowMs = nowMillis,
                            cluster = cluster,
                        )
                    ) {
                        continueFlag = true
                        return@synchronized
                    }

                    val appendResult = tryAppend(
                        timestamp = timestamp,
                        key = key,
                        value = value,
                        headers = actualHeaders,
                        callback = callbacks,
                        deque = dq,
                        nowMs = nowMillis,
                    )
                    if (appendResult != null) {
                        // If queue has incomplete batches we disable switch (see comments in
                        // updatePartitionInfo).
                        val enableSwitch: Boolean = allBatchesFull(dq)
                        topicInfo.builtInPartitioner.updatePartitionInfo(
                            partitionInfo = partitionInfo,
                            appendedBytes = appendResult.appendedBytes,
                            cluster = cluster,
                            enableSwitch = enableSwitch,
                        )
                        return appendResult
                    }
                }

                if (continueFlag) continue

                // we don't have an in-progress record batch try to allocate a new batch
                if (abortOnNewBatch) {
                    // Return a result that will cause another call to append.
                    return RecordAppendResult(
                        future = null,
                        batchIsFull = false,
                        newBatchCreated = false,
                        abortForNewBatch = true,
                        appendedBytes = 0,
                    )
                }
                if (buffer == null) {
                    val maxUsableMagic = apiVersions.maxUsableProduceMagic()
                    val size = AbstractRecords.estimateSizeInBytesUpperBound(
                        magic = maxUsableMagic,
                        compressionType = compression,
                        key = key,
                        value = value,
                        headers = actualHeaders,
                    ).coerceAtLeast(batchSize)

                    log.trace(
                        "Allocating a new {} byte message buffer for topic {} partition {} with " +
                                "remaining timeout {}ms",
                        size,
                        topic,
                        partition,
                        maxTimeToBlock,
                    )

                    // This call may block if we exhausted buffer space.
                    buffer = free.allocate(size, maxTimeToBlock)

                    // Update the current time in case the buffer allocation blocked above.
                    // NOTE: getting time may be expensive, so calling it under a lock
                    // should be avoided.
                    nowMillis = time.milliseconds()
                }

                synchronized(dq) {

                    // After taking the lock, validate that the partition hasn't changed and retry.
                    if (
                        partitionChanged(
                            topic = topic,
                            topicInfo = topicInfo,
                            partitionInfo = partitionInfo,
                            deque = dq,
                            nowMs = nowMillis,
                            cluster = cluster,
                        )
                    ) return@synchronized

                    val appendResult = appendNewBatch(
                        topic = topic,
                        partition = effectivePartition,
                        dq = dq,
                        timestamp = timestamp,
                        key = key,
                        value = value,
                        headers = actualHeaders,
                        callbacks = callbacks,
                        buffer = buffer!!,
                        nowMs = nowMillis,
                    )

                    // Set buffer to null, so that deallocate doesn't return it back to free pool,
                    // since it's used in the batch.
                    if (appendResult.newBatchCreated) buffer = null

                    // If queue has incomplete batches we disable switch (see comments in
                    // updatePartitionInfo).
                    val enableSwitch: Boolean = allBatchesFull(dq)
                    topicInfo.builtInPartitioner.updatePartitionInfo(
                        partitionInfo = partitionInfo,
                        appendedBytes = appendResult.appendedBytes,
                        cluster = cluster,
                        enableSwitch = enableSwitch,
                    )
                    return appendResult
                }
            }
        } finally {
            free.deallocate(buffer)
            appendsInProgress.decrementAndGet()
        }
    }

    /**
     * Append a new batch to the queue
     *
     * @param topic The topic
     * @param partition The partition (cannot be RecordMetadata.UNKNOWN_PARTITION)
     * @param dq The queue
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callbacks The callbacks to execute
     * @param buffer The buffer for the new batch
     * @param nowMs The current time, in milliseconds
     */
    private fun appendNewBatch(
        topic: String,
        partition: Int,
        dq: Deque<ProducerBatch>,
        timestamp: Long,
        key: ByteArray?,
        value: ByteArray?,
        headers: Array<Header>,
        callbacks: AppendCallbacks?,
        buffer: ByteBuffer,
        nowMs: Long,
    ): RecordAppendResult {
        assert(partition != RecordMetadata.UNKNOWN_PARTITION)
        val appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs)
        if (appendResult != null) {
            // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't
            // happen often...
            return appendResult
        }

        val recordsBuilder = recordsBuilder(buffer, apiVersions.maxUsableProduceMagic())
        val batch = ProducerBatch(TopicPartition(topic, partition), recordsBuilder, nowMs)
        val future = batch.tryAppend(
            timestamp = timestamp,
            key = key,
            value = value,
            headers = headers,
            callback = callbacks,
            now = nowMs,
        )!!
        dq.addLast(batch)
        incomplete.add(batch)
        return RecordAppendResult(
            future = future,
            batchIsFull = dq.size > 1 || batch.isFull,
            newBatchCreated = true,
            abortForNewBatch = false,
            appendedBytes = batch.estimatedSizeInBytes,
        )
    }

    private fun recordsBuilder(buffer: ByteBuffer, maxUsableMagic: Byte): MemoryRecordsBuilder {
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw UnsupportedVersionException(
                "Attempting to use idempotence with a broker which does not support the required " +
                        "message format (v2). The broker must be version 0.11 or later."
            )
        }
        return MemoryRecords.builder(
            buffer = buffer,
            magic = maxUsableMagic,
            compressionType = compression,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
    }

    /**
     * Check if all batches in the queue are full.
     */
    private fun allBatchesFull(deque: Deque<ProducerBatch>): Boolean {
        // Only the last batch may be incomplete, so we just check that.
        val last = deque.peekLast()
        return last == null || last.isFull
    }

    /**
     * Try to append to a ProducerBatch.
     *
     * If it is full, we return null and a new batch is created. We also close the batch for record
     * appends to free up resources like compression buffers. The batch will be fully closed (ie.
     * the record batch headers will be written and memory records built) in one of the following
     * cases (whichever comes first): right before send, if it is expired, or when the producer is
     * closed.
     */
    private fun tryAppend(
        timestamp: Long,
        key: ByteArray?,
        value: ByteArray?,
        headers: Array<Header>,
        callback: Callback?,
        deque: Deque<ProducerBatch>,
        nowMs: Long,
    ): RecordAppendResult? {
        if (closed) throw KafkaException("Producer closed while send in progress")
        val last = deque.peekLast() ?: return null

        val initialBytes = last.estimatedSizeInBytes
        val future = last.tryAppend(
            timestamp = timestamp,
            key = key,
            value = value,
            headers = headers,
            callback = callback,
            now = nowMs,
        )

        if (future == null) last.closeForRecordAppends()
        else {
            val appendedBytes = last.estimatedSizeInBytes - initialBytes
            return RecordAppendResult(
                future = future,
                batchIsFull = deque.size > 1 || last.isFull,
                newBatchCreated = false,
                abortForNewBatch = false,
                appendedBytes = appendedBytes,
            )
        }
        return null
    }

    private fun isMuted(tp: TopicPartition): Boolean = muted.contains(tp)

    fun resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE
    }

    fun maybeUpdateNextBatchExpiryTime(batch: ProducerBatch) {
        if (batch.createdMs + deliveryTimeoutMs > 0) {
            // the non-negative check is to guard us against potential overflow due to setting
            // a large value for deliveryTimeoutMs
            nextBatchExpiryTimeMs = min(
                nextBatchExpiryTimeMs,
                batch.createdMs + deliveryTimeoutMs,
            )
        } else log.warn(
            "Skipping next batch expiry time update due to addition overflow: " +
                    "batch.createMs={}, deliveryTimeoutMs={}",
            batch.createdMs,
            deliveryTimeoutMs,
        )
    }

    /**
     * Get a list of batches which have been sitting in the accumulator too long and need to be
     * expired.
     */
    fun expiredBatches(now: Long): List<ProducerBatch> {
        val expiredBatches = mutableListOf<ProducerBatch>()
        for (topicInfo in topicInfoMap.values) {
            for (deque in topicInfo.batches.values) {
                // expire the batches in the order of sending
                synchronized(deque) {
                    while (!deque.isEmpty()) {
                        val batch = deque.first
                        if (batch!!.hasReachedDeliveryTimeout(deliveryTimeoutMs.toLong(), now)) {
                            deque.poll()
                            batch.abortRecordAppends()
                            expiredBatches.add(batch)
                        } else {
                            maybeUpdateNextBatchExpiryTime(batch)
                            break
                        }
                    }
                }
            }
        }
        return expiredBatches
    }

    fun getDeliveryTimeoutMs(): Long = deliveryTimeoutMs.toLong()

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we
     * check whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery
     * timeout check here.
     */
    fun reenqueue(batch: ProducerBatch, now: Long) {
        batch.reenqueued(now)
        val deque = getOrCreateDeque(batch.topicPartition)
        synchronized(deque) {
            if (transactionManager != null) insertInSequenceOrder(deque, batch)
            else deque.addFirst(batch)
        }
    }

    /**
     * Split the big batch that has been rejected and reenqueue the split batches in to the
     * accumulator.
     *
     * @return the number of split batches.
     */
    fun splitAndReenqueue(bigBatch: ProducerBatch): Int {
        // Reset the estimated compression ratio to the initial value or the big batch compression
        // ratio, whichever is bigger. There are several different ways to do the reset. We chose
        // the most conservative one to ensure the split doesn't happen too often.
        setEstimation(
            topic = bigBatch.topicPartition.topic,
            type = compression,
            ratio = bigBatch.compressionRatio.coerceAtLeast(1.0).toFloat()
        )
        val dq = bigBatch.split(batchSize)
        val numSplitBatches = dq.size
        val partitionDequeue = getOrCreateDeque(bigBatch.topicPartition)

        while (!dq.isEmpty()) {
            val batch = dq.pollLast()!!
            incomplete.add(batch)
            // We treat the newly split batches as if they are not even tried.
            synchronized(partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned
                    // sequences.
                    transactionManager.addInFlightBatch((batch))
                    insertInSequenceOrder(partitionDequeue, batch)
                } else partitionDequeue.addFirst(batch)
            }
        }
        return numSplitBatches
    }

    // We will have to do extra work to ensure the queue is in order when requests are being retried
    // and there are multiple requests in flight to that partition. If the first in flight request
    // fails to append, then all the subsequent in flight requests will also fail because the
    // sequence numbers will not be accepted.
    //
    // Further, once batches are being retried, we are reduced to a single in flight request for
    // that partition. So when the subsequent batches come back in sequence order, they will have to
    // be placed further back in the queue.
    //
    // Note that this assumes that all the batches in the queue which have an assigned sequence also
    // have the current producer id. We will not attempt to reorder messages if the producer id has
    // changed, we will throw an IllegalStateException instead.
    private fun insertInSequenceOrder(deque: Deque<ProducerBatch>, batch: ProducerBatch) {
        // When we are re-enqueueing and have enabled idempotence, the re-enqueued batch must always
        // have a sequence.
        if (batch.baseSequence == RecordBatch.NO_SEQUENCE) throw IllegalStateException(
            "Trying to re-enqueue a batch which doesn't have a sequence even " +
                    "though idempotency is enabled."
        )
        check(transactionManager!!.hasInflightBatches(batch.topicPartition)) {
            "We are re-enqueueing a batch which is not tracked as part of the in flight " +
                    "requests. batch.topicPartition: ${batch.topicPartition}; " +
                    "batch.baseSequence: ${batch.baseSequence}"
        }

        val firstBatchInQueue = deque.peekFirst()
        if (
            firstBatchInQueue != null
            && firstBatchInQueue.hasSequence
            && firstBatchInQueue.baseSequence < batch.baseSequence
        ) {
            // The incoming batch can't be inserted at the front of the queue without violating the
            // sequence ordering. This means that the incoming batch should be placed somewhere
            // further back. We need to find the right place for the incoming batch and insert it
            // there. We will only enter this branch if we have multiple inflights sent to different
            // brokers and we need to retry the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by
            // sequence always, it is a simple linear scan of a subset of the in flight batches to
            // find the right place in the queue each time.
            val orderedBatches: MutableList<ProducerBatch?> = ArrayList()

            while (
                deque.peekFirst() != null
                && deque.peekFirst()!!.hasSequence
                && deque.peekFirst()!!.baseSequence < batch.baseSequence
            ) orderedBatches.add(deque.pollFirst())

            log.debug(
                "Reordered incoming batch with sequence {} for partition {}. It was placed in " +
                        "the queue at position {}",
                batch.baseSequence,
                batch.topicPartition,
                orderedBatches.size
            )
            // Either we have reached a point where there are batches without a sequence (ie. never
            // been drained and are hence in order by default), or the batch at the front of the
            // queue has a sequence greater than the incoming batch. This is the right place to add
            // the incoming batch.
            deque.addFirst(batch)

            // Now we have to re-insert the previously queued batches in the right order.
            for (i in orderedBatches.indices.reversed()) deque.addFirst(orderedBatches[i])

            // At this point, the incoming batch has been queued in the correct place according to
            // its sequence.
        } else deque.addFirst(batch)
    }

    /**
     * Add the leader to the ready nodes if the batch is ready
     *
     * @param nowMs The current time
     * @param exhausted 'true' is the buffer pool is exhausted
     * @param part The partition
     * @param leader The leader for the partition
     * @param waitedTimeMs How long batch waited
     * @param backingOff Is backing off
     * @param full Is batch full
     * @param nextReadyCheckDelayMs The delay for next check
     * @param readyNodes The set of ready nodes (to be filled in)
     * @return The delay for next check
     */
    private fun batchReady(
        nowMs: Long,
        exhausted: Boolean,
        part: TopicPartition,
        leader: Node,
        waitedTimeMs: Long,
        backingOff: Boolean,
        full: Boolean,
        nextReadyCheckDelayMs: Long,
        readyNodes: MutableSet<Node>,
    ): Long {
        var nextReadyCheckDelayMs = nextReadyCheckDelayMs
        if (!readyNodes.contains(leader) && !isMuted(part)) {
            val timeToWaitMs = if (backingOff) retryBackoffMs else lingerMs.toLong()
            val expired = waitedTimeMs >= timeToWaitMs
            val transactionCompleting = transactionManager?.isCompleting == true
            val sendable = (full
                    || expired
                    || exhausted
                    || closed
                    || flushInProgress()
                    || transactionCompleting)

            if (sendable && !backingOff) readyNodes.add(leader)
            else {
                val timeLeftMs = (timeToWaitMs - waitedTimeMs).coerceAtLeast(0)
                // Note that this results in a conservative estimate since an un-sendable partition
                // may have a leader that will later be found to have sendable data. However, this
                // is good enough since we'll just wake up and then sleep again for the remaining
                // time.
                nextReadyCheckDelayMs = min(timeLeftMs, nextReadyCheckDelayMs)
            }
        }
        return nextReadyCheckDelayMs
    }

    /**
     * Iterate over partitions to see which one have batches ready and collect leaders of those
     * partitions into the set of ready nodes. If partition has no leader, add the topic to the set
     * of topics with no leader. This function also calculates stats for adaptive partitioning.
     *
     * @param cluster The cluster metadata
     * @param nowMs The current time
     * @param topic The topic
     * @param topicInfo The topic info
     * @param nextReadyCheckDelayMs The delay for next check
     * @param readyNodes The set of ready nodes (to be filled in)
     * @param unknownLeaderTopics The set of topics with no leader (to be filled in)
     * @return The delay for next check
     */
    private fun partitionReady(
        cluster: Cluster,
        nowMs: Long,
        topic: String,
        topicInfo: TopicInfo,
        nextReadyCheckDelayMs: Long,
        readyNodes: MutableSet<Node>,
        unknownLeaderTopics: MutableSet<String>,
    ): Long {
        var nextReadyCheckDelayMs = nextReadyCheckDelayMs
        val batches = topicInfo.batches
        // Collect the queue sizes for available partitions to be used in adaptive partitioning.
        var queueSizes: IntArray? = null
        var partitionIds: IntArray? = null
        if (enableAdaptivePartitioning && batches.size >= cluster.partitionsForTopic(topic).size) {
            // We don't do adaptive partitioning until we scheduled at least a batch for all
            // partitions (i.e. we have the corresponding entries in the batches map), we just do
            // uniform. The reason is that we build queue sizes from the batches map, and if an
            // entry is missing in the batches map, then adaptive partitioning logic won't know
            // about it and won't switch to it.
            queueSizes = IntArray(batches.size)
            partitionIds = IntArray(queueSizes.size)
        }
        var queueSizesIndex = -1
        val exhausted = free.queued() > 0
        for ((partition, deque) in batches) {
            val part = TopicPartition(topic, partition)
            // Advance queueSizesIndex so that we properly index available
            // partitions. Do it here so that it's done for all code paths.
            val leader = cluster.leaderFor(part)
            if (leader != null && queueSizes != null) {
                ++queueSizesIndex
                assert(queueSizesIndex < queueSizes.size)
                partitionIds!![queueSizesIndex] = part.partition
            }

            // Current Kotlin workaround for break and continue inside lambdas
            // See https://youtrack.jetbrains.com/issue/KT-1436/Support-non-local-break-and-continue
            var continueFlag = false

            // nullable due to the workaround, safe to use after workaround ends
            var waitedTimeMs: Long? = null
            var backingOff: Boolean? = null
            var dequeSize: Int? = null
            var full: Boolean? = null

            // This loop is especially hot with large partition counts.

            // We are careful to only perform the minimum required inside the synchronized block, as
            // this lock is also used to synchronize producer threads attempting to append() to a
            // partition/batch.

            synchronized(deque) {

                // Deques are often empty in this path, esp with large partition counts,
                // so we exit early if we can.
                val batch: ProducerBatch = deque.peekFirst() ?: run {
                    continueFlag = true
                    return@synchronized
                }
                waitedTimeMs = batch.waitedTimeMs(nowMs)
                backingOff = batch.attempts() > 0 && waitedTimeMs!! < retryBackoffMs
                dequeSize = deque.size
                full = dequeSize!! > 1 || batch.isFull
            }
            if (continueFlag) continue

            if (leader == null) {
                // This is a partition for which leader is not known, but messages are available to
                // send. Note that entries are currently not removed from batches when deque is
                // empty.
                unknownLeaderTopics.add(part.topic)
            } else {
                if (queueSizes != null) queueSizes[queueSizesIndex] = dequeSize!!
                if (partitionAvailabilityTimeoutMs > 0) {
                    // Check if we want to exclude the partition from the list of available
                    // partitions if the broker hasn't responded for some time.
                    val nodeLatencyStats = nodeStats[leader.id]
                    if (nodeLatencyStats != null) {
                        // NOTE: there is no synchronization between reading metrics, so we read
                        // ready time first to avoid accidentally marking partition unavailable if
                        // we read while the metrics are being updated.
                        val readyTimeMs = nodeLatencyStats.readyTimeMs
                        if (readyTimeMs - nodeLatencyStats.drainTimeMs > partitionAvailabilityTimeoutMs)
                            --queueSizesIndex
                    }
                }
                nextReadyCheckDelayMs = batchReady(
                    nowMs = nowMs,
                    exhausted = exhausted,
                    part = part,
                    leader = leader,
                    waitedTimeMs = waitedTimeMs!!,
                    backingOff = backingOff!!,
                    full = full!!,
                    nextReadyCheckDelayMs = nextReadyCheckDelayMs,
                    readyNodes = readyNodes
                )
            }
        }

        // We've collected the queue sizes for partitions of this topic, now we can calculate load
        // stats. NOTE: the stats are calculated in place, modifying the queueSizes array.
        topicInfo.builtInPartitioner.updatePartitionLoadStats(
            queueSizes = queueSizes,
            partitionIds = partitionIds,
            length = queueSizesIndex + 1,
        )
        return nextReadyCheckDelayMs
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any
     * non-sendable partition will be ready; Also return the flag for whether there are any unknown
     * leaders for the accumulated partition batches.
     *
     * A destination node is ready to send data if:
     *
     * 1. There is at least one partition that is not backing off its send
     * 2. **and** those partitions are not muted (to prevent reordering if
     * [org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION]
     * is set to one)
     * 3. **and *any*** of the following are true
     *
     * - The record set is full
     * - The record set has sat in the accumulator for at least lingerMs milliseconds
     * - The accumulator is out of memory and threads are blocking waiting for data (in this case
     *   all partitions are immediately considered ready).
     * - The accumulator has been closed
     */
    fun ready(cluster: Cluster, nowMs: Long): ReadyCheckResult {
        val readyNodes = mutableSetOf<Node>()
        var nextReadyCheckDelayMs = Long.MAX_VALUE
        val unknownLeaderTopics = mutableSetOf<String>()

        // Go topic by topic so that we can get queue sizes for partitions in a topic and calculate
        // cumulative frequency table (used in partitioner).
        for ((topic, topicInfo) in topicInfoMap) {
            nextReadyCheckDelayMs = partitionReady(
                cluster = cluster,
                nowMs = nowMs,
                topic = topic,
                topicInfo = topicInfo,
                nextReadyCheckDelayMs = nextReadyCheckDelayMs,
                readyNodes = readyNodes,
                unknownLeaderTopics = unknownLeaderTopics,
            )
        }
        return ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics)
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    fun hasUndrained(): Boolean {
        for (topicInfo in topicInfoMap.values) {
            for (deque in topicInfo.batches.values) synchronized(deque) {
                if (!deque.isEmpty()) return true
            }
        }
        return false
    }

    private fun shouldStopDrainBatchesForPartition(
        first: ProducerBatch,
        tp: TopicPartition,
    ): Boolean {
        val producerIdAndEpoch: ProducerIdAndEpoch?
        if (transactionManager != null) {
            if (!transactionManager.isSendToPartitionAllowed(tp)) return true
            producerIdAndEpoch = transactionManager.producerIdAndEpoch
            if (!producerIdAndEpoch.isValid)
            // we cannot send the batch until we have refreshed the producer id
                return true
            if (!first.hasSequence) {
                if (
                    transactionManager.hasInflightBatches(tp)
                    && transactionManager.hasStaleProducerIdAndEpoch(tp)
                ) {
                    // Don't drain any new batches while the partition has in-flight batches with a
                    // different epoch and/or producer ID. Otherwise, a batch with a new epoch and
                    // sequence number 0 could be written before earlier batches complete, which
                    // would cause out of sequence errors
                    return true
                }
                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                // Don't drain any new batches while the state of previous sequence numbers is
                // unknown. The previous batches would be unknown if they were aborted on the
                // client after being sent to the broker at least once.
                    return true
            }
            val firstInFlightSequence =
                transactionManager.firstInFlightSequence(first.topicPartition)
            if (
                firstInFlightSequence != RecordBatch.NO_SEQUENCE
                && first.hasSequence
                && first.baseSequence != firstInFlightSequence
            )
            // If the queued batch already has an assigned sequence, then it is being retried.
            // In this case, we wait until the next immediate batch is ready and drain that.
            // We only move on when the next in line batch is complete (either successfully or
            // due to a fatal broker error). This effectively reduces our in flight request
            // count to 1.
                return true
        }
        return false
    }

    private fun drainBatchesForOneNode(
        cluster: Cluster,
        node: Node,
        maxSize: Int,
        now: Long,
    ): List<ProducerBatch> {
        var size = 0
        val parts = cluster.partitionsForNode(node.id)
        val ready: MutableList<ProducerBatch> = ArrayList()
        // to make starvation less likely each node has its own drainIndex
        var drainIndex = getDrainIndex(node.idString())
        drainIndex %= parts.size
        val start = drainIndex
        do {
            val part = parts[drainIndex]
            val tp = TopicPartition(part.topic, part.partition)
            updateDrainIndex(node.idString(), drainIndex)
            drainIndex = (drainIndex + 1) % parts.size
            // Only proceed if the partition has no in-flight batches.
            if (isMuted(tp)) continue
            val deque = getDeque(tp) ?: continue
            lateinit var batch: ProducerBatch

            // Current Kotlin workaround for break and continue inside lambdas
            // See https://youtrack.jetbrains.com/issue/KT-1436/Support-non-local-break-and-continue
            var continueFlag = false
            var breakFlag = false
            synchronized(deque) {

                // invariant: !isMuted(tp,now) && deque != null
                val first = deque.peekFirst() ?: run {
                    continueFlag = true
                    return@synchronized
                }

                // first != null
                val backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs
                // Only drain the batch if it is not during backoff period.
                if (backoff) {
                    continueFlag = true
                    return@synchronized
                }
                if (size + first.estimatedSizeInBytes > maxSize && ready.isNotEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size
                    // due to compression; in this case we will still eventually send this batch in
                    // a single request
                    breakFlag = true
                    return@synchronized
                } else if (shouldStopDrainBatchesForPartition(first, tp)) {
                    breakFlag = true
                    return@synchronized
                }

                batch = deque.pollFirst()!!

                val isTransactional = transactionManager?.isTransactional == true
                val producerIdAndEpoch = transactionManager?.producerIdAndEpoch

                if (producerIdAndEpoch != null && !batch.hasSequence) {
                    // If the producer id/epoch of the partition do not match the latest one
                    // of the producer, we update it and reset the sequence. This should be
                    // only done when all its in-flight batches have completed. This is guarantee
                    // in `shouldStopDrainBatchesForPartition`.
                    transactionManager!!.maybeUpdateProducerIdAndEpoch(batch.topicPartition)

                    // If the batch already has an assigned sequence, then we should not change the
                    // producer id and sequence number, since this may introduce duplicates. In
                    // particular, the previous attempt may actually have been accepted, and if we
                    // change the producer id and sequence here, this attempt will also be accepted,
                    // causing a duplicate.
                    //
                    // Additionally, we update the next sequence number bound for the partition, and
                    // also have the transaction manager track the batch so as to ensure that
                    // sequence ordering is maintained even if we receive out of order responses.
                    batch.setProducerState(
                        producerIdAndEpoch = producerIdAndEpoch,
                        baseSequence = transactionManager.sequenceNumber(batch.topicPartition),
                        isTransactional = isTransactional,
                    )
                    transactionManager.incrementSequenceNumber(
                        topicPartition = batch.topicPartition,
                        increment = batch.recordCount,
                    )
                    log.debug(
                        "Assigned producerId {} and producerEpoch {} to batch with base sequence " +
                                "{} being sent to partition {}",
                        producerIdAndEpoch.producerId,
                        producerIdAndEpoch.epoch,
                        batch.baseSequence,
                        tp,
                    )
                    transactionManager.addInFlightBatch(batch)
                }
            }

            if (continueFlag) continue
            if (breakFlag) break

            // the rest of the work by processing outside the lock
            // close() is particularly expensive
            batch.close()
            size += batch.records().sizeInBytes()
            ready.add(batch)
            batch.drained(now)
        } while (start != drainIndex)
        return ready
    }

    private fun getDrainIndex(idString: String): Int =
        nodesDrainIndex.computeIfAbsent(idString) { 0 }

    private fun updateDrainIndex(idString: String, drainIndex: Int) {
        nodesDrainIndex[idString] = drainIndex
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit
     * within the specified size on a per-node basis. This method attempts to avoid choosing the
     * same topic-node over and over.
     *
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of [ProducerBatch] for each node specified with total size less than the
     * requested maxSize.
     */
    fun drain(
        cluster: Cluster,
        nodes: Set<Node>,
        maxSize: Int,
        now: Long,
    ): Map<Int, List<ProducerBatch>> {
        if (nodes.isEmpty()) return emptyMap()
        return nodes.associate { node ->
            node.id to drainBatchesForOneNode(cluster, node, maxSize, now)
        }
    }

    fun updateNodeLatencyStats(nodeId: Int, nowMs: Long, canDrain: Boolean) {
        // Don't bother with updating stats if the feature is turned off.
        if (partitionAvailabilityTimeoutMs <= 0) return

        // When the sender gets a node (returned by the ready() function) that has data to send but
        // the node is not ready (and so we cannot drain the data), we only update the ready time,
        // then the difference would reflect for how long a node wasn't ready to send the data. Then
        // we can temporarily remove partitions that are handled by the node from the list of
        // available partitions so that the partitioner wouldn't pick this partition.
        //
        // NOTE: there is no synchronization for metric updates, so drainTimeMs is updated first to
        // avoid accidentally marking a partition unavailable if the reader gets values between
        // updates.
        val nodeLatencyStats = nodeStats.computeIfAbsent(nodeId) { NodeLatencyStats(nowMs) }
        if (canDrain) nodeLatencyStats.drainTimeMs = nowMs
        nodeLatencyStats.readyTimeMs = nowMs
    }

    /* Visible for testing */
    fun getNodeLatencyStats(nodeId: Int): NodeLatencyStats? = nodeStats[nodeId]

    /* Visible for testing */
    fun getBuiltInPartitioner(topic: String): BuiltInPartitioner =
        topicInfoMap[topic]!!.builtInPartitioner

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    fun nextExpiryTimeMs(): Long = nextBatchExpiryTimeMs

    /* Visible for testing */
    fun getDeque(tp: TopicPartition): Deque<ProducerBatch>? =
        topicInfoMap[tp.topic]?.batches?.get(tp.partition)

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private fun getOrCreateDeque(tp: TopicPartition): Deque<ProducerBatch> {
        val topicInfo = topicInfoMap.computeIfAbsent(tp.topic) { t ->
            TopicInfo(
                logContext = logContext,
                topic = t,
                stickyBatchSize = batchSize,
            )
        }
        return topicInfo.batches.computeIfAbsent(tp.partition) { ArrayDeque() }
    }

    /**
     * Deallocate the record batch
     */
    fun deallocate(batch: ProducerBatch) {
        incomplete.remove(batch)
        // Only deallocate the batch if it is not a split batch because split batch are allocated
        // outside the buffer pool.
        if (!batch.isSplitBatch) free.deallocate(batch.buffer(), batch.initialCapacity())
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     */
    fun bufferPoolAvailableMemory(): Long = free.availableMemory()

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    fun flushInProgress(): Boolean = flushesInProgress.get() > 0

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately
     * ready
     */
    fun beginFlush() = flushesInProgress.getAndIncrement()

    /**
     * Are there any threads currently appending messages?
     */
    private fun appendsInProgress(): Boolean = appendsInProgress.get() > 0

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    @Throws(InterruptedException::class)
    fun awaitFlushCompletion() {
        try {
            // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the time of the
            // flush. We must be careful not to hold a reference to the ProduceBatch(s) so that
            // garbage collection can occur on the contents.
            // The sender will remove ProducerBatch(s) from the original incomplete collection.
            for (result: ProduceRequestResult in incomplete.requestResults()) result.await()
        } finally {
            flushesInProgress.decrementAndGet()
        }
    }

    /**
     * Check whether there are any pending batches (whether sent or unsent).
     */
    fun hasIncomplete(): Boolean = !incomplete.isEmpty

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    fun abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches()
        } while (appendsInProgress())
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a
        // new batch appended by the last appending thread.
        abortBatches()
        topicInfoMap.clear()
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private fun abortBatches() = abortBatches(KafkaException("Producer is closed forcefully."))

    /**
     * Abort all incomplete batches (whether they have been sent or not)
     */
    fun abortBatches(reason: RuntimeException?) {
        for (batch: ProducerBatch in incomplete.copyAll()) {
            val dq = getDeque(batch.topicPartition)
            synchronized(dq!!) {
                batch.abortRecordAppends()
                dq.remove(batch)
            }
            batch.abort(reason)
            deallocate(batch)
        }
    }

    /**
     * Abort any batches which have not been drained
     */
    fun abortUndrainedBatches(reason: RuntimeException?) {
        for (batch: ProducerBatch in incomplete.copyAll()) {
            val dq = getDeque(batch.topicPartition)!!
            var aborted = false
            synchronized(dq) {
                if (
                    transactionManager != null && !batch.hasSequence
                    || transactionManager == null && !batch.isClosed
                ) {
                    aborted = true
                    batch.abortRecordAppends()
                    dq.remove(batch)
                }
            }
            if (aborted) {
                batch.abort(reason)
                deallocate(batch)
            }
        }
    }

    fun mutePartition(tp: TopicPartition) = muted.add(tp)

    fun unmutePartition(tp: TopicPartition) = muted.remove(tp)

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    fun close() {
        closed = true
        free.close()
    }

    /**
     * Partitioner config for built-in partitioner
     *
     * @property enableAdaptivePartitioning If it's true, partition switching adapts to broker load,
     * otherwise partition switching is random.
     * @property partitionAvailabilityTimeoutMs If a broker cannot process produce requests from a
     * partition for the specified time, the partition is treated by the partitioner as not
     * available. If the timeout is 0, this logic is disabled.
     */
    class PartitionerConfig(
        val enableAdaptivePartitioning: Boolean = false,
        val partitionAvailabilityTimeoutMs: Long = 0,
    )

    /*
    * Metadata about a record just appended to the record accumulator
    */
    class RecordAppendResult(
        val future: FutureRecordMetadata?,
        val batchIsFull: Boolean,
        val newBatchCreated: Boolean,
        val abortForNewBatch: Boolean,
        val appendedBytes: Int,
    )

    /**
     * The callbacks passed into append
     */
    interface AppendCallbacks : Callback {

        /**
         * Called to set partition (when append is called, partition may not be calculated yet).
         *
         * @param partition The partition
         */
        fun setPartition(partition: Int)
    }

    /**
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    class ReadyCheckResult(
        val readyNodes: MutableSet<Node>,
        val nextReadyCheckDelayMs: Long,
        val unknownLeaderTopics: Set<String>,
    )

    /**
     * Per topic info.
     */
    private class TopicInfo(logContext: LogContext, topic: String, stickyBatchSize: Int) {
        val batches: ConcurrentMap<Int, Deque<ProducerBatch>> = CopyOnWriteMap()
        val builtInPartitioner: BuiltInPartitioner = BuiltInPartitioner(
            logContext = logContext,
            topic = topic,
            stickyBatchSize = stickyBatchSize
        )
    }

    /**
     * Node latency stats for each node that are used for adaptive partition distribution
     * Visible for testing
     *
     * @property readyTimeMs Last time the node had batches ready to send
     */
    class NodeLatencyStats internal constructor(
        @field:Volatile var readyTimeMs: Long
    ) {

        /**
         * Last time the node was able to drain batches
         */
        @Volatile
        var drainTimeMs: Long = readyTimeMs
    }
}
