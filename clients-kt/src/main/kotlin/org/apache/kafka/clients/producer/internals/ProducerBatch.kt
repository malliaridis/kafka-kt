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

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordBatchTooLargeException
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.record.AbstractRecords
import org.apache.kafka.common.record.CompressionRatioEstimator.estimation
import org.apache.kafka.common.record.CompressionRatioEstimator.updateEstimation
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 */
class ProducerBatch(
    val topicPartition: TopicPartition,
    private val recordsBuilder: MemoryRecordsBuilder,
    val createdMs: Long,
    val isSplitBatch: Boolean = false,
) {

    val produceFuture: ProduceRequestResult = ProduceRequestResult(topicPartition)

    private val thunks: MutableList<Thunk> = ArrayList()

    private val attempts = AtomicInteger(0)

    private val finalState = AtomicReference<FinalState?>(null)

    var recordCount = 0

    var maxRecordSize = 0

    private var lastAttemptMs: Long = createdMs

    private var lastAppendTime: Long = createdMs

    private var drainedMs: Long = 0

    private var retry: Boolean = false

    private var reopened = false

    init {
        val compressionRatioEstimation = estimation(
            this.topicPartition.topic,
            recordsBuilder.compressionType
        )
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation)
    }

    /**
     * Append the record to the current record set and return the relative offset within that record
     * set.
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    fun tryAppend(
        timestamp: Long,
        key: ByteArray?,
        value: ByteArray?,
        headers: Array<Header>,
        callback: Callback?,
        now: Long,
    ): FutureRecordMetadata? {
        return if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) null
        else {
            recordsBuilder.append(timestamp, key, value, headers)
            maxRecordSize = AbstractRecords.estimateSizeInBytesUpperBound(
                magic = magic,
                compressionType = recordsBuilder.compressionType,
                key = key,
                value = value,
                headers = headers,
            ).coerceAtLeast(maxRecordSize)

            lastAppendTime = now
            val future = FutureRecordMetadata(
                result = produceFuture,
                batchIndex = recordCount,
                createTimestamp = timestamp,
                serializedKeySize = key?.size ?: -1,
                serializedValueSize = value?.size ?: -1,
                time = Time.SYSTEM,
            )
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            thunks.add(Thunk(callback, future))
            recordCount++
            future
        }
    }

    /**
     * This method is only used by [split] when splitting a large batch to smaller ones.
     * @return true if the record has been successfully appended, false otherwise.
     */
    private fun tryAppendForSplit(
        timestamp: Long,
        key: ByteBuffer?,
        value: ByteBuffer?,
        headers: Array<Header>,
        thunk: Thunk,
    ): Boolean {
        return if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) false
        else {
            // No need to get the CRC.
            recordsBuilder.append(timestamp, key, value, headers)
            maxRecordSize = AbstractRecords.estimateSizeInBytesUpperBound(
                magic = magic,
                compressionType = recordsBuilder.compressionType,
                key = key,
                value = value,
                headers = headers,
            ).coerceAtLeast(maxRecordSize)

            val future = FutureRecordMetadata(
                result = produceFuture,
                batchIndex = recordCount,
                createTimestamp = timestamp,
                serializedKeySize = key?.remaining() ?: -1,
                serializedValueSize = value?.remaining() ?: -1,
                time = Time.SYSTEM,
            )
            // Chain the future to the original thunk.
            thunk.future.chain(future)
            thunks.add(thunk)
            recordCount++
            true
        }
    }

    /**
     * Abort the batch and complete the future and callbacks.
     *
     * @param exception The exception to use to complete the future and awaiting callbacks.
     */
    fun abort(exception: RuntimeException?) {
        check(finalState.compareAndSet(null, FinalState.ABORTED)) {
            "Batch has already been completed in final state " + finalState.get()
        }
        log.trace("Aborting batch for partition {}", topicPartition, exception)
        completeFutureAndFireCallbacks(
            baseOffset = ProduceResponse.INVALID_OFFSET,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
        ) { exception }
    }

    /**
     * Check if the batch has been completed (either successfully or exceptionally).
     *
     * @return `true` if the batch has been completed, `false` otherwise.
     */
    val isDone: Boolean
        get() = finalState() != null

    /**
     * Complete the batch successfully.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @return true if the batch was completed as a result of this call, and false
     * if it had been completed previously
     */
    fun complete(baseOffset: Long, logAppendTime: Long): Boolean = done(
        baseOffset = baseOffset,
        logAppendTime = logAppendTime,
        topLevelException = null,
        recordExceptions = null,
    )

    /**
     * Complete the batch exceptionally. The provided top-level exception will be used
     * for each record future contained in the batch.
     *
     * @param topLevelException top-level partition error
     * @param recordExceptions Record exception function mapping batchIndex to the respective record
     * exception
     * @return `true` if the batch was completed as a result of this call, and `false` if it had
     * been completed previously
     */
    fun completeExceptionally(
        topLevelException: RuntimeException,
        recordExceptions: (Int) -> RuntimeException?,
    ): Boolean = done(
        baseOffset = ProduceResponse.INVALID_OFFSET,
        logAppendTime = RecordBatch.NO_TIMESTAMP,
        topLevelException = topLevelException,
        recordExceptions = recordExceptions
    )

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be
     * called once or twice on a batch. It may be called twice if
     * 1. An inflight batch expires before a response from the broker is received. The batch's final
     *    state is set to FAILED. But it could succeed on the broker and second time around
     *    batch.done() may try to set SUCCEEDED final state.
     * 2. If a transaction abortion happens or if the producer is closed forcefully, the final state
     *    is ABORTED but again it could succeed if broker responds with a success.
     *
     * Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
     *
     * Attempted transitions from one failure state to the same or a different failed state are
     * ignored.
     *
     * Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param topLevelException The exception that occurred (or null if the request was successful)
     * @param recordExceptions Record exception function mapping batchIndex to the respective record
     * exception
     * @return true if the batch was completed successfully and false if the batch was previously
     * aborted
     */
    private fun done(
        baseOffset: Long,
        logAppendTime: Long,
        topLevelException: RuntimeException?,
        recordExceptions: ((Int) -> RuntimeException?)?,
    ): Boolean {
        val tryFinalState =
            if (topLevelException == null) FinalState.SUCCEEDED
            else FinalState.FAILED

        if (tryFinalState == FinalState.SUCCEEDED) log.trace(
            "Successfully produced messages to {} with base offset {}.",
            topicPartition,
            baseOffset,
        )
        else log.trace(
            "Failed to produce messages to {} with base offset {}.",
            topicPartition,
            baseOffset,
            topLevelException,
        )

        if (finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, recordExceptions)
            return true
        }

        if (finalState.get() != FinalState.SUCCEEDED) {
            // Log if a previously unsuccessful batch succeeded later on.
            if (tryFinalState == FinalState.SUCCEEDED) log.debug(
                "ProduceResponse returned {} for {} after batch with base offset {} had already " +
                        "been {}.",
                tryFinalState,
                topicPartition,
                baseOffset,
                finalState.get(),
            )
            // FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
            else log.debug(
                "Ignored state transition {} -> {} for {} batch with base offset {}",
                finalState.get(),
                tryFinalState,
                topicPartition,
                baseOffset
            )
        } else error( // A SUCCESSFUL batch must not attempt another state change.
            "A ${finalState.get()} batch must not attempt another state change to $tryFinalState"
        )
        return false
    }

    private fun completeFutureAndFireCallbacks(
        baseOffset: Long,
        logAppendTime: Long,
        recordExceptions: ((Int) -> RuntimeException?)?,
    ) {
        // Set the future before invoking the callbacks as we rely on its state for the
        // `onCompletion` call
        produceFuture[baseOffset, logAppendTime] = recordExceptions

        // execute callbacks
        thunks.forEachIndexed { index, thunk ->
            try {
                if (thunk.callback != null) {
                    if (recordExceptions == null) thunk.callback.onCompletion(
                        metadata = thunk.future.value(),
                        exception = null
                    ) else thunk.callback.onCompletion(
                        metadata = null,
                        exception = recordExceptions(index),
                    )
                }
            } catch (e: Exception) {
                log.error(
                    "Error executing user-provided callback on message for topic-partition '{}'",
                    topicPartition,
                    e
                )
            }
        }
        produceFuture.done()
    }

    fun split(splitBatchSize: Int): Deque<ProducerBatch?> {
        val batches: Deque<ProducerBatch?> = ArrayDeque()
        val memoryRecords = recordsBuilder.build()
        val recordBatchIter = memoryRecords.batches().iterator()
        check(recordBatchIter.hasNext()) { "Cannot split an empty producer batch." }
        val recordBatch: RecordBatch = recordBatchIter.next()
        require(recordBatch.magic() >= RecordBatch.MAGIC_VALUE_V2 || recordBatch.isCompressed) {
            "Batch splitting cannot be used with non-compressed messages with version v0 and v1"
        }
        require(!recordBatchIter.hasNext()) {
            "A producer batch should only have one record batch."
        }
        val thunkIter: Iterator<Thunk> = thunks.iterator()
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        var batch: ProducerBatch? = null
        for (record in recordBatch) {
            assert(thunkIter.hasNext())
            val thunk = thunkIter.next()
            if (batch == null) batch = createBatchOffAccumulatorForRecord(record, splitBatchSize)

            // A newly created batch can always host the first message.
            if (!batch.tryAppendForSplit(
                    timestamp = record.timestamp(),
                    key = record.key(),
                    value = record.value(),
                    headers = record.headers(),
                    thunk = thunk,
                )
            ) {
                batches.add(batch)
                batch.closeForRecordAppends()
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize)
                batch.tryAppendForSplit(
                    timestamp = record.timestamp(),
                    key = record.key(),
                    value = record.value(),
                    headers = record.headers(),
                    thunk = thunk,
                )
            }
        }

        // Close the last batch and add it to the batch list after split.
        if (batch != null) {
            batches.add(batch)
            batch.closeForRecordAppends()
        }
        produceFuture[ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP] = 
            { RecordBatchTooLargeException() }
        produceFuture.done()
        
        if (hasSequence) {
            var sequence = baseSequence
            val producerIdAndEpoch = ProducerIdAndEpoch(producerId, producerEpoch)
            for (newBatch in batches) {
                newBatch!!.setProducerState(producerIdAndEpoch, sequence, isTransactional)
                sequence += newBatch.recordCount
            }
        }
        return batches
    }

    private fun createBatchOffAccumulatorForRecord(record: Record, batchSize: Int): ProducerBatch {
        val initialSize = AbstractRecords.estimateSizeInBytesUpperBound(
            magic = magic,
            compressionType = recordsBuilder.compressionType,
            key = record.key(),
            value = record.value(),
            headers = record.headers(),
        ).coerceAtLeast(batchSize)
        val buffer = ByteBuffer.allocate(initialSize)

        // Note that we intentionally do not set producer state (producerId, epoch, sequence, and
        // isTransactional) for the newly created batch. This will be set when the batch is dequeued
        // for sending (which is consistent with how normal batches are handled).
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = magic,
            compressionType = recordsBuilder.compressionType,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        return ProducerBatch(topicPartition, builder, createdMs, true)
    }

    val isCompressed: Boolean
        get() = recordsBuilder.compressionType !== CompressionType.NONE

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    private class Thunk(
        val callback: Callback?,
        val future: FutureRecordMetadata,
    )

    override fun toString(): String =
        "ProducerBatch(topicPartition=$topicPartition, recordCount=$recordCount)"

    fun hasReachedDeliveryTimeout(deliveryTimeoutMs: Long, now: Long): Boolean =
        deliveryTimeoutMs <= now - createdMs

    fun finalState(): FinalState? = finalState.get()

    fun attempts(): Int = attempts.get()

    fun reenqueued(now: Long) {
        attempts.getAndIncrement()
        lastAttemptMs = lastAppendTime.coerceAtLeast(now)
        lastAppendTime = lastAppendTime.coerceAtLeast(now)
        retry = true
    }

    fun queueTimeMs(): Long = drainedMs - createdMs

    fun waitedTimeMs(nowMs: Long): Long = (nowMs - lastAttemptMs).coerceAtLeast(0)

    fun drained(nowMs: Long) {
        drainedMs = drainedMs.coerceAtLeast(nowMs)
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    fun inRetry(): Boolean = retry

    fun records(): MemoryRecords = recordsBuilder.build()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("estimatedSizeInBytes"),
    )
    fun estimatedSizeInBytes(): Int = recordsBuilder.estimatedSizeInBytes()

    val estimatedSizeInBytes: Int
        get() = recordsBuilder.estimatedSizeInBytes()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("compressionRatio"),
    )
    fun compressionRatio(): Double = recordsBuilder.compressionRatio

    val compressionRatio: Double
        get() = recordsBuilder.compressionRatio

    val isFull: Boolean
        get() = recordsBuilder.isFull

    fun setProducerState(
        producerIdAndEpoch: ProducerIdAndEpoch,
        baseSequence: Int,
        isTransactional: Boolean,
    ) = recordsBuilder.setProducerState(
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = producerIdAndEpoch.epoch,
        baseSequence = baseSequence,
        isTransactional = isTransactional,
    )

    fun resetProducerState(
        producerIdAndEpoch: ProducerIdAndEpoch,
        baseSequence: Int,
        isTransactional: Boolean,
    ) {
        log.info(
            "Resetting sequence number of batch with current sequence {} for partition {} to {}",
            baseSequence,
            topicPartition,
            baseSequence
        )
        reopened = true
        recordsBuilder.reopenAndRewriteProducerState(
            producerId = producerIdAndEpoch.producerId,
            producerEpoch = producerIdAndEpoch.epoch,
            baseSequence = baseSequence,
            isTransactional = isTransactional,
        )
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is
     * called, it's only possible to update the RecordBatch header.
     */
    fun closeForRecordAppends() = recordsBuilder.closeForRecordAppends()

    fun close() {
        recordsBuilder.close()
        if (!recordsBuilder.isControlBatch) updateEstimation(
            topic = topicPartition.topic,
            type = recordsBuilder.compressionType,
            observedRatio = recordsBuilder.compressionRatio.toFloat(),
        )
        reopened = false
    }

    /**
     * Abort the record builder and reset the state of the underlying buffer. This is used prior to
     * aborting the batch with [abort] and ensures that no record previously appended can be read.
     * This is used in scenarios where we want to ensure a batch ultimately gets aborted, but in
     * which it is not safe to invoke the completion callbacks (e.g. because we are holding a lock,
     * such as when aborting batches in [RecordAccumulator]).
     */
    fun abortRecordAppends() = recordsBuilder.abort()

    val isClosed: Boolean
        get() = recordsBuilder.isClosed

    fun buffer(): ByteBuffer = recordsBuilder.buffer

    fun initialCapacity(): Int = recordsBuilder.initialCapacity

    val isWritable: Boolean
        get() = !recordsBuilder.isClosed

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("magic"),
    )
    fun magic(): Byte = recordsBuilder.magic

    val magic: Byte
        get() = recordsBuilder.magic

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("producerId"),
    )
    fun producerId(): Long = recordsBuilder.producerId

    val producerId: Long
        get() = recordsBuilder.producerId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("producerEpoch"),
    )
    fun producerEpoch(): Short = recordsBuilder.producerEpoch

    val producerEpoch: Short
        get() = recordsBuilder.producerEpoch

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("baseSequence"),
    )
    fun baseSequence(): Int = recordsBuilder.baseSequence

    val baseSequence: Int
        get() = recordsBuilder.baseSequence

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("lastSequence"),
    )
    fun lastSequence(): Int = recordsBuilder.baseSequence + recordsBuilder.numRecords() - 1

    val lastSequence: Int
        get() = recordsBuilder.baseSequence + recordsBuilder.numRecords() - 1

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasSequence"),
    )
    fun hasSequence(): Boolean = baseSequence != RecordBatch.NO_SEQUENCE

    val hasSequence: Boolean
        get() = baseSequence != RecordBatch.NO_SEQUENCE

    val isTransactional: Boolean
        get() = recordsBuilder.isTransactional

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("sequenceHasBeenReset"),
    )
    fun sequenceHasBeenReset(): Boolean = reopened

    val sequenceHasBeenReset: Boolean
        get() = reopened

    enum class FinalState {
        ABORTED,
        FAILED,
        SUCCEEDED
    }

    companion object {
        private val log = LoggerFactory.getLogger(ProducerBatch::class.java)
    }
}
