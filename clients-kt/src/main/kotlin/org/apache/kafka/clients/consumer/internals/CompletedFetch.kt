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

import java.util.PriorityQueue
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.errors.RecordDeserializationException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction
import org.apache.kafka.common.record.ControlRecordType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * [CompletedFetch] represents a [batch][RecordBatch] of [records][Record] that was returned from the
 * broker via a [FetchRequest]. It contains logic to maintain state between calls to [.fetchRecords].
 *
 * @param K Record key type
 * @param V Record value type
 */
internal class CompletedFetch<K, V>(
    logContext: LogContext,
    private val subscriptions: SubscriptionState,
    private val fetchConfig: FetchConfig<K, V>,
    private val decompressionBufferSupplier: BufferSupplier,
    val partition: TopicPartition,
    val partitionData: FetchResponseData.PartitionData,
    private val metricAggregator: FetchMetricsAggregator,
    fetchOffset: Long,
    val requestVersion: Short,
) {

    var nextFetchOffset: Long = fetchOffset

    var lastEpoch: Int? = null

    var isConsumed = false

    var initialized = false

    private val log: Logger = logContext.logger(javaClass)

    private val batches: Iterator<RecordBatch> = FetchResponse.recordsOrFail(partitionData).batches().iterator()

    private val abortedProducerIds: MutableSet<Long> = mutableSetOf()

    private val abortedTransactions: PriorityQueue<AbortedTransaction>? = abortedTransactions(partitionData)

    private var recordsRead = 0

    private var bytesRead = 0

    private lateinit var currentBatch: RecordBatch

    private var lastRecord: Record? = null

    private var records: CloseableIterator<Record>? = null

    private var cachedRecordException: Exception? = null

    private var corruptLastRecord = false

    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    fun recordAggregatedMetrics(bytes: Int, records: Int) {
        metricAggregator.record(partition, bytes, records)
    }

    /**
     * Draining a [CompletedFetch] will signal that the data has been consumed and the underlying resources
     * are closed. This is somewhat analogous to [closing][Closeable.close], though no error will result if a
     * caller invokes [fetchRecords]; an empty [list][List] will be returned instead.
     */
    fun drain() {
        if (!isConsumed) {
            maybeCloseRecordStream()
            cachedRecordException = null
            isConsumed = true
            recordAggregatedMetrics(bytesRead, recordsRead)

            // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
            // for the same topic can remain together (allowing for more efficient serialization).
            if (bytesRead > 0) subscriptions.movePartitionToEnd(partition)
        }
    }

    private fun maybeEnsureValid(batch: RecordBatch) {
        if (fetchConfig.checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid()
            } catch (e: CorruptRecordException) {
                throw KafkaException(
                    "Record batch for partition $partition at offset " +
                            "${batch.baseOffset()} is invalid, cause: ${e.message}"
                )
            }
        }
    }

    private fun maybeEnsureValid(record: Record) {
        if (fetchConfig.checkCrcs) {
            try {
                record.ensureValid()
            } catch (e: CorruptRecordException) {
                throw KafkaException(
                    "Record for partition $partition at offset ${record.offset()} is invalid, cause: ${e.message}"
                )
            }
        }
    }

    private fun maybeCloseRecordStream() {
        records?.let {
            it.close()
            records = null
        }
    }

    private fun nextFetchedRecord(): Record? {
        while (true) {
            if (records == null || !records!!.hasNext()) {
                maybeCloseRecordStream()

                if (!batches.hasNext()) {
                    // Message format v2 preserves the last offset in a batch even if the last record is removed
                    // through compaction. By using the next offset computed from the last offset in the batch,
                    // we ensure that the offset of the next fetch will point to the next batch, which avoids
                    // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    // fetching the same batch repeatedly).
                    if (::currentBatch.isInitialized) nextFetchOffset = currentBatch.nextOffset()
                    drain()
                    return null
                }

                currentBatch = batches.next()
                lastEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch())
                maybeEnsureValid(currentBatch)

                if (fetchConfig.isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // remove from the aborted transaction queue all aborted transactions which have begun
                    // before the current batch's last offset and add the associated producerIds to the
                    // aborted producer set
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset())

                    val producerId = currentBatch.producerId()
                    if (containsAbortMarker(currentBatch)) abortedProducerIds.remove(producerId)
                    else if (isBatchAborted(currentBatch)) {
                        log.debug(
                            "Skipping aborted record batch from partition {} with producerId {} and offsets {} to {}",
                            partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset()
                        )
                        nextFetchOffset = currentBatch.nextOffset()
                        continue
                    }
                }

                records = currentBatch.streamingIterator(decompressionBufferSupplier)
            } else {
                val record = records!!.next()
                // skip any records out of range
                if (record.offset() >= nextFetchOffset) {
                    // we only do validation when the message should not be skipped.
                    maybeEnsureValid(record)

                    // control records are not returned to the user
                    if (!currentBatch.isControlBatch) return record
                    else {
                        // Increment the next fetch offset when we skip a control batch.
                        nextFetchOffset = record.offset() + 1
                    }
                }
            }
        }
    }

    /**
     * The [batch][RecordBatch] of [records][Record] is converted to a [list][List] of
     * [consumer records][ConsumerRecord] and returned. [Decompression][BufferSupplier] and
     * [deserialization][Deserializer] of the [record&#39;s][Record] key and value are performed in
     * this step.
     *
     * @param maxRecords The number of records to return; the number returned may be `0 <= maxRecords`
     * @return [Consumer records][ConsumerRecord]
     */
    fun fetchRecords(maxRecords: Int): List<ConsumerRecord<K, V>> {
        // Error when fetching the next record before deserialization.
        if (corruptLastRecord) throw KafkaException(
            "Received exception when fetching the next record from $partition. " +
                    "If needed, please seek past the record to continue consumption.",
            cachedRecordException
        )

        if (isConsumed) return emptyList()

        val records = mutableListOf<ConsumerRecord<K, V>>()

        try {
            for (i in 0..<maxRecords) {
                // Only move to next record if there was no exception in the last fetch. Otherwise, we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true
                    lastRecord = nextFetchedRecord()
                    corruptLastRecord = false
                }

                if (lastRecord == null) break

                val leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch())
                val timestampType = currentBatch.timestampType()
                val record = parseRecord(partition, leaderEpoch, timestampType, lastRecord!!)
                records.add(record)
                recordsRead++
                bytesRead += lastRecord!!.sizeInBytes()
                nextFetchOffset = lastRecord!!.offset() + 1
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null
            }
        } catch (se: SerializationException) {
            cachedRecordException = se
            if (records.isEmpty()) throw se
        } catch (e: KafkaException) {
            cachedRecordException = e
            if (records.isEmpty()) throw KafkaException(
                message = "Received exception when fetching the next record from $partition. " +
                        "If needed, please seek past the record to continue consumption.",
                cause = e,
            )
        }
        return records
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    fun parseRecord(
        partition: TopicPartition,
        leaderEpoch: Int?,
        timestampType: TimestampType,
        record: Record,
    ): ConsumerRecord<K, V> {
        try {
            val offset = record.offset()
            val timestamp = record.timestamp()
            val headers: Headers = RecordHeaders(record.headers())
            val keyBytes = record.key()
            val key= keyBytes?.let {
                fetchConfig.keyDeserializer.deserialize(partition.topic, headers, it)
            }
            val valueBytes = record.value()
            val value = valueBytes?.let {
                fetchConfig.valueDeserializer.deserialize(partition.topic, headers, it)
            }
            return ConsumerRecord(
                topic = partition.topic,
                partition = partition.partition,
                offset = offset,
                timestamp = timestamp,
                timestampType = timestampType,
                serializedKeySize = keyBytes?.remaining() ?: ConsumerRecord.NULL_SIZE,
                serializedValueSize = valueBytes?.remaining() ?: ConsumerRecord.NULL_SIZE,
                key = key as K,
                value = value as V,
                headers = headers,
                leaderEpoch = leaderEpoch,
            )
        } catch (e: RuntimeException) {
            throw RecordDeserializationException(
                partition = partition,
                offset = record.offset(),
                message = "Error deserializing key/value for partition $partition at offset " +
                        "${record.offset()}. If needed, please seek past the record to continue consumption.",
                cause = e,
            )
        }
    }

    private fun maybeLeaderEpoch(leaderEpoch: Int): Int? =
        if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) null
        else leaderEpoch

    private fun consumeAbortedTransactionsUpTo(offset: Long) {
        abortedTransactions ?: return
        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset <= offset) {
            val abortedTransaction = abortedTransactions.poll()
            abortedProducerIds.add(abortedTransaction.producerId)
        }
    }

    private fun isBatchAborted(batch: RecordBatch): Boolean =
        batch.isTransactional && abortedProducerIds.contains(batch.producerId())

    private fun abortedTransactions(partition: FetchResponseData.PartitionData): PriorityQueue<AbortedTransaction>? {
        val abortedTransactions = partition.abortedTransactions
        if (abortedTransactions.isNullOrEmpty()) return null
        val actualAbortedTransactions: PriorityQueue<AbortedTransaction> = PriorityQueue(
            abortedTransactions.size,
            Comparator.comparingLong(AbortedTransaction::firstOffset),
        )
        actualAbortedTransactions.addAll(abortedTransactions)
        return actualAbortedTransactions
    }

    private fun containsAbortMarker(batch: RecordBatch): Boolean {
        if (!batch.isControlBatch) return false

        val batchIterator = batch.iterator()
        if (!batchIterator.hasNext()) return false

        val firstRecord = batchIterator.next()
        return ControlRecordType.ABORT == ControlRecordType.parse((firstRecord.key())!!)
    }
}
