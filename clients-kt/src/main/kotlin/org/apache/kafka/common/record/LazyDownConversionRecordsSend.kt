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

package org.apache.kafka.common.record

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.network.TransferableChannel
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * Encapsulation for [RecordsSend] for [LazyDownConversionRecords]. Records are down-converted in
 * batches and on-demand when [writeTo] method is called.
 */
class LazyDownConversionRecordsSend(
    records: LazyDownConversionRecords,
) : RecordsSend<LazyDownConversionRecords>(records, records.sizeInBytes()) {

    val recordConversionStats: RecordConversionStats = RecordConversionStats()

    private var convertedRecordsWriter: RecordsSend<*>? = null

    private val convertedRecordsIterator: Iterator<ConvertedRecords<*>> =
        records().iterator(MAX_READ_SIZE.toLong())

    private fun buildOverflowBatch(remaining: Int): MemoryRecords {

        // We do not have any records left to down-convert. Construct an overflow message for the
        // length remaining. This message will be ignored by the consumer because its length will be
        // past the length of maximum possible response size.
        // DefaultRecordBatch =>
        //      BaseOffset => Int64
        //      Length => Int32
        //      ...
        val overflowMessageBatch = ByteBuffer.allocate(
            max(MIN_OVERFLOW_MESSAGE_LENGTH, (remaining + 1).coerceAtMost(MAX_READ_SIZE))
        )
        overflowMessageBatch.putLong(-1L)

        // Fill in the length of the overflow batch. A valid batch must be at least as long as the
        // minimum batch overhead.
        overflowMessageBatch.putInt(
            (remaining + 1).coerceAtLeast(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
        )
        log.debug(
            "Constructed overflow message batch for partition {} with length={}",
            topicPartition(),
            remaining
        )
        return MemoryRecords.readableRecords(overflowMessageBatch)
    }

    @Throws(IOException::class)
    public override fun writeTo(
        channel: TransferableChannel,
        previouslyWritten: Long,
        remaining: Int
    ): Long {
        if (convertedRecordsWriter == null || convertedRecordsWriter!!.completed()) {
            var convertedRecords: MemoryRecords
            try {
                // Check if we have more chunks left to down-convert
                if (convertedRecordsIterator.hasNext()) {
                    // Get next chunk of down-converted messages
                    val recordsAndStats = convertedRecordsIterator.next()
                    convertedRecords = recordsAndStats.records as MemoryRecords
                    recordConversionStats.add(recordsAndStats.recordConversionStats)
                    log.debug(
                        "Down-converted records for partition {} with length={}",
                        topicPartition(),
                        convertedRecords.sizeInBytes()
                    )
                } else {
                    convertedRecords = buildOverflowBatch(remaining)
                }
            } catch (e: UnsupportedCompressionTypeException) {
                // We have encountered a compression type which does not support down-conversion (e.g. zstd).
                // Since we have already sent at least one batch and we have committed to the fetch size, we
                // send an overflow batch. The consumer will read the first few records and then fetch from the
                // offset of the batch which has the unsupported compression type. At that time, we will
                // send back the UNSUPPORTED_COMPRESSION_TYPE error which will allow the consumer to fail gracefully.
                convertedRecords = buildOverflowBatch(remaining)
            }
            convertedRecordsWriter = DefaultRecordsSend(
                convertedRecords,
                Math.min(convertedRecords.sizeInBytes(), remaining)
            )
        }
        return convertedRecordsWriter!!.writeTo(channel)
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("recordConversionStats"),
    )
    fun recordConversionStats(): RecordConversionStats = recordConversionStats

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topicPartition"),
    )
    fun topicPartition(): TopicPartition = records().topicPartition

    val topicPartition: TopicPartition
        get() = records().topicPartition

    companion object {

        private val log: Logger = LoggerFactory.getLogger(LazyDownConversionRecordsSend::class.java)

        private const val MAX_READ_SIZE = 128 * 1024

        const val MIN_OVERFLOW_MESSAGE_LENGTH = Records.LOG_OVERHEAD
    }
}
