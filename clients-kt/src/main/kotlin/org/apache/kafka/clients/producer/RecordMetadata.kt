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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse

/**
 * The metadata for a record that has been acknowledged by the server
 *
 * @property topicPartition
 * @property offset The offset of the record in the topic/partition or `-1` if [hasOffset] returns
 * `false`.
 * @property timestamp The timestamp of the record in the topic/partition. May be `-1` if
 * [hasTimestamp] returns `false`.
 * @property timestamp The timestamp of the message.
 * If `LogAppendTime` is used for the topic, the timestamp will be the timestamp returned by the
 * broker. If `CreateTime` is used for the topic, the timestamp is the timestamp in the
 * corresponding [ProducerRecord] if the user provided one. Otherwise, it will be the producer
 * local time when the producer record was handed to the producer.
 * @property serializedKeySize The size of the serialized, uncompressed key in bytes. Set to `-1` if
 * key is `null`.
 * @property serializedValueSize The size of the serialized, uncompressed value in bytes. Set to
 * `-1` if value is `null`.
 */
class RecordMetadata private constructor(
    val topicPartition: TopicPartition,
    val offset: Long,
    val timestamp: Long,
    val serializedKeySize: Int,
    val serializedValueSize: Int
) {

    constructor(
        topicPartition: TopicPartition,
        baseOffset: Long,
        batchIndex: Int,
        timestamp: Long,
        serializedKeySize: Int,
        serializedValueSize: Int,
    ) : this(
        topicPartition = topicPartition,
        // ignore the batchIndex if the base offset is -1, since this indicates the offset is unknown
        offset = if (baseOffset == -1L) baseOffset else baseOffset + batchIndex,
        timestamp = timestamp,
        serializedKeySize = serializedKeySize,
        serializedValueSize = serializedValueSize,
    )

    /**
     * Indicates whether the record metadata includes the offset.
     * @return true if the offset is included in the metadata, false otherwise.
     */
    fun hasOffset(): Boolean {
        return offset != ProduceResponse.INVALID_OFFSET
    }

    /**
     * The offset of the record in the topic/partition.
     * @return the offset of the record, or -1 if {[hasOffset]} returns false.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("offset")
    )
    fun offset(): Long {
        return offset
    }

    /**
     * Indicates whether the record metadata includes the timestamp.
     * @return true if a valid timestamp exists, false otherwise.
     */
    fun hasTimestamp(): Boolean {
        return timestamp != RecordBatch.NO_TIMESTAMP
    }

    /**
     * The timestamp of the record in the topic/partition.
     *
     * @return the timestamp of the record, or -1 if the {[hasTimestamp]} returns false.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("timestamp")
    )
    fun timestamp(): Long {
        return timestamp
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("serializedKeySize")
    )
    fun serializedKeySize(): Int {
        return serializedKeySize
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the returned
     * size is -1.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("serializedValueSize")
    )
    fun serializedValueSize(): Int {
        return serializedValueSize
    }

    /**
     * The topic the record was appended to
     */
    @Deprecated(
        message = "Use property access instead.",
        replaceWith = ReplaceWith("topic")
    )
    fun topic(): String? {
        return topicPartition.topic
    }

    /**
     * The topic the record was appended to
     */
    val topic: String?
        get() = topicPartition.topic

    /**
     * The partition the record was sent to
     */
    @Deprecated(
        message = "Use property access instead.",
        replaceWith = ReplaceWith("partition")
    )
    fun partition(): Int {
        return topicPartition.partition
    }

    /**
     * The partition the record was sent to
     */
    val partition: Int
        get() = topicPartition.partition

    override fun toString(): String {
        return "$topicPartition@$offset"
    }

    companion object {

        /**
         * Partition value for record without partition assigned
         */
        const val UNKNOWN_PARTITION = -1
    }
}
