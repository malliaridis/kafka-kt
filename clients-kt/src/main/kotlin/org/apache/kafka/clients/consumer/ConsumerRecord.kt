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

package org.apache.kafka.clients.consumer

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.TimestampType

/**
 * A key/value pair to be received from Kafka. This also consists of a topic name and
 * a partition number from which the record is being received, an offset that points
 * to the record in a Kafka partition, and a timestamp as marked by the corresponding ProducerRecord.
 *
 * @constructor Creates a record to be received from a specified topic and partition
 * @param topic The topic this record is received from
 * @param partition The partition of the topic this record is received from
 * @param offset The offset of this record in the corresponding Kafka partition
 * @param timestamp The timestamp of this record, in milliseconds elapsed since unix epoch.
 * @param timestampType The timestamp type
 * @param serializedKeySize The length of the serialized key
 * @param serializedValueSize The length of the serialized value
 * @param key The key of the record, if one exists (null is allowed)
 * @param value The record contents
 * @param headers The headers of the record
 * @param leaderEpoch Optional leader epoch of the record (may be empty for legacy record formats)
 */
data class ConsumerRecord<K, V>(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long = NO_TIMESTAMP,
    val timestampType: TimestampType = TimestampType.NO_TIMESTAMP_TYPE,
    val serializedKeySize: Int = NULL_SIZE,
    val serializedValueSize: Int = NULL_SIZE,
    val key: K,
    val value: V,
    val headers: Headers = RecordHeaders(),
    val leaderEpoch: Int? = null,
) {

    /**
     * Creates a record to be received from a specified topic and partition (provided for
     * compatibility with Kafka 0.10 before the message format supported headers).
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     */
    @Deprecated(
        """use one of the constructors without a `checksum` parameter. This constructor 
            will be removed inApache Kafka 4.0 (deprecated since 3.0)."""
    )
    constructor(
        topic: String,
        partition: Int,
        offset: Long,
        timestamp: Long,
        timestampType: TimestampType,
        checksum: Long,
        serializedKeySize: Int,
        serializedValueSize: Int,
        key: K,
        value: V
    ) : this(
        topic = topic,
        partition = partition,
        offset = offset,
        timestamp = timestamp,
        timestampType = timestampType,
        serializedKeySize = serializedKeySize,
        serializedValueSize = serializedValueSize,
        key = key,
        value = value,
        headers = RecordHeaders(),
        leaderEpoch = null,
    )

    /**
     * Creates a record to be received from a specified topic and partition
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     * @param headers The headers of the record.
     */
    @Deprecated(
        """use one of the constructors without a `checksum` parameter. This constructor 
            will be removed in Apache Kafka 4.0 (deprecated since 3.0)."""
    )
    constructor(
        topic: String,
        partition: Int,
        offset: Long,
        timestamp: Long,
        timestampType: TimestampType,
        checksum: Long?,
        serializedKeySize: Int,
        serializedValueSize: Int,
        key: K,
        value: V,
        headers: Headers,
    ) : this(
        topic = topic,
        partition = partition,
        offset = offset,
        timestamp = timestamp,
        timestampType = timestampType,
        serializedKeySize = serializedKeySize,
        serializedValueSize = serializedValueSize,
        key = key,
        value = value,
        headers = headers,
        leaderEpoch = null,
    )

    /**
     * Creates a record to be received from a specified topic and partition
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     * @param headers The headers of the record
     * @param leaderEpoch Optional leader epoch of the record (may be empty for legacy record formats)
     */
    @Deprecated(
        """use one of the constructors without a `checksum` parameter. This constructor will
             be removed in Apache Kafka 4.0 (deprecated since 3.0)."""
    )
    constructor(
        topic: String,
        partition: Int,
        offset: Long,
        timestamp: Long,
        timestampType: TimestampType,
        checksum: Long?,
        serializedKeySize: Int,
        serializedValueSize: Int,
        key: K,
        value: V,
        headers: Headers,
        leaderEpoch: Int?,
    ) : this(
        topic = topic,
        partition = partition,
        offset = offset,
        timestamp = timestamp,
        timestampType = timestampType,
        serializedKeySize = serializedKeySize,
        serializedValueSize = serializedValueSize,
        key = key,
        value = value,
        headers = headers,
        leaderEpoch = leaderEpoch,
    )

    /**
     * The topic this record is received from (never null)
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topic"),
    )
    fun topic(): String = topic

    /**
     * The partition from which this record is received
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("partition"),
    )
    fun partition(): Int = partition

    /**
     * The headers (never null)
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("headers"),
    )
    fun headers(): Headers = headers

    /**
     * The key (or null if no key is specified)
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("key"),
    )
    fun key(): K = key

    /**
     * The value
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("value"),
    )
    fun value(): V = value

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("offset"),
    )
    fun offset(): Long = offset

    /**
     * The timestamp of this record, in milliseconds elapsed since unix epoch.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("timestamp"),
    )
    fun timestamp(): Long = timestamp

    /**
     * The timestamp type of this record
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("timestampType"),
    )
    fun timestampType(): TimestampType = timestampType

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("serializedKeySize"),
    )
    fun serializedKeySize(): Int = serializedKeySize

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the
     * returned size is -1.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("serializedValueSize"),
    )
    fun serializedValueSize(): Int = serializedValueSize

    /**
     * Get the leader epoch for the record if available
     *
     * @return the leader epoch or empty for legacy record formats
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("leaderEpoch"),
    )
    fun leaderEpoch(): Int? = leaderEpoch

    override fun toString(): String =
        "ConsumerRecord(topic = $topic" +
                ", partition = $partition" +
                ", leaderEpoch = $leaderEpoch" +
                ", offset = $offset" +
                ", $timestampType = $timestamp" +
                ", serialized key size = $serializedKeySize" +
                ", serialized value size = $serializedValueSize" +
                ", headers = $headers" +
                ", key = $key" +
                ", value = $value" +
                ")"

    companion object {

        const val NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP

        const val NULL_SIZE = -1

        @Deprecated(
            """checksums are no longer exposed by this class, this constant will be 
                removed in Apache Kafka 4.0(deprecated since 3.0)."""
        )
        val NULL_CHECKSUM = -1
    }
}
