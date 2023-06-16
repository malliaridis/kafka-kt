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

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders

/**
 * A key/value pair to be sent to Kafka. This consists of a [topic] name to which the record is
 * being sent, an optional [partition] number, and an optional [key] and [value].
 *
 * If a valid partition number is specified that partition will be used when sending the record. If
 * no partition is specified but a key is present a partition will be chosen using a hash of the
 * key. If neither key nor partition is present a partition will be assigned in a round-robin
 * fashion.
 *
 * The record also has an associated timestamp. If the user did not provide a timestamp, the
 * producer will stamp the record with its current time. The timestamp eventually used by Kafka
 * depends on the timestamp type configured for the topic.
 *
 * * If the topic is configured to use [org.apache.kafka.common.record.TimestampType.CREATE_TIME],
 * the timestamp in the producer record will be used by the broker.
 *
 * * If the topic is configured to use [org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME],
 * the timestamp in the producer record will be overwritten by the broker with the broker local time
 * when it appends the message to its log.
 *
 * In either of the cases above, the timestamp that has actually been used will be returned to user
 * in [RecordMetadata].
 *
 * @property topic The topic the record will be appended to
 * @property partition The partition to which the record should be sent. Cannot be negative.
 * @property timestamp The timestamp of the record, in milliseconds since epoch. If `null`, the
 * producer will assign the timestamp using `System.currentTimeMillis()`. Cannot be negative.
 * @property key The key that will be included in the record
 * @property value The record contents
 * @property headers The headers that will be included in the record
 */
data class ProducerRecord<K, V>(
    val topic: String,
    val partition: Int? = null,
    val timestamp: Long? = null,
    val key: K? = null,
    val value: V?,
    val headers: Headers = RecordHeaders(),
) {

    init {
        require(!(timestamp != null && timestamp < 0)) {
            String.format(
                "Invalid timestamp: %d. Timestamp should always be non-negative or null.",
                timestamp
            )
        }
        require(!(partition != null && partition < 0)) {
            String.format(
                "Invalid partition: %d. Partition number should always be non-negative or null.",
                partition
            )
        }
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers The headers that will be included in the record
     */
    constructor(
        topic: String,
        partition: Int? = null,
        timestamp: Long? = null,
        key: K?,
        value: V?,
        headers: Iterable<Header>? = null,
    ) : this(
        topic = topic,
        partition = partition,
        timestamp = timestamp,
        key = key,
        value = value,
        headers = RecordHeaders(headers),
    )

    /**
     * @return The topic this record is being sent to
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("topic"),
    )
    fun topic(): String {
        return topic
    }

    /**
     * @return The headers
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("headers"),
    )
    fun headers(): Headers {
        return headers
    }

    /**
     * @return The key (or null if no key is specified)
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("key"),
    )
    fun key(): K? {
        return key
    }

    /**
     * @return The value
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("value"),
    )
    fun value(): V? {
        return value
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("timestamp"),
    )
    fun timestamp(): Long? {
        return timestamp
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("partition"),
    )
    fun partition(): Int? {
        return partition
    }

    override fun toString(): String {
        return "ProducerRecord(topic=" + topic +
                ", partition=" + partition +
                ", headers=" + headers +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ")"
    }
}
