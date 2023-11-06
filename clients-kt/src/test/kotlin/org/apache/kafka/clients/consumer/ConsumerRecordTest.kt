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

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ConsumerRecordTest {
    @Test
    fun testShortConstructor() {
        val topic = "topic"
        val partition = 0
        val offset: Long = 23
        val key = "key"
        val value = "value"
        val record = ConsumerRecord(
            topic = topic,
            partition = partition,
            offset = offset,
            key = key,
            value = value,
        )
        assertEquals(topic, record.topic)
        assertEquals(partition, record.partition)
        assertEquals(offset, record.offset)
        assertEquals(key, record.key)
        assertEquals(value, record.value)
        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, record.timestampType)
        assertEquals(ConsumerRecord.NO_TIMESTAMP, record.timestamp)
        assertEquals(ConsumerRecord.NULL_SIZE, record.serializedKeySize)
        assertEquals(ConsumerRecord.NULL_SIZE, record.serializedValueSize)
        assertEquals(null, record.leaderEpoch)
        assertEquals(RecordHeaders(), record.headers)
    }

    @Test
    @Deprecated("")
    fun testConstructorsWithChecksum() {
        val topic = "topic"
        val partition = 0
        val offset: Long = 23
        val timestamp = 23434217432432L
        val timestampType = TimestampType.CREATE_TIME
        val key = "key"
        val value = "value"
        val checksum = 50L
        val serializedKeySize = 100
        val serializedValueSize = 1142
        var record = ConsumerRecord(
            topic = topic,
            partition = partition,
            offset = offset,
            timestamp = timestamp,
            timestampType = timestampType,
            checksum = checksum,
            serializedKeySize = serializedKeySize,
            serializedValueSize = serializedValueSize,
            key = key,
            value = value,
        )
        assertEquals(topic, record.topic)
        assertEquals(partition, record.partition)
        assertEquals(offset, record.offset)
        assertEquals(key, record.key)
        assertEquals(value, record.value)
        assertEquals(timestampType, record.timestampType)
        assertEquals(timestamp, record.timestamp)
        assertEquals(serializedKeySize, record.serializedKeySize)
        assertEquals(serializedValueSize, record.serializedValueSize)
        assertNull(record.leaderEpoch)
        assertEquals(RecordHeaders(), record.headers)
        val headers = RecordHeaders()
        headers.add(
            RecordHeader(
                key = "header key",
                value = "header value".toByteArray(),
            )
        )
        record = ConsumerRecord(
            topic = topic,
            partition = partition,
            offset = offset,
            timestamp = timestamp,
            timestampType = timestampType,
            checksum = checksum,
            serializedKeySize = serializedKeySize,
            serializedValueSize = serializedValueSize,
            key = key,
            value = value,
            headers = headers,
        )
        assertEquals(topic, record.topic)
        assertEquals(partition, record.partition)
        assertEquals(offset, record.offset)
        assertEquals(key, record.key)
        assertEquals(value, record.value)
        assertEquals(timestampType, record.timestampType)
        assertEquals(timestamp, record.timestamp)
        assertEquals(serializedKeySize, record.serializedKeySize)
        assertEquals(serializedValueSize, record.serializedValueSize)
        assertEquals(null, record.leaderEpoch)
        assertEquals(headers, record.headers)
        val leaderEpoch = 10
        record = ConsumerRecord(
            topic = topic,
            partition = partition,
            offset = offset,
            timestamp = timestamp,
            timestampType = timestampType,
            checksum = checksum,
            serializedKeySize = serializedKeySize,
            serializedValueSize = serializedValueSize,
            key = key,
            value = value,
            headers = headers,
            leaderEpoch = leaderEpoch,
        )
        assertEquals(topic, record.topic)
        assertEquals(partition, record.partition)
        assertEquals(offset, record.offset)
        assertEquals(key, record.key)
        assertEquals(value, record.value)
        assertEquals(timestampType, record.timestampType)
        assertEquals(timestamp, record.timestamp)
        assertEquals(serializedKeySize, record.serializedKeySize)
        assertEquals(serializedValueSize, record.serializedValueSize)
        assertEquals(leaderEpoch, record.leaderEpoch)
        assertEquals(headers, record.headers)
    }
}
