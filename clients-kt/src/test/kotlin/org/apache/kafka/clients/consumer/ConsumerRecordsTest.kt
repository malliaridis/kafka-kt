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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ConsumerRecordsTest {

    @Test
    @Throws(Exception::class)
    operator fun iterator() {
        val records = mutableMapOf<TopicPartition, List<ConsumerRecord<Int, String>>>()
        val topic = "topic"
        records[TopicPartition(topic, 0)] = emptyList()
        val record1 = ConsumerRecord(
            topic = topic,
            partition = 1,
            offset = 0,
            timestamp = 0L,
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 0,
            serializedValueSize = 0,
            key = 1,
            value = "value1",
            headers = RecordHeaders(),
            leaderEpoch = null,
        )
        val record2 = ConsumerRecord(
            topic = topic,
            partition = 1,
            offset = 1,
            timestamp = 0L,
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 0,
            serializedValueSize = 0,
            key = 2,
            value = "value2",
            headers = RecordHeaders(),
            leaderEpoch = null,
        )
        records[TopicPartition(topic, 1)] = listOf(record1, record2)
        records[TopicPartition(topic, 2)] = emptyList()
        val consumerRecords = ConsumerRecords(records)
        val iter = consumerRecords.iterator()
        var c = 0
        while (iter.hasNext()) {
            val (topic1, partition, offset) = iter.next()
            assertEquals(1, partition)
            assertEquals(topic, topic1)
            assertEquals(c.toLong(), offset)
            c++
        }
        assertEquals(2, c)
    }
}
