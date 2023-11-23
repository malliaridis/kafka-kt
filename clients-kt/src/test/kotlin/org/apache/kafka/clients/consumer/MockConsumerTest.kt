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
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MockConsumerTest {
    
    private val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
    
    @Test
    fun testSimpleMock() {
        consumer.subscribe(setOf("test"))
        assertEquals(0, consumer.poll(Duration.ZERO).count())
        consumer.rebalance(listOf(TopicPartition("test", 0), TopicPartition("test", 1)))
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        val beginningOffsets = HashMap<TopicPartition, Long?>()
        beginningOffsets[TopicPartition("test", 0)] = 0L
        beginningOffsets[TopicPartition("test", 1)] = 0L
        consumer.updateBeginningOffsets(beginningOffsets)
        consumer.seek(TopicPartition("test", 0), 0)
        val rec1 = ConsumerRecord(
            topic = "test",
            partition = 0,
            offset = 0,
            timestamp = 0L,
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 0,
            serializedValueSize = 0,
            key = "key1",
            value = "value1",
            headers = RecordHeaders(),
            leaderEpoch = null,
        )
        val rec2 = ConsumerRecord(
            topic = "test",
            partition = 0,
            offset = 1,
            timestamp = 0L,
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 0,
            serializedValueSize = 0,
            key = "key2",
            value = "value2",
            headers = RecordHeaders(),
            leaderEpoch = null,
        )
        consumer.addRecord(rec1)
        consumer.addRecord(rec2)
        val recs = consumer.poll(Duration.ofMillis(1))
        val iter = recs.iterator()
        assertEquals(rec1, iter.next())
        assertEquals(rec2, iter.next())
        assertFalse(iter.hasNext())
        val tp = TopicPartition("test", 0)
        assertEquals(2L, consumer.position(tp))
        consumer.commitSync()
        assertEquals(
            expected = 2L,
            actual = consumer.committed(setOf(tp))[tp]!!.offset,
        )
    }

    @Suppress("Deprecation")
    @Test
    fun testSimpleMockDeprecated() {
        consumer.subscribe(setOf("test"))
        assertEquals(0, consumer.poll(timeout = 1000).count())
        consumer.rebalance(
            newAssignment = listOf(
                TopicPartition("test", 0),
                TopicPartition("test", 1),
            ),
        )
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        val beginningOffsets = mutableMapOf<TopicPartition, Long?>()
        beginningOffsets[TopicPartition("test", 0)] = 0L
        beginningOffsets[TopicPartition("test", 1)] = 0L
        consumer.updateBeginningOffsets(beginningOffsets)
        consumer.seek(TopicPartition("test", 0), 0)
        val rec1 = ConsumerRecord(
            topic = "test",
            partition = 0,
            offset = 0,
            timestamp = 0L,
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 0,
            serializedValueSize = 0,
            key = "key1",
            value = "value1",
            headers = RecordHeaders(),
            leaderEpoch = null,
        )
        val rec2 = ConsumerRecord(
            topic = "test",
            partition = 0,
            offset = 1,
            timestamp = 0L,
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 0,
            serializedValueSize = 0,
            key = "key2",
            value = "value2",
            headers = RecordHeaders(),
            leaderEpoch = null,
        )
        consumer.addRecord(rec1)
        consumer.addRecord(rec2)
        val recs = consumer.poll(timeout = 1)
        val iter = recs.iterator()
        assertEquals(rec1, iter.next())
        assertEquals(rec2, iter.next())
        assertFalse(iter.hasNext())
        val tp = TopicPartition("test", 0)
        assertEquals(2L, consumer.position(tp))
        consumer.commitSync()
        assertEquals(2L, consumer.committed(setOf(tp))[tp]!!.offset)
        assertEquals(
            ConsumerGroupMetadata(
                groupId = "dummy.group.id",
                generationId = 1,
                memberId = "1",
                groupInstanceId = null,
            ),
            consumer.groupMetadata()
        )
    }

    @Test
    fun testConsumerRecordsIsEmptyWhenReturningNoRecords() {
        val partition = TopicPartition("test", 0)
        consumer.assign(setOf(partition))
        consumer.addRecord(
            ConsumerRecord(
                topic = "test",
                partition = 0,
                offset = 0,
                key = "key",
                value = "value",
            )
        )
        consumer.updateEndOffsets(mapOf(partition to 1L))
        consumer.seekToEnd(setOf(partition))
        val records = consumer.poll(Duration.ofMillis(1))
        assertEquals(0, records.count())
        assertTrue(records.isEmpty)
    }

    @Test
    fun shouldNotClearRecordsForPausedPartitions() {
        val partition0 = TopicPartition("test", 0)
        val testPartitionList: Collection<TopicPartition> = listOf(partition0)
        consumer.assign(testPartitionList)
        consumer.addRecord(
            ConsumerRecord(
                topic = "test",
                partition = 0,
                offset = 0,
                key = "key",
                value = "value",
            )
        )
        consumer.updateBeginningOffsets(mapOf(partition0 to 0L))
        consumer.seekToBeginning(testPartitionList)
        consumer.pause(testPartitionList)
        consumer.poll(Duration.ofMillis(1))
        consumer.resume(testPartitionList)
        val recordsSecondPoll = consumer.poll(Duration.ofMillis(1))
        assertEquals(1, recordsSecondPoll.count())
    }

    @Test
    fun endOffsetsShouldBeIdempotent() {
        val partition = TopicPartition("test", 0)
        consumer.updateEndOffsets(mapOf(partition to 10L))

        // consumer.endOffsets should NOT change the value of end offsets
        assertEquals(10L, consumer.endOffsets(setOf(partition))[partition] as Long)
        assertEquals(10L, consumer.endOffsets(setOf(partition))[partition] as Long)
        assertEquals(10L, consumer.endOffsets(setOf(partition))[partition] as Long)
        consumer.updateEndOffsets(mapOf(partition to 11L))

        // consumer.endOffsets should NOT change the value of end offsets
        assertEquals(11L, consumer.endOffsets(setOf(partition))[partition] as Long)
        assertEquals(11L, consumer.endOffsets(setOf(partition))[partition] as Long)
        assertEquals(11L, consumer.endOffsets(setOf(partition))[partition] as Long)
    }
}
