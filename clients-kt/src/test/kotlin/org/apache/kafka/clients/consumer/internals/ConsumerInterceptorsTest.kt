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

import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ConsumerInterceptorsTest {

    private val filterPartition1 = 5

    private val filterPartition2 = 6

    private val topic = "test"

    private val partition = 1

    private val tp = TopicPartition(topic = topic, partition = partition)

    private val filterTopicPart1 = TopicPartition(topic = "test5", partition = filterPartition1)

    private val filterTopicPart2 = TopicPartition(topic = "test6", partition = filterPartition2)

    private val consumerRecord = ConsumerRecord<Int?, Int?>(
        topic = topic,
        partition = partition,
        offset = 0,
        timestamp = 0L,
        timestampType = TimestampType.CREATE_TIME,
        serializedKeySize = 0,
        serializedValueSize = 0,
        key = 1,
        value = 1,
        headers = RecordHeaders(),
        leaderEpoch = null,
    )

    private var onCommitCount = 0

    private var onConsumeCount = 0

    /**
     * Test consumer interceptor that filters records in onConsume() intercept
     */
    private inner class FilterConsumerInterceptor<K, V>(
        private val filterPartition: Int,
    ) : ConsumerInterceptor<K, V> {

        private var throwExceptionOnConsume = false

        private var throwExceptionOnCommit = false

        override fun configure(configs: Map<String, Any?>) = Unit

        override fun onConsume(records: ConsumerRecords<K?, V?>): ConsumerRecords<K?, V?> {
            onConsumeCount++
            if (throwExceptionOnConsume)
                throw KafkaException("Injected exception in FilterConsumerInterceptor.onConsume.")

            // filters out topic/partitions with partition == FILTER_PARTITION
            val recordMap: MutableMap<TopicPartition, List<ConsumerRecord<K?, V?>>> = HashMap()
            for (tp in records.partitions())
                if (tp.partition != filterPartition) recordMap[tp] = records.records(tp)

            return ConsumerRecords(recordMap)
        }

        override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) {
            onCommitCount++
            if (throwExceptionOnCommit)
                throw KafkaException("Injected exception in FilterConsumerInterceptor.onCommit.")
        }

        override fun close() = Unit

        // if 'on' is true, onConsume will always throw an exception
        fun injectOnConsumeError(on: Boolean) {
            throwExceptionOnConsume = on
        }

        // if 'on' is true, onConsume will always throw an exception
        fun injectOnCommitError(on: Boolean) {
            throwExceptionOnCommit = on
        }
    }

    @Test
    fun testOnConsumeChain() {

        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaConsumer, but ok for testing interceptor callbacks
        val interceptor1 = FilterConsumerInterceptor<Int, Int>(filterPartition1)
        val interceptor2 = FilterConsumerInterceptor<Int, Int>(filterPartition2)
        val interceptors = ConsumerInterceptors(listOf(interceptor1, interceptor2))

        // verify that onConsumer modifies ConsumerRecords
        val records = mutableMapOf<TopicPartition, List<ConsumerRecord<Int?, Int?>>>()
        val list1 = listOf(consumerRecord)

        val list2 = listOf<ConsumerRecord<Int?, Int?>>(
            ConsumerRecord(
                topic = filterTopicPart1.topic,
                partition = filterTopicPart1.partition,
                offset = 0,
                timestamp = 0L,
                timestampType = TimestampType.CREATE_TIME,
                serializedKeySize = 0,
                serializedValueSize = 0,
                key = 1,
                value = 1,
                headers = RecordHeaders(),
                leaderEpoch = null,
            ),
        )
        val list3 = listOf<ConsumerRecord<Int?, Int?>>(
            ConsumerRecord(
                topic = filterTopicPart2.topic,
                partition = filterTopicPart2.partition,
                offset = 0,
                timestamp = 0L,
                timestampType = TimestampType.CREATE_TIME,
                serializedKeySize = 0,
                serializedValueSize = 0,
                key = 1,
                value = 1,
                headers = RecordHeaders(),
                leaderEpoch = null
            ),
        )
        records[tp] = list1
        records[filterTopicPart1] = list2
        records[filterTopicPart2] = list3
        val consumerRecords = ConsumerRecords(records)
        val interceptedRecords = interceptors.onConsume(consumerRecords)

        assertEquals(1, interceptedRecords.count())
        assertTrue(interceptedRecords.partitions().contains(tp))
        assertFalse(interceptedRecords.partitions().contains(filterTopicPart1))
        assertFalse(interceptedRecords.partitions().contains(filterTopicPart2))
        assertEquals(2, onConsumeCount)

        // verify that even if one of the intermediate interceptors throws an exception,
        // all interceptors' onConsume are called
        interceptor1.injectOnConsumeError(true)
        val partInterceptedRecs = interceptors.onConsume(consumerRecords)

        assertEquals(2, partInterceptedRecs.count())
        assertTrue(partInterceptedRecs.partitions().contains(filterTopicPart1)) // since interceptor1 threw exception
        assertFalse(partInterceptedRecs.partitions().contains(filterTopicPart2)) // interceptor2 should still be called
        assertEquals(4, onConsumeCount)

        // if all interceptors throw an exception, records should be unmodified
        interceptor2.injectOnConsumeError(true)
        val noneInterceptedRecs = interceptors.onConsume(consumerRecords)

        assertEquals(noneInterceptedRecs, consumerRecords)
        assertEquals(3, noneInterceptedRecs.count())
        assertEquals(6, onConsumeCount)
        interceptors.close()
    }

    @Test
    fun testOnCommitChain() {

        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaConsumer, but ok for testing interceptor callbacks
        val interceptor1 = FilterConsumerInterceptor<Int, Int>(filterPartition1)
        val interceptor2 = FilterConsumerInterceptor<Int, Int>(filterPartition2)
        val interceptors = ConsumerInterceptors(listOf(interceptor1, interceptor2))

        // verify that onCommit is called for all interceptors in the chain
        val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        offsets[tp] = OffsetAndMetadata(0)
        interceptors.onCommit(offsets)
        assertEquals(2, onCommitCount)

        // verify that even if one of the interceptors throws an exception, all interceptors' onCommit are called
        interceptor1.injectOnCommitError(true)
        interceptors.onCommit(offsets)
        assertEquals(4, onCommitCount)
        interceptors.close()
    }
}
