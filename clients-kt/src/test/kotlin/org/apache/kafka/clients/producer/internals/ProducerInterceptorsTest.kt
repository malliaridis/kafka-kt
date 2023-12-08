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

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ProducerInterceptorsTest {
    
    private val tp = TopicPartition("test", 0)
    
    private val producerRecord = ProducerRecord<Int?, String?,>(
        topic = "test",
        partition = 0,
        key = 1,
        value = "value",
    )
    
    private var onAckCount = 0
    
    private var onErrorAckCount = 0
    
    private var onErrorAckWithTopicSetCount = 0
    
    private var onErrorAckWithTopicPartitionSetCount = 0
    
    private var onSendCount = 0

    private inner class AppendProducerInterceptor(
        private val appendStr: String,
    ) : ProducerInterceptor<Int?, String?> {
        
        private var throwExceptionOnSend = false
        
        private var throwExceptionOnAck = false

        override fun configure(configs: Map<String, Any?>) = Unit

        override fun onSend(record: ProducerRecord<Int?, String?>): ProducerRecord<Int?, String?> {
            onSendCount++
            if (throwExceptionOnSend)
                throw KafkaException("Injected exception in AppendProducerInterceptor.onSend")
            
            return ProducerRecord(
                topic = record.topic,
                partition = record.partition,
                key = record.key!!.toInt(),
                value = record.value + appendStr,
            )
        }

        override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
            onAckCount++
            if (exception != null) {
                onErrorAckCount++
                // the length check is just to call topic() method and let it throw an exception
                // if RecordMetadata.TopicPartition is null
                if (metadata != null && metadata.topic.length >= 0) {
                    onErrorAckWithTopicSetCount++
                    if (metadata.partition >= 0) onErrorAckWithTopicPartitionSetCount++
                }
            }
            if (throwExceptionOnAck)
                throw KafkaException("Injected exception in AppendProducerInterceptor.onAcknowledgement")
        }

        override fun close() = Unit

        // if 'on' is true, onSend will always throw an exception
        fun injectOnSendError(on: Boolean) {
            throwExceptionOnSend = on
        }

        // if 'on' is true, onAcknowledgement will always throw an exception
        fun injectOnAcknowledgementError(on: Boolean) {
            throwExceptionOnAck = on
        }
    }

    @Test
    fun testOnSendChain() {
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        val interceptor1 = AppendProducerInterceptor("One")
        val interceptor2 = AppendProducerInterceptor("Two")
        
        val interceptorList = listOf(interceptor1, interceptor2)
        val interceptors = ProducerInterceptors(interceptorList)

        // verify that onSend() mutates the record as expected
        val interceptedRecord = interceptors.onSend(producerRecord)
        assertEquals(2, onSendCount)
        assertEquals(producerRecord.topic, interceptedRecord.topic)
        assertEquals(producerRecord.partition, interceptedRecord.partition)
        assertEquals(producerRecord.key, interceptedRecord.key)
        assertEquals(interceptedRecord.value, "${producerRecord.value}OneTwo")

        // onSend() mutates the same record the same way
        val anotherRecord = interceptors.onSend(producerRecord)
        assertEquals(4, onSendCount)
        assertEquals(interceptedRecord, anotherRecord)

        // verify that if one of the interceptors throws an exception, other interceptors' callbacks are still called
        interceptor1.injectOnSendError(true)
        val (_, _, _, _, value) = interceptors.onSend(producerRecord)
        assertEquals(6, onSendCount)
        assertEquals(value, producerRecord.value + "Two")

        // verify the record remains valid if all onSend throws an exception
        interceptor2.injectOnSendError(true)
        val noInterceptRecord = interceptors.onSend(producerRecord)
        assertEquals(producerRecord, noInterceptRecord)
        interceptors.close()
    }

    @Test
    fun testOnAcknowledgementChain() {
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        val interceptor1: AppendProducerInterceptor = AppendProducerInterceptor("One")
        val interceptor2: AppendProducerInterceptor = AppendProducerInterceptor("Two")

        val interceptorList = listOf(interceptor1, interceptor2)
        val interceptors = ProducerInterceptors(interceptorList)

        // verify onAck is called on all interceptors
        val meta = RecordMetadata(
            topicPartition = tp,
            baseOffset = 0,
            batchIndex = 0,
            timestamp = 0,
            serializedKeySize = 0,
            serializedValueSize = 0,
        )
        interceptors.onAcknowledgement(metadata = meta, exception = null)
        assertEquals(2, onAckCount)

        // verify that onAcknowledgement exceptions do not propagate
        interceptor1.injectOnAcknowledgementError(true)
        interceptors.onAcknowledgement(meta, null)
        assertEquals(4, onAckCount)

        interceptor2.injectOnAcknowledgementError(true)
        interceptors.onAcknowledgement(meta, null)
        assertEquals(6, onAckCount)
        interceptors.close()
    }

    @Test
    fun testOnAcknowledgementWithErrorChain() {
        val interceptor1 = AppendProducerInterceptor("One")
        val interceptorList = listOf(interceptor1)
        val interceptors = ProducerInterceptors(interceptorList)

        // verify that metadata contains both topic and partition
        interceptors.onSendError(
            producerRecord,
            TopicPartition(producerRecord.topic, producerRecord.partition!!),
            KafkaException("Test")
        )
        assertEquals(1, onErrorAckCount)
        assertEquals(1, onErrorAckWithTopicPartitionSetCount)

        // verify that metadata contains both topic and partition (because record already contains partition)
        interceptors.onSendError(producerRecord, null, KafkaException("Test"))
        assertEquals(2, onErrorAckCount)
        assertEquals(2, onErrorAckWithTopicPartitionSetCount)

        // if producer record does not contain partition, interceptor should get partition == -1
        val record2 = ProducerRecord<Int?, String?>(
            topic = "test2",
            partition = null,
            key = 1,
            value = "value",
        )
        interceptors.onSendError(
            record = record2,
            interceptTopicPartition = null,
            exception = KafkaException("Test"),
        )
        assertEquals(3, onErrorAckCount)
        assertEquals(3, onErrorAckWithTopicSetCount)
        assertEquals(2, onErrorAckWithTopicPartitionSetCount)

        // if producer record does not contain partition, but topic/partition is passed to
        // onSendError, then interceptor should get valid partition
        val reassignedPartition = producerRecord.partition!! + 1
        interceptors.onSendError(
            record = record2,
            interceptTopicPartition = TopicPartition(record2.topic, reassignedPartition),
            exception = KafkaException("Test"),
        )
        assertEquals(4, onErrorAckCount)
        assertEquals(4, onErrorAckWithTopicSetCount)
        assertEquals(3, onErrorAckWithTopicPartitionSetCount)

        // if both record and topic/partition are null, interceptor should not receive metadata
        interceptors.onSendError(null, null, KafkaException("Test"))
        assertEquals(5, onErrorAckCount)
        assertEquals(4, onErrorAckWithTopicSetCount)
        assertEquals(3, onErrorAckWithTopicPartitionSetCount)
        interceptors.close()
    }
}
