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
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class RecordMetadataTest {

    @Test
    fun testConstructionWithMissingBatchIndex() {
        val tp = TopicPartition(topic = "foo", partition = 0)
        val timestamp = 2340234L
        val keySize = 3
        val valueSize = 5
        val metadata = RecordMetadata(
            topicPartition = tp,
            baseOffset = -1L,
            batchIndex = -1,
            timestamp = timestamp,
            serializedKeySize = keySize,
            serializedValueSize = valueSize,
        )

        assertEquals(tp.topic, metadata.topic)
        assertEquals(tp.partition, metadata.partition)
        assertEquals(timestamp, metadata.timestamp)
        assertFalse(metadata.hasOffset())
        assertEquals(-1L, metadata.offset)
        assertEquals(keySize, metadata.serializedKeySize)
        assertEquals(valueSize, metadata.serializedValueSize)
    }

    @Test
    fun testConstructionWithBatchIndexOffset() {
        val tp = TopicPartition(topic = "foo", partition = 0)
        val timestamp = 2340234L
        val keySize = 3
        val valueSize = 5
        val baseOffset = 15L
        val batchIndex = 3
        val metadata = RecordMetadata(
            tp,
            baseOffset,
            batchIndex,
            timestamp,
            keySize,
            valueSize,
        )

        assertEquals(tp.topic, metadata.topic)
        assertEquals(tp.partition, metadata.partition)
        assertEquals(timestamp, metadata.timestamp)
        assertEquals(baseOffset + batchIndex, metadata.offset)
        assertEquals(keySize, metadata.serializedKeySize)
        assertEquals(valueSize, metadata.serializedValueSize)
    }

    @Test
    @Deprecated("")
    @Disabled("Kotlin Migration: Checksum was not migrated in new code")
    fun testConstructionWithChecksum() {
//        val tp = TopicPartition("foo", 0)
//        val timestamp = 2340234L
//        val baseOffset = 15L
//        val batchIndex = 3L
//        val keySize = 3
//        val valueSize = 5
//        var metadata = RecordMetadata(
//            topicPartition = tp,
//            baseOffset = baseOffset,
//            batchIndex = batchIndex.toInt(),
//            timestamp = timestamp,
//            checksum = null,
//            serializedKeySize = keySize,
//            serializedValueSize = valueSize,
//        )
//        assertEquals(tp.topic, metadata.topic)
//        assertEquals(tp.partition, metadata.partition)
//        assertEquals(timestamp, metadata.timestamp)
//        assertEquals(baseOffset + batchIndex, metadata.offset)
//        assertEquals(keySize, metadata.serializedKeySize)
//        assertEquals(valueSize, metadata.serializedValueSize)
//        val checksum = 133424L
//        metadata = RecordMetadata(
//            topicPartition = tp,
//            baseOffset = baseOffset,
//            batchIndex = batchIndex.toInt(),
//            timestamp = timestamp,
//            checksum = checksum.toInt(),
//            serializedKeySize = keySize,
//            serializedValueSize = valueSize,
//        )
//        assertEquals(tp.topic, metadata.topic)
//        assertEquals(tp.partition, metadata.partition)
//        assertEquals(timestamp, metadata.timestamp)
//        assertEquals(baseOffset + batchIndex, metadata.offset)
//        assertEquals(keySize, metadata.serializedKeySize)
//        assertEquals(valueSize, metadata.serializedValueSize)
    }
}
