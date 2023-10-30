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

import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class ProducerRecordTest {

    @Test
    fun testEqualsAndHashCode() {
        val producerRecord = ProducerRecord(topic = "test", partition = 1, key = "key", value = 1)

        assertEquals(producerRecord, producerRecord)
        assertEquals(producerRecord.hashCode(), producerRecord.hashCode())

        val equalRecord = ProducerRecord(topic = "test", partition = 1, key = "key", value = 1)

        assertEquals(producerRecord, equalRecord)
        assertEquals(producerRecord.hashCode(), equalRecord.hashCode())

        val topicMisMatch = ProducerRecord(topic = "test-1", partition = 1, key = "key", value = 1)

        assertNotEquals(producerRecord, topicMisMatch)

        val partitionMismatch = ProducerRecord(topic = "test", partition = 2, key = "key", value = 1)

        assertNotEquals(producerRecord, partitionMismatch)

        val keyMisMatch = ProducerRecord(topic = "test", partition = 1, key = "key-1", value = 1)

        assertNotEquals(producerRecord, keyMisMatch)

        val valueMisMatch = ProducerRecord(topic = "test", partition = 1, key = "key", value = 2)

        assertNotEquals(producerRecord, valueMisMatch)

        val nullFieldsRecord = ProducerRecord(
            topic = "topic",
            partition = null,
            timestamp = null,
            key = null,
            value = null,
            headers = RecordHeaders(),
        )

        assertEquals(nullFieldsRecord, nullFieldsRecord)
        assertEquals(nullFieldsRecord.hashCode(), nullFieldsRecord.hashCode())
    }

    @Test
    fun testInvalidRecords() {
        // Kotlin Migration: Topic is not nullable in Kotlin
//        assertFailsWith<IllegalArgumentException> {
//            ProducerRecord(topic = null, partition =  0, key = "key", value =  1)
//        }
        assertFailsWith<IllegalArgumentException>(
            message = "Expected IllegalArgumentException to be raised because of negative timestamp",
        ) { ProducerRecord(topic = "test", partition = 0, timestamp = -1L, key = "key", value = 1) }

        assertFailsWith<IllegalArgumentException>(
            message = "Expected IllegalArgumentException to be raised because of negative partition",
        ) {
            ProducerRecord(topic = "test", partition = -1, key = "key", value = 1)
        }
    }
}
