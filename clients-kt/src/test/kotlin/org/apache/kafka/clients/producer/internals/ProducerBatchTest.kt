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

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.concurrent.ExecutionException
import org.junit.jupiter.api.Disabled
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProducerBatchTest {
    
    private val now = 1488748346917L
    
    private val memoryRecordsBuilder: MemoryRecordsBuilder = MemoryRecords.builder(
        buffer = ByteBuffer.allocate(512),
        compressionType = CompressionType.NONE,
        timestampType = TimestampType.CREATE_TIME,
        baseOffset = 128,
    )

    @Test
    @Throws(Exception::class)
    fun testBatchAbort() {
        val batch = ProducerBatch(TopicPartition("topic", 1), memoryRecordsBuilder, now)
        val callback = MockCallback()
        val future = batch.tryAppend(now, null, ByteArray(10), Record.EMPTY_HEADERS, callback, now)
        val exception = KafkaException()
        batch.abort(exception)

        assertTrue(future!!.isDone())
        assertEquals(1, callback.invocations)
        assertEquals(exception, callback.exception)
        assertNull(callback.metadata)

        // subsequent completion should be ignored
        assertFalse(batch.complete(500L, 2342342341L))
        assertFalse(batch.completeExceptionally(KafkaException()) { KafkaException() })
        assertEquals(1, callback.invocations)
        assertTrue(future.isDone())

        val e = assertFailsWith<ExecutionException>("Future should have thrown") { future.get() }
        assertEquals(exception, e.cause)
    }

    @Test
    @Throws(Exception::class)
    fun testBatchCannotAbortTwice() {
        val batch = ProducerBatch(
            topicPartition = TopicPartition(topic = "topic", partition = 1),
            recordsBuilder = memoryRecordsBuilder,
            createdMs = now,
        )
        val callback = MockCallback()
        val future = batch.tryAppend(
            timestamp = now,
            key = null,
            value = ByteArray(10),
            headers = Record.EMPTY_HEADERS,
            callback = callback,
            now = now,
        )
        val exception = KafkaException()
        batch.abort(exception)

        assertEquals(1, callback.invocations)
        assertEquals(exception, callback.exception)
        assertNull(callback.metadata)
        assertFailsWith<IllegalStateException>(
            message = "Expected exception from abort",
        ) { batch.abort(KafkaException()) }
        assertEquals(1, callback.invocations)
        assertTrue(future!!.isDone())
        val e = assertFailsWith<ExecutionException>("Future should have thrown") { future.get() }
        assertEquals(exception, e.cause)
    }

    @Test
    @Throws(Exception::class)
    fun testBatchCannotCompleteTwice() {
        val batch = ProducerBatch(
            topicPartition = TopicPartition(topic = "topic", partition = 1),
            recordsBuilder = memoryRecordsBuilder,
            createdMs = now,
        )
        val callback = MockCallback()
        val future = batch.tryAppend(
            timestamp = now,
            key = null,
            value = ByteArray(10),
            headers = Record.EMPTY_HEADERS,
            callback = callback,
            now = now,
        )
        batch.complete(500L, 10L)

        assertEquals(1, callback.invocations)
        assertNull(callback.exception)
        assertNotNull(callback.metadata)
        assertFailsWith<IllegalStateException> { batch.complete(baseOffset = 1000L, logAppendTime = 20L) }

        val recordMetadata = future!!.get()

        assertEquals(500L, recordMetadata?.offset)
        assertEquals(10L, recordMetadata?.timestamp)
    }

    @Test
    fun testSplitPreservesHeaders() {
        for (compressionType in CompressionType.entries) {
            val builder = MemoryRecords.builder(
                buffer = ByteBuffer.allocate(1024),
                magic = RecordBatch.MAGIC_VALUE_V2,
                compressionType = compressionType,
                timestampType = TimestampType.CREATE_TIME,
                baseOffset = 0L,
            )
            val batch = ProducerBatch(
                topicPartition = TopicPartition(topic = "topic", partition = 1),
                recordsBuilder = builder,
                createdMs = now,
            )
            val header = RecordHeader(
                key = "header-key",
                value = "header-value".toByteArray(),
            )
            while (true) {
                val future = batch.tryAppend(
                    timestamp = now,
                    key = "hi".toByteArray(),
                    value = "there".toByteArray(),
                    headers = arrayOf(header),
                    callback = null,
                    now = now,
                ) ?: break
            }
            val batches = batch.split(200)

            assertTrue(batches.size >= 2, "This batch should be split to multiple small batches.")

            for (splitProducerBatch in batches) {
                for (splitBatch in splitProducerBatch!!.records().batches()) {
                    for (record in splitBatch) {
                        assertEquals(
                            expected = 1,
                            actual = record.headers().size,
                            message = "Header size should be 1.",
                        )
                        assertEquals(
                            expected = "header-key",
                            actual = record.headers()[0].key,
                            message = "Header key should be 'header-key'.",
                        )
                        assertEquals(
                            expected = "header-value",
                            actual = String(record.headers()[0].value!!),
                            message = "Header value should be 'header-value'.",
                        )
                    }
                }
            }
        }
    }

    @Test
    fun testSplitPreservesMagicAndCompressionType() {
        listOf(
            RecordBatch.MAGIC_VALUE_V0,
            RecordBatch.MAGIC_VALUE_V1,
            RecordBatch.MAGIC_VALUE_V2
        ).forEach outer@ { magic ->
            CompressionType.entries.forEach { compressionType ->
                if (compressionType === CompressionType.NONE && magic < RecordBatch.MAGIC_VALUE_V2) return@forEach
                if (compressionType === CompressionType.ZSTD && magic < RecordBatch.MAGIC_VALUE_V2) return@forEach
                val builder = MemoryRecords.builder(
                    ByteBuffer.allocate(1024), magic,
                    compressionType, TimestampType.CREATE_TIME, 0L
                )
                val batch = ProducerBatch(TopicPartition("topic", 1), builder, now)
                while (true) {
                    val future = batch.tryAppend(
                        timestamp = now,
                        key = "hi".toByteArray(),
                        value = "there".toByteArray(),
                        headers = Record.EMPTY_HEADERS,
                        callback = null,
                        now = now,
                    ) ?: break
                }
                val batches = batch.split(512)

                assertTrue(batches.size >= 2)

                batches.forEach { splitProducerBatch ->
                    assertEquals(magic, splitProducerBatch!!.magic)
                    assertTrue(splitProducerBatch.isSplitBatch)

                    for (splitBatch in splitProducerBatch.records().batches()) {
                        assertEquals(magic, splitBatch.magic())
                        assertEquals(0L, splitBatch.baseOffset())
                        assertEquals(compressionType, splitBatch.compressionType())
                    }
                }
            }
        }
    }

    /**
     * A [ProducerBatch] configured using a timestamp preceding its create time is interpreted correctly
     * as not expired by [ProducerBatch.hasReachedDeliveryTimeout].
     */
    @Test
    fun testBatchExpiration() {
        val deliveryTimeoutMs: Long = 10240
        val batch = ProducerBatch(
            topicPartition = TopicPartition(topic = "topic", partition = 1),
            recordsBuilder = memoryRecordsBuilder,
            createdMs = now,
        )

        // Set `now` to 2ms before the create time.
        assertFalse(batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now - 2))
        // Set `now` to deliveryTimeoutMs.
        assertTrue(batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now + deliveryTimeoutMs))
    }

    /**
     * A [ProducerBatch] configured using a timestamp preceding its create time is interpreted correctly
     * * as not expired by [ProducerBatch.hasReachedDeliveryTimeout].
     */
    @Test
    fun testBatchExpirationAfterReenqueue() {
        val batch = ProducerBatch(
            topicPartition = TopicPartition(topic = "topic", partition = 1),
            recordsBuilder = memoryRecordsBuilder,
            createdMs = now,
        )
        // Set batch.retry = true
        batch.reenqueued(now)

        // Set `now` to 2ms before the create time.
        assertFalse(batch.hasReachedDeliveryTimeout(10240, now - 2L))
    }

    @Test
    fun testShouldNotAttemptAppendOnceRecordsBuilderIsClosedForAppends() {
        val batch = ProducerBatch(
            topicPartition = TopicPartition(topic = "topic", partition = 1),
            recordsBuilder = memoryRecordsBuilder,
            createdMs = now,
        )
        val result0 = batch.tryAppend(
            timestamp = now,
            key = null,
            value = ByteArray(10),
            headers = Record.EMPTY_HEADERS,
            callback = null,
            now = now,
        )

        assertNotNull(result0)
        assertTrue(memoryRecordsBuilder.hasRoomFor(
            timestamp = now,
            key = null,
            value = ByteArray(10),
            headers = Record.EMPTY_HEADERS,
        ))

        memoryRecordsBuilder.closeForRecordAppends()

        assertFalse(
            memoryRecordsBuilder.hasRoomFor(
                timestamp = now,
                key = null,
                value = ByteArray(10),
                headers = Record.EMPTY_HEADERS,
            )
        )
        assertNull(
            batch.tryAppend(
                timestamp = now + 1,
                key = null,
                value = ByteArray(10),
                headers = Record.EMPTY_HEADERS,
                callback = null,
                now = now + 1,
            )
        )
    }

    @Test
    fun testCompleteExceptionallyWithRecordErrors() {
        val recordCount = 5
        val topLevelException = RuntimeException()
        val recordExceptionMap = mapOf(
            0 to RuntimeException(),
            3 to RuntimeException(),
        )
        val recordExceptions: (Int) -> RuntimeException = { batchIndex ->
            recordExceptionMap.getOrDefault(batchIndex, topLevelException)
        }
        testCompleteExceptionally(recordCount, topLevelException, recordExceptions)
    }

    @Test
    @Disabled("Kotlin Migration - ProducerBatch.completeExceptionally does not accept null values for recordExceptions.")
    fun testCompleteExceptionallyWithNullRecordErrors() {
//        val recordCount = 5
//        val topLevelException = RuntimeException()
//        assertFailsWith<NullPointerException> {
//            testCompleteExceptionally(recordCount, topLevelException, null)
//        }
    }

    private fun testCompleteExceptionally(
        recordCount: Int,
        topLevelException: RuntimeException,
        recordExceptions: (Int) -> RuntimeException?,
    ) {
        val batch = ProducerBatch(
            topicPartition = TopicPartition("topic", 1),
            recordsBuilder = memoryRecordsBuilder,
            createdMs = now,
        )
        val futures = List(recordCount) {
            batch.tryAppend(
                timestamp = now,
                key = null,
                value = ByteArray(10),
                headers = Record.EMPTY_HEADERS,
                callback = null,
                now = now
            )
        }
        assertEquals(recordCount, batch.recordCount)
        batch.completeExceptionally(topLevelException, recordExceptions)
        assertTrue(batch.isDone)
        futures.indices.forEach { i ->
            val future = futures[i]
            val caughtException = assertFutureThrows(
                future = future!!,
                exceptionCauseClass = RuntimeException::class.java,
            )
            val expectedException = recordExceptions!!.invoke(i)
            assertEquals(expectedException, caughtException)
        }
    }

    private class MockCallback : Callback {
        
        var invocations = 0
        
        var metadata: RecordMetadata? = null
        
        var exception: Exception? = null
        
        override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
            invocations++
            this.metadata = metadata
            this.exception = exception
        }
    }
}
