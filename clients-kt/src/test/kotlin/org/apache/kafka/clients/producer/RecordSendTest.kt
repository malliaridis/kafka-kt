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

import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.clients.producer.internals.ProduceRequestResult
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RecordSendTest {

    private val topicPartition = TopicPartition(topic = "test", partition = 0)

    private val baseOffset: Long = 45

    private val relOffset = 5

    /**
     * Test that waiting on a request that never completes times out
     */
    @Test
    @Throws(Exception::class)
    fun testTimeout() {
        val request = ProduceRequestResult(topicPartition)
        val future = FutureRecordMetadata(
            result = request,
            batchIndex = relOffset,
            createTimestamp = RecordBatch.NO_TIMESTAMP,
            serializedKeySize = 0,
            serializedValueSize = 0,
            time = Time.SYSTEM,
        )

        assertFalse(future.isDone(), "Request is not completed")
        assertFailsWith<TimeoutException> { future[5, TimeUnit.MILLISECONDS] }

        request[baseOffset, RecordBatch.NO_TIMESTAMP] = null
        request.done()

        assertTrue(future.isDone())
        assertEquals(baseOffset + relOffset, future.get().offset)
    }

    /**
     * Test that an asynchronous request will eventually throw the right exception
     */
    @Test
    @Throws(Exception::class)
    fun testError() {
        val future = FutureRecordMetadata(
            result = asyncRequest(
                baseOffset = baseOffset,
                error = CorruptRecordException(),
                timeout = 50L,
            ),
            batchIndex = relOffset,
            createTimestamp = RecordBatch.NO_TIMESTAMP,
            serializedKeySize = 0,
            serializedValueSize = 0,
            time = Time.SYSTEM,
        )

        assertFailsWith<ExecutionException> { future.get() }
    }

    /**
     * Test that an asynchronous request will eventually return the right offset
     */
    @Test
    @Throws(Exception::class)
    fun testBlocking() {
        val future = FutureRecordMetadata(
            result = asyncRequest(
                baseOffset = baseOffset,
                error = null,
                timeout = 50L,
            ),
            batchIndex = relOffset,
            createTimestamp = RecordBatch.NO_TIMESTAMP,
            serializedKeySize = 0,
            serializedValueSize = 0,
            time = Time.SYSTEM,
        )

        assertEquals(baseOffset + relOffset, future.get().offset)
    }

    /* create a new request result that will be completed after the given timeout */
    fun asyncRequest(baseOffset: Long, error: RuntimeException?, timeout: Long): ProduceRequestResult {
        val request = ProduceRequestResult(topicPartition)
        val thread = object : Thread() {
            override fun run() {
                try {
                    sleep(timeout)

                    if (error == null) request[baseOffset, RecordBatch.NO_TIMESTAMP] = null
                    else request[-1L, RecordBatch.NO_TIMESTAMP] = { error }

                    request.done()
                } catch (_: InterruptedException) {
                }
            }
        }
        thread.start()
        return request
    }
}
