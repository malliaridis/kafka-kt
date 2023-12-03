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

import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class FutureRecordMetadataTest {

    private val time = MockTime()

    @Test
    @Throws(
        ExecutionException::class,
        InterruptedException::class,
        TimeoutException::class,
    )
    fun testFutureGetWithSeconds() {
        val produceRequestResult = mockProduceRequestResult()
        val future = futureRecordMetadata(produceRequestResult)
        val chainedProduceRequestResult = mockProduceRequestResult()
        future.chain(futureRecordMetadata(chainedProduceRequestResult))
        future[1L, TimeUnit.SECONDS]

        verify(produceRequestResult).await(1L, TimeUnit.SECONDS)
        verify(chainedProduceRequestResult).await(1000L, TimeUnit.MILLISECONDS)
    }

    @Test
    @Throws(
        ExecutionException::class,
        InterruptedException::class,
        TimeoutException::class
    )
    fun testFutureGetWithMilliSeconds() {
        val produceRequestResult = mockProduceRequestResult()
        val future = futureRecordMetadata(produceRequestResult)
        val chainedProduceRequestResult = mockProduceRequestResult()
        future.chain(futureRecordMetadata(chainedProduceRequestResult))
        future[1000L, TimeUnit.MILLISECONDS]

        verify(produceRequestResult).await(1000L, TimeUnit.MILLISECONDS)
        verify(chainedProduceRequestResult).await(1000L, TimeUnit.MILLISECONDS)
    }

    private fun futureRecordMetadata(produceRequestResult: ProduceRequestResult): FutureRecordMetadata =
        FutureRecordMetadata(
            result = produceRequestResult,
            batchIndex = 0,
            createTimestamp = RecordBatch.NO_TIMESTAMP,
            serializedKeySize = 0,
            serializedValueSize = 0,
            time = time,
        )

    @Throws(InterruptedException::class)
    private fun mockProduceRequestResult(): ProduceRequestResult {
        val mockProduceRequestResult = mock<ProduceRequestResult>()
        // Kotlin Migration - Include topic partition in mock sicne value may not be null
        val mockTopicPartition = mock<TopicPartition>()

        whenever(mockProduceRequestResult.await(any(), any())).thenReturn(true)
        whenever(mockProduceRequestResult.topicPartition).thenReturn(mockTopicPartition)

        return mockProduceRequestResult
    }
}
