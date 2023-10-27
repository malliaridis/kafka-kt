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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.test.TestUtils.assertFutureError
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull

class DeleteConsumerGroupOffsetsResultTest {
    
    private val topic = "topic"
    
    private val tpZero = TopicPartition(topic, 0)
    
    private val tpOne = TopicPartition(topic, 1)
    
    private lateinit var partitions: MutableSet<TopicPartition>
    
    private lateinit var errorsMap: MutableMap<TopicPartition, Errors>
    
    private lateinit var partitionFutures: KafkaFutureImpl<Map<TopicPartition, Errors>>
    
    @BeforeEach
    fun setUp() {
        partitionFutures = KafkaFutureImpl()
        partitions = HashSet()
        partitions.add(tpZero)
        partitions.add(tpOne)
        errorsMap = HashMap()
        errorsMap[tpZero] = Errors.NONE
        errorsMap[tpOne] = Errors.UNKNOWN_TOPIC_OR_PARTITION
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTopLevelErrorConstructor() {
        partitionFutures.completeExceptionally(Errors.GROUP_AUTHORIZATION_FAILED.exception!!)
        val topLevelErrorResult = DeleteConsumerGroupOffsetsResult(partitionFutures, partitions)
        assertFutureError(topLevelErrorResult.all(), GroupAuthorizationException::class.java)
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testPartitionLevelErrorConstructor() {
        createAndVerifyPartitionLevelErrror()
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testPartitionMissingInResponseErrorConstructor() {
        errorsMap.remove(tpOne)
        partitionFutures.complete(errorsMap)
        assertFalse(partitionFutures.isCompletedExceptionally)
        val missingPartitionResult = DeleteConsumerGroupOffsetsResult(partitionFutures, partitions)
        assertFutureError(missingPartitionResult.all(), IllegalArgumentException::class.java)
        assertNull(missingPartitionResult.partitionResult(tpZero).get())
        assertFutureError(missingPartitionResult.partitionResult(tpOne), IllegalArgumentException::class.java)
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testPartitionMissingInRequestErrorConstructor() {
        val partitionLevelErrorResult = createAndVerifyPartitionLevelErrror()
        assertFailsWith<IllegalArgumentException> {
            partitionLevelErrorResult.partitionResult(
                TopicPartition(topic = "invalid-topic", partition = 0)
            )
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testNoErrorConstructor() {
        val errorsMap: MutableMap<TopicPartition, Errors> = HashMap()
        errorsMap[tpZero] = Errors.NONE
        errorsMap[tpOne] = Errors.NONE
        val noErrorResult = DeleteConsumerGroupOffsetsResult(partitionFutures, partitions)
        partitionFutures.complete(errorsMap)
        assertNull(noErrorResult.all().get())
        assertNull(noErrorResult.partitionResult(tpZero).get())
        assertNull(noErrorResult.partitionResult(tpOne).get())
    }

    @Throws(InterruptedException::class, ExecutionException::class)
    private fun createAndVerifyPartitionLevelErrror(): DeleteConsumerGroupOffsetsResult {
        partitionFutures.complete(errorsMap)
        assertFalse(partitionFutures.isCompletedExceptionally)
        val partitionLevelErrorResult = DeleteConsumerGroupOffsetsResult(
            partitionFutures,
            partitions
        )
        assertFutureError(
            partitionLevelErrorResult.all(),
            UnknownTopicOrPartitionException::class.java
        )
        assertNull(partitionLevelErrorResult.partitionResult(tpZero).get())
        assertFutureError(
            partitionLevelErrorResult.partitionResult(tpOne),
            UnknownTopicOrPartitionException::class.java
        )
        return partitionLevelErrorResult
    }
}
