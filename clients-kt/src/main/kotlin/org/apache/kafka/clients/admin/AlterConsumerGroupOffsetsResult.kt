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

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.protocol.Errors

/**
 * The result of the [AdminClient.alterConsumerGroupOffsets] call.
 *
 * The API of this class is evolving, see [AdminClient] for details.
 */
@Evolving
class AlterConsumerGroupOffsetsResult internal constructor(
    private val future: KafkaFuture<Map<TopicPartition, Errors>>,
) {

    /**
     * Return a future which can be used to check the result for a given partition.
     */
    fun partitionResult(partition: TopicPartition): KafkaFuture<Unit> {
        val result = KafkaFutureImpl<Unit>()
        future.whenComplete { topicPartitions, throwable ->
            if (throwable != null) result.completeExceptionally(throwable)
            else if (!topicPartitions!!.containsKey(partition)) result.completeExceptionally(
                IllegalArgumentException(
                    "Alter offset for partition \"$partition\" was not attempted"
                )
            ) else {
                val error = topicPartitions[partition]!!
                if (error === Errors.NONE) result.complete(Unit)
                else result.completeExceptionally(error.exception!!)
            }
        }
        return result
    }

    /**
     * Return a future which succeeds if all the alter offsets succeed.
     */
    fun all(): KafkaFuture<Unit> {
        return future.thenApply { topicPartitionErrorsMap: Map<TopicPartition, Errors> ->
            val partitionsFailed = topicPartitionErrorsMap
                .filter { e: Map.Entry<TopicPartition, Errors> -> e.value !== Errors.NONE }
                .map(Map.Entry<TopicPartition, Errors>::key)

            topicPartitionErrorsMap.values.forEach { error ->
                if (error !== Errors.NONE) throw error.exception(
                    "Failed altering consumer group offsets for the following partitions: $partitionsFailed"
                )!!
            }
        }
    }
}
