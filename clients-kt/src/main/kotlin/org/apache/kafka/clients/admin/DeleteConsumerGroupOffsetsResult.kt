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
 * The result of the [Admin.deleteConsumerGroupOffsets] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DeleteConsumerGroupOffsetsResult internal constructor(
    private val future: KafkaFuture<Map<TopicPartition, Errors>>,
    private val partitions: Set<TopicPartition>,
) {

    /**
     * Return a future which can be used to check the result for a given partition.
     */
    fun partitionResult(partition: TopicPartition): KafkaFuture<Unit> {
        require(partitions.contains(partition)) {
            "Partition $partition was not included in the original request"
        }

        val result = KafkaFutureImpl<Unit>()
        future.whenComplete { topicPartitions, throwable ->
            if (throwable != null) result.completeExceptionally(throwable)
            else if (!maybeCompleteExceptionally(topicPartitions!!, partition, result))
                result.complete(Unit)
        }
        return result
    }

    /**
     * Return a future which succeeds only if all the deletions succeed.
     * If not, the first partition error shall be returned.
     */
    fun all(): KafkaFuture<Unit> {
        val result = KafkaFutureImpl<Unit>()
        future.whenComplete { topicPartitions, throwable ->
            if (throwable != null) result.completeExceptionally(throwable)
            else {
                val completedExceptionally = partitions.any { partition ->
                    maybeCompleteExceptionally(topicPartitions!!, partition, result)
                }
                if (completedExceptionally) return@whenComplete
                result.complete(Unit)
            }
        }
        return result
    }

    private fun maybeCompleteExceptionally(
        partitionLevelErrors: Map<TopicPartition, Errors>,
        partition: TopicPartition,
        result: KafkaFutureImpl<Unit>,
    ): Boolean {
        val exception = KafkaAdminClient.getSubLevelError(
            subLevelErrors = partitionLevelErrors,
            subKey = partition,
            keyNotFoundMsg = "Offset deletion result for partition \"$partition\" was not " +
                    "included in the response"
        )
        return if (exception != null) {
            result.completeExceptionally(exception)
            true
        } else false
    }
}
