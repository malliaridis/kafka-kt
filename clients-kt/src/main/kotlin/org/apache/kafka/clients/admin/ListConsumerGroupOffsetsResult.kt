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

import java.util.concurrent.ExecutionException
import java.util.function.Function
import java.util.stream.Collectors
import org.apache.kafka.clients.admin.internals.CoordinatorKey
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [Admin.listConsumerGroupOffsets] and [Admin.listConsumerGroupOffsets] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class ListConsumerGroupOffsetsResult internal constructor(
    futures: Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata?>>>
) {

    val futures: Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata?>>>

    init {
        this.futures = futures.mapKeys { (key, _) -> key.idValue }.toMap()
    }

    /**
     * Return a future which yields a map of topic partitions to OffsetAndMetadata objects. If the
     * group does not have a committed offset for this partition, the corresponding value in the
     * returned map will be `null`.
     */
    fun partitionsToOffsetAndMetadata(): KafkaFuture<Map<TopicPartition, OffsetAndMetadata?>> {
        check(futures.size == 1) {
            "Offsets from multiple consumer groups were requested. " +
                    "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a " +
                    "specific group."
        }
        return futures.values.iterator().next()
    }

    /**
     * Return a future which yields a map of topic partitions to OffsetAndMetadata objects for the
     * specified group. If the group doesn't have a committed offset for a specific partition, the
     * corresponding value in the returned map will be `null`.
     */
    fun partitionsToOffsetAndMetadata(
        groupId: String,
    ): KafkaFuture<Map<TopicPartition, OffsetAndMetadata?>> {
        return requireNotNull(futures[groupId]) {
            "Offsets for consumer group '$groupId' were not requested."
        }
    }

    /**
     * Return a future which yields all `Map<String, Map<TopicPartition, OffsetAndMetadata>`
     * objects, if requests for all the groups succeed.
     */
    fun all(): KafkaFuture<Map<String, Map<TopicPartition, OffsetAndMetadata?>>> {
        return KafkaFuture.allOf(*futures.values.toTypedArray()).thenApply {
            val listedConsumerGroupOffsets: MutableMap<String, Map<TopicPartition, OffsetAndMetadata?>> =
                HashMap(futures.size)

            futures.forEach { (key, future) ->
                try {
                    listedConsumerGroupOffsets[key] = future.get()
                } catch (e: InterruptedException) {
                    // This should be unreachable, since the KafkaFuture#allOf already ensured
                    // that all the futures completed successfully.
                    throw RuntimeException(e)
                } catch (e: ExecutionException) {
                    throw RuntimeException(e)
                }
            }
            listedConsumerGroupOffsets
        }
    }
}
