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

import java.util.*
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.KafkaFuture.BaseFunction
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [AdminClient.listOffsets] call.
 *
 * The API of this class is evolving, see [AdminClient] for details.
 */
@Evolving
data class ListOffsetsResult(
    private val futures: Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>
) {
    /**
     * Return a future which can be used to check the result for a given partition.
     */
    fun partitionResult(partition: TopicPartition): KafkaFuture<ListOffsetsResultInfo> {
        return futures[partition] ?: throw IllegalArgumentException(
            "List Offsets for partition \"$partition\" was not attempted"
        )
    }

    /**
     * Return a future which succeeds only if offsets for all specified partitions have been successfully
     * retrieved.
     */
    fun all(): KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> {
        return KafkaFuture.allOf(futures.values)
            .thenApply {
                val offsets: MutableMap<TopicPartition, ListOffsetsResultInfo> = HashMap(futures.size)
                for ((key, value) in futures) try {
                    offsets[key] = value.get()
                } catch (e: InterruptedException) {
                    // This should be unreachable, because allOf ensured that all the futures
                    // completed successfully.
                    throw RuntimeException(e)
                } catch (e: ExecutionException) {
                    throw RuntimeException(e)
                }
                offsets
            }
    }

    data class ListOffsetsResultInfo(
        val offset: Long,
        val timestamp: Long,
        val leaderEpoch: Int?
    ) {
        override fun toString(): String {
            return ("ListOffsetsResultInfo(offset=" + offset + ", timestamp=" + timestamp + ", leaderEpoch="
                    + leaderEpoch + ")")
        }
    }
}
