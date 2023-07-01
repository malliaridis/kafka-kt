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
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

@Evolving
class DescribeProducersResult internal constructor(
    private val futures: Map<TopicPartition, KafkaFuture<PartitionProducerState>>,
) {

    fun partitionResult(partition: TopicPartition): KafkaFuture<PartitionProducerState> {
        return requireNotNull(futures[partition]) {
            "Topic partition $partition was not included in the request"
        }
    }

    fun all(): KafkaFuture<Map<TopicPartition, PartitionProducerState>> {
        return KafkaFuture.allOf(*futures.values.toTypedArray())
            .thenApply {
                val results: MutableMap<TopicPartition, PartitionProducerState> =
                    HashMap(futures.size)

                futures.forEach { entry ->
                    try {
                        results[entry.key] = entry.value.get()
                    } catch (e: InterruptedException) {
                        // This should be unreachable, because allOf ensured that all the futures
                        // completed successfully.
                        throw KafkaException(cause = e)
                    } catch (e: ExecutionException) {
                        throw KafkaException(cause = e)
                    }
                }
                results
            }
    }

    class PartitionProducerState(private val activeProducers: List<ProducerState>) {
        fun activeProducers(): List<ProducerState> {
            return activeProducers
        }

        override fun toString(): String {
            return ("PartitionProducerState(" +
                    "activeProducers=" + activeProducers +
                    ')')
        }
    }
}
