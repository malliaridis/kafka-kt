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
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeLogDirsResponse
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo
import java.util.concurrent.ExecutionException
import java.util.function.Function
import java.util.stream.Collectors

/**
 * The result of the [Admin.describeLogDirs] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeLogDirsResult internal constructor(
    private val futures: Map<Int, KafkaFuture<Map<String, LogDirDescription>>>,
) {

    /**
     * Return a map from brokerId to future which can be used to check the information of partitions
     * on each individual broker.
     */
    @Suppress("deprecation")
    @Deprecated("Deprecated Since Kafka 2.7. Use {@link #descriptions()}.")
    fun values(): Map<Int, KafkaFuture<Map<String, LogDirInfo>>> =
        descriptions().mapValues { (_, value) ->
            value.thenApply { map -> convertMapValues(map) }
        }

    @Suppress("deprecation")
    private fun convertMapValues(map: Map<String, LogDirDescription>): Map<String, LogDirInfo> {
        return map.mapValues { (_, logDir) ->
            LogDirInfo(
                error = logDir.error()?.let { Errors.forException(it) } ?: Errors.NONE,
                replicaInfos = logDir.replicaInfos().mapValues { (_, value) ->
                    DescribeLogDirsResponse.ReplicaInfo(
                        size = value.size(),
                        offsetLag = value.offsetLag,
                        isFuture = value.isFuture,
                    )
                },
            )
        }
    }

    /**
     * Return a map from brokerId to future which can be used to check the information of partitions
     * on each individual broker. The result of the future is a map from broker log directory path
     * to a description of that log directory.
     */
    fun descriptions(): Map<Int, KafkaFuture<Map<String, LogDirDescription>>> = futures

    /**
     * Return a future which succeeds only if all the brokers have responded without error
     */
    @Suppress("deprecation")
    @Deprecated("Deprecated Since Kafka 2.7. Use {@link #allDescriptions()}.")
    fun all(): KafkaFuture<Map<Int, Map<String, LogDirInfo>>> {
        return allDescriptions().thenApply { map ->
            map.mapValues { (_, value) -> convertMapValues(value) }
        }
    }

    /**
     * Return a future which succeeds only if all the brokers have responded without error. The
     * result of the future is a map from brokerId to a map from broker log directory path to a
     * description of that log directory.
     */
    fun allDescriptions(): KafkaFuture<Map<Int, Map<String, LogDirDescription>>> =
        KafkaFuture.allOf(futures.values).thenApply {
            futures.mapValues { (_, value) -> value.get() }
        }
}
