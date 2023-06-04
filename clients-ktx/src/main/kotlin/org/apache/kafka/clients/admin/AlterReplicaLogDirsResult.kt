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
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of [Admin.alterReplicaLogDirs].
 *
 * To retrieve the detailed result per specified [TopicPartitionReplica], use [.values]. To retrieve the
 * overall result only, use [.all].
 */
@Evolving
class AlterReplicaLogDirsResult internal constructor(
    val futures: Map<TopicPartitionReplica, KafkaFuture<Unit>>,
) {

    /**
     * Return a map from [TopicPartitionReplica] to [KafkaFuture] which holds the status of individual
     * replica movement.
     *
     * To check the result of individual replica movement, call [KafkaFuture.get] from the value contained
     * in the returned map. If there is no error, it will return silently; if not, an [Exception] will be thrown
     * like the following:
     *
     * - [CancellationException]: The task was canceled.
     * - [InterruptedException]: Interrupted while joining I/O thread.
     * - [ExecutionException]: Execution failed with the following causes:
     *
     * - [ClusterAuthorizationException]: Authorization failed. (CLUSTER_AUTHORIZATION_FAILED, 31)
     * - [InvalidTopicException]: The specified topic name is too long. (INVALID_TOPIC_EXCEPTION, 17)
     * - [LogDirNotFoundException]: The specified log directory is not found in the broker. (LOG_DIR_NOT_FOUND, 57)
     * - [ReplicaNotAvailableException]: The replica does not exist on the broker. (REPLICA_NOT_AVAILABLE, 9)
     * - [KafkaStorageException]: Disk error occurred. (KAFKA_STORAGE_ERROR, 56)
     * - [UnknownServerException]: Unknown. (UNKNOWN_SERVER_ERROR, -1)
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("futures")
    )
    fun values(): Map<TopicPartitionReplica, KafkaFuture<Unit>> = futures

    /**
     * Return a [KafkaFuture] which succeeds on [KafkaFuture.get] if all the replica movement have succeeded.
     * if not, it throws an [Exception] described in [.values] method.
     */
    fun all(): KafkaFuture<Unit> {
        return KafkaFuture.allOf(*futures.values.toTypedArray())
    }
}
