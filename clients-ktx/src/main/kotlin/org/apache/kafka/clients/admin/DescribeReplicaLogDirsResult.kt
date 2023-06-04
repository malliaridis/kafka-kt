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
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.requests.DescribeLogDirsResponse

/**
 * The result of [Admin.describeReplicaLogDirs].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeReplicaLogDirsResult internal constructor(
    val futures: Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>>,
) {

    /**
     * Return a map from replica to future which can be used to check the log directory information
     * of individual replicas.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("futures")
    )
    fun values(): Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> = futures

    /**
     * Return a future which succeeds if log directory information of all replicas are available
     */
    fun all(): KafkaFuture<Map<TopicPartitionReplica, ReplicaLogDirInfo>> {
        return KafkaFuture.allOf(*futures.values.toTypedArray()).thenApply {
            val replicaLogDirInfos: MutableMap<TopicPartitionReplica, ReplicaLogDirInfo> = HashMap()
            futures.forEach { (key, future) ->
                try {
                    replicaLogDirInfos[key] = future.get()
                } catch (e: InterruptedException) {
                    // This should be unreachable, because allOf ensured that all the futures completed successfully.
                    throw RuntimeException(e)
                } catch (e: ExecutionException) {
                    throw RuntimeException(e)
                }
            }
            replicaLogDirInfos
        }
    }

    /**
     * @property currentReplicaLogDir The current log directory of the replica of this partition on
     * the given broker. Null if no replica is not found for this partition on the given broker.
     * @property currentReplicaOffsetLag Defined as max(HW of partition - LEO of the replica, 0).
     * @property futureReplicaLogDir The future log directory of the replica of this partition on
     * the given broker. Null if the replica of this partition is not being moved to another log
     * directory on the given broker.
     * @property futureReplicaOffsetLag The LEO of the replica - LEO of the future log of this
     * replica in the destination log directory. -1 if either there is not replica for this
     * partition or the replica of this partition is not being moved to another log directory on the
     * given broker.
     */
    class ReplicaLogDirInfo internal constructor(
        val currentReplicaLogDir: String? = null,
        val currentReplicaOffsetLag: Long = DescribeLogDirsResponse.INVALID_OFFSET_LAG,
        val futureReplicaLogDir: String? = null,
        val futureReplicaOffsetLag: Long = DescribeLogDirsResponse.INVALID_OFFSET_LAG
    ) {

        override fun toString(): String {
            return if (futureReplicaLogDir != null)
                "(currentReplicaLogDir=$currentReplicaLogDir" +
                        ", futureReplicaLogDir=$futureReplicaLogDir" +
                        ", futureReplicaOffsetLag=$futureReplicaOffsetLag" +
                        ")"
            else "ReplicaLogDirInfo(currentReplicaLogDir=$currentReplicaLogDir)"
        }
    }
}
