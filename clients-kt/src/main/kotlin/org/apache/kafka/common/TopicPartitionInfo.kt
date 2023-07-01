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

package org.apache.kafka.common

import org.apache.kafka.common.utils.Utils

/**
 * A class containing leadership, replicas and ISR information for a topic partition.
 *
 * @property partition the partition id
 * @property leader the leader of the partition or [Node.noNode] if there is none.
 * @property replicas the replicas of the partition in the same order as the replica assignment (the
 * preferred replica is the head of the list)
 * @property inSyncReplicas the in-sync replicas
 */
data class TopicPartitionInfo(
    val partition: Int,
    val leader: Node?,
    val replicas: List<Node>,
    val inSyncReplicas: List<Node>,
) {

    /**
     * Return the partition id.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("partition"),
    )
    fun partition(): Int {
        return partition
    }

    /**
     * Return the leader of the partition or null if there is none.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("leader"),
    )
    fun leader(): Node? {
        return leader
    }

    /**
     * Return the replicas of the partition in the same order as the replica assignment. The
     * preferred replica is the head of the list.
     *
     * Brokers with version lower than 0.11.0.0 return the replicas in unspecified order due to a bug.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("replicas"),
    )
    fun replicas(): List<Node>? {
        return replicas
    }

    /**
     * Return the in-sync replicas of the partition. Note that the ordering of the result is
     * unspecified.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("inSyncReplicas"),
    )
    fun isr(): List<Node> {
        return inSyncReplicas
    }

    override fun toString(): String {
        return "(partition=$partition" +
                ", leader=$leader" +
                ", replicas=${Utils.join(replicas, ", ")}" +
                ", isr=${Utils.join(inSyncReplicas, ", ")})"
    }
}
