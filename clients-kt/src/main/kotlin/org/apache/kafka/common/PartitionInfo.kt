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

/**
 * This is used to describe per-partition state in the MetadataResponse.
 */
data class PartitionInfo(
    val topic: String,
    val partition: Int,
    val leader: Node?,
    val replicas: List<Node>,
    val inSyncReplicas: List<Node>,
    val offlineReplicas: List<Node> = emptyList(),
) {

    @Deprecated("Use primary constructor instead")
    constructor(
        topic: String,
        partition: Int,
        leader: Node?,
        replicas: Array<Node>,
        inSyncReplicas: Array<Node>,
        offlineReplicas: Array<Node> = emptyArray(),
    ) : this(
        topic = topic,
        partition = partition,
        leader = leader,
        replicas = replicas.toList(),
        inSyncReplicas = inSyncReplicas.toList(),
        offlineReplicas = offlineReplicas.toList(),
    )

    /**
     * The topic name
     */
    @Deprecated(
        message = "use property instead.",
        replaceWith = ReplaceWith("topic"),
    )
    fun topic(): String = topic

    /**
     * The partition id
     */
    @Deprecated(
        message = "use property instead.",
        replaceWith = ReplaceWith("partition"),
    )
    fun partition(): Int = partition

    /**
     * The node id of the node currently acting as a leader for this partition or null if there is no leader
     */
    @Deprecated(
        message = "use property instead.",
        replaceWith = ReplaceWith("leader"),
    )
    fun leader(): Node? {
        return leader
    }

    /**
     * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
     */
    @Deprecated(
        message = "use property instead.",
        replaceWith = ReplaceWith("replicas"),
    )
    fun replicas(): Array<Node> {
        return replicas.toTypedArray()
    }

    /**
     * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if
     * the leader should fail
     */
    @Deprecated(
        message = "use property instead.",
        replaceWith = ReplaceWith("inSyncReplicas"),
    )
    fun inSyncReplicas(): Array<Node> {
        return inSyncReplicas.toTypedArray()
    }

    /**
     * The subset of the replicas that are offline
     */
    @Deprecated(
        message = "use property instead.",
        replaceWith = ReplaceWith("offlineReplicas"),
    )
    fun offlineReplicas(): Array<Node> {
        return offlineReplicas.toTypedArray()
    }

    override fun toString(): String {
        return "Partition(topic = $topic, " +
                "partition = $partition, " +
                "leader = ${leader?.idString() ?: "none"}, " +
                "replicas = ${formatNodeIds(replicas)}, " +
                "isr = ${formatNodeIds(inSyncReplicas)}, " +
                "offlineReplicas = ${formatNodeIds(offlineReplicas)})"
    }

    /* Extract the node ids from each item in the array and format for display */
    private fun formatNodeIds(nodes: List<Node>?): String {
        val b = StringBuilder("[")
        if (nodes != null) b.append(nodes.joinToString(",") { it.idString() })
        b.append("]")
        return b.toString()
    }
}
