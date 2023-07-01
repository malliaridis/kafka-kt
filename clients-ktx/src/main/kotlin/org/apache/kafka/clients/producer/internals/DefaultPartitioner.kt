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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
 * NOTE this partitioner is deprecated and shouldn't be used. To use default partitioning logic
 * remove partitioner.class configuration setting. See KIP-794 for more info.
 *
 * The default partitioning strategy:
 *
 * - If a partition is specified in the record, use it
 * - If no partition is specified but a key is present choose a partition based on a hash of the
 *   key
 * - If no partition or key is present choose the sticky partition that changes when the batch is
 *   full.
 *
 * See KIP-480 for details about sticky partitioning.
 */
@Deprecated("")
class DefaultPartitioner : Partitioner {

    private val stickyPartitionCache = StickyPartitionCache()

    override fun configure(configs: Map<String, *>) = Unit

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
    ): Int = partition(
        topic = topic,
        key = key,
        keyBytes = keyBytes,
        value = value,
        valueBytes = valueBytes,
        cluster = cluster,
        numPartitions = cluster.partitionsForTopic(topic).size,
    )

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param numPartitions The number of partitions of the given `topic`
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
        numPartitions: Int,
    ): Int = if (keyBytes == null) stickyPartitionCache.partition(topic, cluster)
        else BuiltInPartitioner.partitionForKey(keyBytes, numPartitions)

    override fun close() = Unit

    /**
     * If a batch completed for the current sticky partition, change the sticky partition.
     * Alternately, if no sticky partition has been determined, set one.
     */
    @Deprecated("")
    override fun onNewBatch(topic: String, cluster: Cluster, prevPartition: Int) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition)
    }
}
