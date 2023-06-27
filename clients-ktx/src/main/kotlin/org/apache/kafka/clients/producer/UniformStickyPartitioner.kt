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

package org.apache.kafka.clients.producer

import org.apache.kafka.clients.producer.internals.StickyPartitionCache
import org.apache.kafka.common.Cluster

/**
 * NOTE this partitioner is deprecated and shouldn't be used.  To use default partitioning logic
 * remove partitioner.class configuration setting and set partitioner.ignore.keys=true.
 * See KIP-794 for more info.
 *
 * The partitioning strategy:
 *
 * - If a partition is specified in the record, use it
 * - Otherwise choose the sticky partition that changes when the batch is full.
 *
 * NOTE: In contrast to the DefaultPartitioner, the record key is NOT used as part of the
 * partitioning strategy in this partitioner. Records with the same key are not guaranteed to be
 * sent to the same partition.
 *
 * See KIP-480 for details about sticky partitioning.
 */
@Deprecated("")
class UniformStickyPartitioner : Partitioner {

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
        cluster: Cluster
    ): Int {
        return stickyPartitionCache.partition(topic, cluster)
    }

    override fun close() {}

    /**
     * If a batch completed for the current sticky partition, change the sticky partition.
     * Alternately, if no sticky partition has been determined, set one.
     */
    @Suppress("deprecation")
    override fun onNewBatch(topic: String?, cluster: Cluster?, prevPartition: Int) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition)
    }
}
