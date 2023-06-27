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

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils.toPositive
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The "Round-Robin" partitioner
 *
 * This partitioning strategy can be used when user wants to distribute the writes to all partitions
 * equally. This is the behaviour regardless of record key hash.
 */
class RoundRobinPartitioner : Partitioner {

    private val topicCounterMap: ConcurrentMap<String, AtomicInteger> = ConcurrentHashMap()

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
    ): Int {
        val partitions = cluster.partitionsForTopic(topic)
        val numPartitions = partitions.size
        val nextValue = nextValue(topic)
        val availablePartitions = cluster.availablePartitionsForTopic(topic)
        return if (availablePartitions.isNotEmpty()) {
            val part = toPositive(nextValue) % availablePartitions.size
            availablePartitions[part].partition
        } else {
            // no partitions are available, give a non-available partition
            toPositive(nextValue) % numPartitions
        }
    }

    private fun nextValue(topic: String): Int {
        val counter = topicCounterMap.computeIfAbsent(topic) { AtomicInteger(0)  }
        return counter.getAndIncrement()
    }

    override fun close() = Unit
}
