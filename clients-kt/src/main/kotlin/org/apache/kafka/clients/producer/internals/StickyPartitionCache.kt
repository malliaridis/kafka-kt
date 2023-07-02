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

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ThreadLocalRandom

/**
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks
 * the current sticky partition for any given topic. This class should not be used externally.
 */
class StickyPartitionCache {

    private val indexCache: ConcurrentMap<String, Int> = ConcurrentHashMap()

    fun partition(topic: String, cluster: Cluster): Int {
        return indexCache[topic] ?: nextPartition(topic, cluster, -1)
    }

    fun nextPartition(topic: String, cluster: Cluster, prevPartition: Int): Int {
        val partitions = cluster.partitionsForTopic(topic)
        val oldPart = indexCache[topic]
        var newPart = oldPart

        // Check that the current sticky partition for the topic is either not set or that the
        // partition that triggered the new batch matches the sticky partition that needs to be
        // changed.
        if (oldPart == null || oldPart == prevPartition) {
            val availablePartitions = cluster.availablePartitionsForTopic(topic)
            if (availablePartitions.isEmpty()) {
                val random = Utils.toPositive(ThreadLocalRandom.current().nextInt())
                newPart = random % partitions.size
            } else if (availablePartitions.size == 1) {
                newPart = availablePartitions[0].partition
            } else {
                while (newPart == null || newPart == oldPart) {
                    val random = Utils.toPositive(ThreadLocalRandom.current().nextInt())
                    newPart = availablePartitions[random % availablePartitions.size].partition
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current
            // sticky partition.
            if (oldPart == null) indexCache.putIfAbsent(topic, newPart)
            else indexCache.replace(topic, prevPartition, newPart)

            return indexCache[topic]!!
        }
        return indexCache[topic]!!
    }
}
