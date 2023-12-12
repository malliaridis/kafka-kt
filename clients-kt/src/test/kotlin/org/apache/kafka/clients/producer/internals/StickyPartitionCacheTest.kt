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
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class StickyPartitionCacheTest {
    
    @Test
    fun testStickyPartitionCache() {
        val allPartitions = listOf(
            PartitionInfo(
                topic = TOPIC_A,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_A,
                partition = 1,
                leader = NODES[1],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_A,
                partition = 2,
                leader = NODES[2],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_B,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
        )
        val testCluster = Cluster(
            clusterId = "clusterId",
            nodes = NODES,
            partitions = allPartitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val stickyPartitionCache = StickyPartitionCache()
        val partA = stickyPartitionCache.partition(TOPIC_A, testCluster)

        assertEquals(partA, stickyPartitionCache.partition(TOPIC_A, testCluster))

        val partB = stickyPartitionCache.partition(TOPIC_B, testCluster)

        assertEquals(partB, stickyPartitionCache.partition(TOPIC_B, testCluster))

        val changedPartA = stickyPartitionCache.nextPartition(TOPIC_A, testCluster, partA)

        assertEquals(changedPartA, stickyPartitionCache.partition(TOPIC_A, testCluster))
        assertNotEquals(partA, changedPartA)

        val changedPartA2 = stickyPartitionCache.partition(TOPIC_A, testCluster)

        assertEquals(changedPartA2, changedPartA)

        // We do not want to change partitions because the previous partition does not match the current sticky one.
        val changedPartA3 = stickyPartitionCache.nextPartition(TOPIC_A, testCluster, partA)
        assertEquals(changedPartA3, changedPartA2)

        // Check that we can still use the partitioner when there is only one partition
        val changedPartB = stickyPartitionCache.nextPartition(TOPIC_B, testCluster, partB)
        assertEquals(changedPartB, stickyPartitionCache.partition(TOPIC_B, testCluster))
    }

    @Test
    fun unavailablePartitionsTest() {
        // Partition 1 in topic A and partition 0 in topic B are unavailable partitions.
        val allPartitions = listOf(
            PartitionInfo(
                topic = TOPIC_A,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_A,
                partition = 1,
                leader = null,
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_A,
                partition = 2,
                leader = NODES[2],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_B,
                partition = 0,
                leader = null,
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_B,
                partition = 1,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_C,
                partition = 0,
                leader = null,
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
        )
        val testCluster = Cluster(
            clusterId = "clusterId",
            nodes = listOf(NODES[0],NODES[1], NODES[2]),
            partitions = allPartitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val stickyPartitionCache = StickyPartitionCache()

        // Assure we never choose partition 1 because it is unavailable.
        var partA = stickyPartitionCache.partition(TOPIC_A, testCluster)

        assertNotEquals(1, partA)

        repeat(100) {
            partA = stickyPartitionCache.nextPartition(TOPIC_A, testCluster, partA)

            assertNotEquals(1, stickyPartitionCache.partition(TOPIC_A, testCluster))
        }

        // Assure we always choose partition 1 for topic B.
        var partB = stickyPartitionCache.partition(TOPIC_B, testCluster)

        assertEquals(1, partB)

        repeat(100) {
            partB = stickyPartitionCache.nextPartition(TOPIC_B, testCluster, partB)

            assertEquals(1, stickyPartitionCache.partition(TOPIC_B, testCluster))
        }

        // Assure that we still choose the partition when there are no partitions available.
        var partC = stickyPartitionCache.partition(TOPIC_C, testCluster)

        assertEquals(0, partC)

        partC = stickyPartitionCache.nextPartition(TOPIC_C, testCluster, partC)

        assertEquals(0, partC)
    }

    companion object {
        
        private val NODES = listOf(
            Node(id = 0, host = "localhost", port = 99),
            Node(id = 1, host = "localhost", port = 100),
            Node(id = 2, host = "localhost", port = 101),
            Node(id = 11, host = "localhost", port = 102),
        )
        
        const val TOPIC_A = "topicA"
        
        const val TOPIC_B = "topicB"
        
        const val TOPIC_C = "topicC"
    }
}
