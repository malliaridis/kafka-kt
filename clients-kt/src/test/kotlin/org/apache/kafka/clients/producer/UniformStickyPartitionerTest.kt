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
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UniformStickyPartitionerTest {

    @Suppress("Deprecation")
    @Test
    fun testRoundRobinWithUnavailablePartitions() {
        // Intentionally make the partition list not in partition order to test the edge
        // cases.
        val partitions = listOf(
            PartitionInfo(
                topic = "test",
                partition = 1,
                leader = null,
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = "test",
                partition = 2,
                leader = NODES[1],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = "test",
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
        )
        // When there are some unavailable partitions, we want to make sure that (1) we
        // always pick an available partition,
        // and (2) the available partitions are selected in a sticky way.
        var countForPart0 = 0
        var countForPart2 = 0
        var part = 0
        val partitioner = UniformStickyPartitioner()
        val cluster = Cluster(
            clusterId = "clusterId",
            nodes = listOf(NODES[0], NODES[1], NODES[2]),
            partitions = partitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        for (i in 0..49) {
            part = partitioner.partition(
                topic = "test",
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = cluster,
            )

            assertTrue(part == 0 || part == 2, "We should never choose a leader-less node in round robin")

            if (part == 0) countForPart0++ else countForPart2++
        }
        // Simulates switching the sticky partition on a new batch.
        partitioner.onNewBatch("test", cluster, part)

        for (i in 1..50) {
            part = partitioner.partition(
                topic = "test",
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = cluster,
            )

            assertTrue(part == 0 || part == 2, "We should never choose a leader-less node in round robin")

            if (part == 0) countForPart0++ else countForPart2++
        }

        assertEquals(
            expected = countForPart0,
            actual = countForPart2,
            message = "The distribution between two available partitions should be even",
        )
    }

    @Suppress("Deprecation")
    @Test
    @Throws(InterruptedException::class)
    fun testRoundRobinWithKeyBytes() {
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
            ), PartitionInfo(
                topic = TOPIC_A,
                partition = 2,
                leader = NODES[1],
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
            nodes = listOf(NODES[0], NODES[1], NODES[2]),
            partitions = allPartitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val partitionCount = mutableMapOf<Int, Int>()
        val keyBytes = "key".toByteArray()
        var partition = 0
        val partitioner = UniformStickyPartitioner()

        for (i in 0..29) {
            partition = partitioner.partition(
                topic = TOPIC_A,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
            var count = partitionCount[partition]
            if (null == count) count = 0
            partitionCount[partition] = count + 1

            if (i % 5 == 0) partitioner.partition(
                topic = TOPIC_B,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }
        // Simulate a batch filling up and switching the sticky partition.
        partitioner.onNewBatch(topic = TOPIC_A, cluster = testCluster, prevPartition = partition)
        partitioner.onNewBatch(topic = TOPIC_B, cluster = testCluster, prevPartition = 0)

        // Save old partition to ensure that the wrong partition does not trigger a new batch.
        val oldPart = partition
        for (i in 0..29) {
            partition = partitioner.partition(
                topic = TOPIC_A,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
            var count = partitionCount[partition]
            if (null == count) count = 0
            partitionCount[partition] = count + 1

            if (i % 5 == 0) partitioner.partition(
                topic = TOPIC_B,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }
        val newPart = partition

        // Attempt to switch the partition with the wrong previous partition. Sticky partition should not change.
        partitioner.onNewBatch(TOPIC_A, testCluster, oldPart)
        for (i in 0..29) {
            partition = partitioner.partition(
                topic = TOPIC_A,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
            var count = partitionCount[partition]
            if (null == count) count = 0
            partitionCount[partition] = count + 1

            if (i % 5 == 0) partitioner.partition(
                topic = TOPIC_B,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }

        assertEquals(30, partitionCount[oldPart])
        assertEquals(60, partitionCount[newPart])
    }

    @Suppress("Deprecation")
    @Test
    @Throws(InterruptedException::class)
    fun testRoundRobinWithNullKeyBytes() {
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
            ), PartitionInfo(
                topic = TOPIC_A,
                partition = 2,
                leader = NODES[1],
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
            nodes = listOf(NODES[0],NODES[1], NODES[2]),
            partitions = allPartitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val partitionCount = mutableMapOf<Int, Int>()
        var partition = 0
        val partitioner = UniformStickyPartitioner()

        for (i in 0..29) {
            partition = partitioner.partition(
                topic = TOPIC_A,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
            var count = partitionCount[partition]
            if (null == count) count = 0
            partitionCount[partition] = count + 1
            if (i % 5 == 0) partitioner.partition(
                topic = TOPIC_B,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }
        // Simulate a batch filling up and switching the sticky partition.
        partitioner.onNewBatch(topic = TOPIC_A, cluster = testCluster, prevPartition = partition)
        partitioner.onNewBatch(topic = TOPIC_B, cluster = testCluster, prevPartition = 0)

        // Save old partition to ensure that the wrong partition does not trigger a new batch.
        val oldPart = partition
        for (i in 0..29) {
            partition = partitioner.partition(
                topic = TOPIC_A,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
            var count = partitionCount[partition]
            if (null == count) count = 0
            partitionCount[partition] = count + 1
            if (i % 5 == 0) partitioner.partition(
                topic = TOPIC_B,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }
        val newPart = partition

        // Attempt to switch the partition with the wrong previous partition. Sticky partition should not change.
        partitioner.onNewBatch(topic = TOPIC_A, cluster = testCluster, prevPartition = oldPart)
        for (i in 0..29) {
            partition = partitioner.partition(
                topic = TOPIC_A,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
            var count = partitionCount[partition]
            if (null == count) count = 0
            partitionCount[partition] = count + 1

            if (i % 5 == 0) partitioner.partition(
                topic = TOPIC_B,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }
        
        assertEquals(30, partitionCount[oldPart])
        assertEquals(60, partitionCount[newPart])
    }

    companion object {
        
        private val NODES = listOf(
            Node(id = 0, host = "localhost", port = 99),
            Node(id = 1, host = "localhost", port = 100),
            Node(id = 2, host = "localhost", port = 101)
        )
        
        private const val TOPIC_A = "TOPIC_A"
        
        private const val TOPIC_B = "TOPIC_B"
    }
}
