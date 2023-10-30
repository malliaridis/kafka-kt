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

class RoundRobinPartitionerTest {

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
        // and (2) the available partitions are selected in a round-robin way.
        var countForPart0 = 0
        var countForPart2 = 0
        val partitioner = RoundRobinPartitioner()
        val cluster = Cluster(
            clusterId = "clusterId",
            nodes = listOf(NODES[0], NODES[1], NODES[2]),
            partitions = partitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        for (i in 1..100) {
            val part = partitioner.partition(
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
            message = "The distribution between two available partitions should be even"
        )
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRoundRobinWithKeyBytes() {
        val topicA = "topicA"
        val topicB = "topicB"
        val allPartitions = listOf(
            PartitionInfo(
                topic = topicA,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = topicA,
                partition = 1,
                leader = NODES[1],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = topicA,
                partition = 2,
                leader = NODES[2],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = topicB,
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
        val partitioner = RoundRobinPartitioner()

        for (i in 0..29) {
            val partition = partitioner.partition(
                topic = topicA,
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
                topic = topicB,
                key = null,
                keyBytes = keyBytes,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }

        assertEquals(10, partitionCount[0])
        assertEquals(10, partitionCount[1])
        assertEquals(10, partitionCount[2])
    }

    @Test
    @Throws(InterruptedException::class)
    fun testRoundRobinWithNullKeyBytes() {
        val topicA = "topicA"
        val topicB = "topicB"
        val allPartitions = listOf(
            PartitionInfo(
                topic = topicA,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = topicA,
                partition = 1,
                leader = NODES[1],
                replicas = NODES,
                inSyncReplicas = NODES,
            ), PartitionInfo(
                topic = topicA,
                partition = 2,
                leader = NODES[2],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = topicB,
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
        val partitioner = RoundRobinPartitioner()

        for (i in 0..29) {
            val partition = partitioner.partition(
                topic = topicA,
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
                topic = topicB,
                key = null,
                keyBytes = null,
                value = null,
                valueBytes = null,
                cluster = testCluster,
            )
        }

        assertEquals(10, partitionCount[0])
        assertEquals(10, partitionCount[1])
        assertEquals(10, partitionCount[2])
    }

    companion object {

        private val NODES = listOf(
            Node(id = 0, host = "localhost", port = 99),
            Node(id = 1, host = "localhost", port = 100),
            Node(id = 2, host = "localhost", port = 101)
        )
    }
}
