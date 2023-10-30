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
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class BuiltInPartitionerTest {
    
    val logContext = LogContext()
    
    @AfterEach
    fun tearDown() {
        BuiltInPartitioner.mockRandom = null
    }

    @Test
    fun testStickyPartitioning() {
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
            )
        )
        val testCluster = Cluster(
            clusterId = "clusterId",
            nodes = NODES,
            partitions = allPartitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )

        // Create partitions with "sticky" batch size to accommodate 3 records.
        val builtInPartitionerA = BuiltInPartitioner(
            logContext = logContext,
            topic = TOPIC_A,
            stickyBatchSize = 3,
        )

        // Test the partition is not switched until sticky batch size is reached.
        // Mock random number generator with just sequential integer.
        val mockRandom = AtomicInteger()
        BuiltInPartitioner.mockRandom = Supplier { mockRandom.getAndAdd(1) }
        var partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster)!!
        val partA = partitionInfo.partition()
        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster)
        partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster)!!

        assertEquals(partA, partitionInfo.partition())

        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster)
        partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster)!!

        assertEquals(partA, partitionInfo.partition())

        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster)

        // After producing 3 records, partition must've switched.
        assertNotEquals(
            illegal = partA,
            actual = builtInPartitionerA.peekCurrentPartitionInfo(testCluster)!!.partition()
        )

        // Check that switching works even when there is one partition.
        val builtInPartitionerB = BuiltInPartitioner(logContext, TOPIC_B, 1)
        var c = 10
        while (c-- > 0) {
            partitionInfo = builtInPartitionerB.peekCurrentPartitionInfo(testCluster)!!

            assertEquals(0, partitionInfo.partition())

            builtInPartitionerB.updatePartitionInfo(partitionInfo, 1, testCluster)
        }
    }

    @Test
    fun unavailablePartitionsTest() {
        // Partition 1 in topic A, partition 0 in topic B and partition 0 in topic C are unavailable partitions.
        val allPartitions = listOf(
            PartitionInfo(
                topic = TOPIC_A,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES

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

        // Create partitions with "sticky" batch size to accommodate 1 record.
        val builtInPartitionerA = BuiltInPartitioner(
            logContext = logContext,
            topic = TOPIC_A,
            stickyBatchSize = 1
        )

        // Assure we never choose partition 1 because it is unavailable.
        var partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster)!!
        val partA = partitionInfo.partition()
        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster)
        var foundAnotherPartA = false

        assertNotEquals(1, partA)

        repeat(100) {
            partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster)!!
            val anotherPartA = partitionInfo.partition()
            builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster)

            assertNotEquals(1, anotherPartA)

            foundAnotherPartA = foundAnotherPartA || anotherPartA != partA
        }

        assertTrue(foundAnotherPartA, "Expected to find partition other than $partA")

        val builtInPartitionerB = BuiltInPartitioner(logContext, TOPIC_B, 1)
        // Assure we always choose partition 1 for topic B.
        partitionInfo = builtInPartitionerB.peekCurrentPartitionInfo(testCluster)!!
        val partB = partitionInfo.partition()
        builtInPartitionerB.updatePartitionInfo(partitionInfo, 1, testCluster)

        assertEquals(1, partB)

        repeat(100) {
            partitionInfo = builtInPartitionerB.peekCurrentPartitionInfo(testCluster)!!

            assertEquals(1, partitionInfo.partition())

            builtInPartitionerB.updatePartitionInfo(partitionInfo, 1, testCluster)
        }

        // Assure that we still choose the partition when there are no partitions available.
        val builtInPartitionerC = BuiltInPartitioner(logContext, TOPIC_C, 1)
        partitionInfo = builtInPartitionerC.peekCurrentPartitionInfo(testCluster)!!
        var partC = partitionInfo.partition()
        builtInPartitionerC.updatePartitionInfo(partitionInfo, 1, testCluster)

        assertEquals(0, partC)

        partitionInfo = builtInPartitionerC.peekCurrentPartitionInfo(testCluster)!!
        partC = partitionInfo.partition()

        assertEquals(0, partC)
    }

    @Test
    fun adaptivePartitionsTest() {
        // Mock random number generator with just sequential integer.
        val mockRandom = AtomicInteger()
        BuiltInPartitioner.mockRandom = Supplier { mockRandom.getAndAdd(1) }
        val builtInPartitioner = BuiltInPartitioner(
            logContext = logContext,
            topic = TOPIC_A,
            stickyBatchSize = 1
        )

        // Simulate partition queue sizes.
        val queueSizes = intArrayOf(5, 0, 3, 0, 1)
        val partitionIds = IntArray(queueSizes.size)
        val expectedFrequencies = IntArray(queueSizes.size)
        val allPartitions: MutableList<PartitionInfo> = ArrayList()
        for (i in partitionIds.indices) {
            partitionIds[i] = i
            allPartitions.add(
                PartitionInfo(
                    TOPIC_A, i,
                    NODES[i % NODES.size], NODES, NODES
                )
            )
            expectedFrequencies[i] = 6 - queueSizes[i] // 6 is max(queueSizes) + 1
        }
        builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, queueSizes.size)
        val testCluster = Cluster(
            clusterId = "clusterId",
            nodes = NODES,
            partitions = allPartitions,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )

        // Issue a certain number of partition calls to validate that the partitions would be
        // distributed with frequencies that are reciprocal to the queue sizes.  The number of
        // iterations is defined by the last element of the cumulative frequency table which is
        // the sum of all frequencies.  We do 2 cycles, just so it's more than 1.
        val numberOfCycles = 2
        val numberOfIterations = builtInPartitioner.loadStatsRangeEnd() * numberOfCycles
        val frequencies = IntArray(queueSizes.size)
        for (i in 0 until numberOfIterations) {
            val partitionInfo = builtInPartitioner.peekCurrentPartitionInfo(testCluster)!!
            ++frequencies[partitionInfo.partition()]
            builtInPartitioner.updatePartitionInfo(partitionInfo, 1, testCluster)
        }

        // Verify that frequencies are reciprocal of queue sizes.
        for (i in frequencies.indices) assertEquals(
            expected = expectedFrequencies[i] * numberOfCycles, actual = frequencies[i],
            message = "Partition $i was chosen ${frequencies[i]} times",
        )
    }

    companion object {

        private val NODES = listOf(
            Node(id = 0, host = "localhost", port = 99),
            Node(id = 1, host = "localhost", port = 100),
            Node(id = 2, host = "localhost", port = 101),
            Node(id = 11, host = "localhost", port = 102)
        )

        const val TOPIC_A = "topicA"

        const val TOPIC_B = "topicB"

        const val TOPIC_C = "topicC"
    }
}
