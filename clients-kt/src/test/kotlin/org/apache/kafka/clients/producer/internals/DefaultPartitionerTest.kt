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

class DefaultPartitionerTest {

    @Test
    fun testKeyPartitionIsStable() {
        @Suppress("Deprecation")
        val partitioner = DefaultPartitioner()

        val cluster = Cluster(
            clusterId = "clusterId",
            nodes = NODES,
            partitions = PARTITIONS,
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
        )
        val partition = partitioner.partition(
            topic = TOPIC,
            key = null,
            keyBytes = KEY_BYTES,
            value = null,
            valueBytes = null,
            cluster = cluster,
        )
        assertEquals(
            expected = partition,
            actual = partitioner.partition(
                topic = TOPIC,
                key = null,
                keyBytes = KEY_BYTES,
                value = null,
                valueBytes = null,
                cluster = cluster,
            ),
            message = "Same key should yield same partition",
        )
    }

    companion object {

        private val KEY_BYTES = "key".toByteArray()

        private val NODES = listOf(
            Node(id = 0, host = "localhost", port = 99),
            Node(id = 1, host = "localhost", port = 100),
            Node(id = 12, host = "localhost", port = 101),
        )

        private const val TOPIC = "test"

        // Intentionally make the partition list not in partition order to test the edge cases.
        private val PARTITIONS = listOf(
            PartitionInfo(
                topic = TOPIC,
                partition = 1,
                leader = null,
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC,
                partition = 2,
                leader = NODES[1],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
        )
    }
}
