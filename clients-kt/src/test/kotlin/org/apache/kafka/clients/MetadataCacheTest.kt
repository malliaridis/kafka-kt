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

package org.apache.kafka.clients

import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MetadataCacheTest {

    @Test
    fun testMissingLeaderEndpoint() {
        // Although the broker attempts to ensure leader information is available, the
        // client metadata cache may retain partition metadata across multiple responses.
        // For example, separate responses may contain conflicting leader epochs for
        // separate partitions and the client will always retain the highest.
        val topicPartition = TopicPartition(topic = "topic", partition = 0)
        val partitionMetadata = PartitionMetadata(
            error = Errors.NONE,
            topicPartition = topicPartition,
            leaderId = 5,
            leaderEpoch = 10,
            replicaIds = mutableListOf(5, 6, 7),
            inSyncReplicaIds = mutableListOf(5, 6, 7),
            offlineReplicaIds = emptyList(),
        )
        val nodesById: MutableMap<Int, Node> = HashMap()
        nodesById[6] = Node(id = 6, host = "localhost", port = 2077)
        nodesById[7] = Node(id = 7, host = "localhost", port = 2078)
        nodesById[8] = Node(id = 8, host = "localhost", port = 2079)
        val cache = MetadataCache(
            clusterId = "clusterId",
            nodes = nodesById,
            partitions = setOf(partitionMetadata),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet(),
            internalTopics = emptySet(),
            controller = null,
            topicIds = emptyMap(),
        )
        val cluster = cache.cluster()
        assertNull(cluster.leaderFor(topicPartition))
        val partitionInfo = cluster.partition(topicPartition)!!
        val replicas = partitionInfo.replicas.associateBy { it.id }
        assertNull(partitionInfo.leader)
        assertEquals(expected = 3, actual = replicas.size)
        assertTrue(replicas[5]!!.isEmpty)
        assertEquals(expected = nodesById[6], actual = replicas[6])
        assertEquals(expected = nodesById[7], actual = replicas[7])
    }
}
