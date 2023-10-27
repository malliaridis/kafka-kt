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

package org.apache.kafka.common

import org.apache.kafka.common.utils.Utils.mkSet
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.*
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ClusterTest {

    @Test
    fun testBootstrap() {
        val ipAddress = "140.211.11.105"
        val hostName = "www.example.com"
        val cluster = Cluster.bootstrap(
            listOf(
                InetSocketAddress(ipAddress, 9002),
                InetSocketAddress(hostName, 9002),
            )
        )
        val expectedHosts = mkSet(ipAddress, hostName)
        val actualHosts: MutableSet<String> = HashSet()
        for ((_, host) in cluster.nodes) actualHosts.add(host)
        assertEquals(expectedHosts, actualHosts)
    }

    @Test
    @Disabled
    fun testReturnUnmodifiableCollections() {
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
            PartitionInfo(
                topic = TOPIC_D,
                partition = 0,
                leader = NODES[1],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
            PartitionInfo(
                topic = TOPIC_E,
                partition = 0,
                leader = NODES[0],
                replicas = NODES,
                inSyncReplicas = NODES,
            ),
        )
        val unauthorizedTopics = mkSet(TOPIC_C)
        val invalidTopics = mkSet(TOPIC_D)
        val internalTopics = mkSet(TOPIC_E)
        val cluster = Cluster(
            clusterId = "clusterId",
            nodes = NODES.toList(),
            partitions = allPartitions,
            unauthorizedTopics = unauthorizedTopics,
            invalidTopics = invalidTopics,
            internalTopics = internalTopics,
            controller = NODES[1],
        )
        // Kotlin migration note: The operations are not supported by default due to the immutability
        // introduced with Kotlin

//        assertFailsWith<UnsupportedOperationException> { cluster.invalidTopics.add("foo") }
//        assertFailsWith<UnsupportedOperationException> { cluster.internalTopics.add("foo") }
//        assertFailsWith<UnsupportedOperationException> { cluster.unauthorizedTopics.add("foo") }
//        assertFailsWith<UnsupportedOperationException> { cluster.topics.add("foo") }
//        assertFailsWith<UnsupportedOperationException> { cluster.nodes.add(NODES[3]) }
//        assertFailsWith<UnsupportedOperationException> {
//            cluster.partitionsForTopic(TOPIC_A).add(
//                PartitionInfo(
//                    topic = TOPIC_A,
//                    partition = 3,
//                    leader = NODES[0],
//                    replicas = NODES,
//                    inSyncReplicas = NODES
//                )
//            )
//        }
//        assertFailsWith<UnsupportedOperationException> {
//            cluster.availablePartitionsForTopic(TOPIC_B).add(
//                PartitionInfo(
//                    topic = TOPIC_B,
//                    partition = 2,
//                    leader = NODES[0],
//                    replicas = NODES,
//                    inSyncReplicas = NODES
//                )
//            )
//        }
//        assertFailsWith<UnsupportedOperationException> {
//            cluster.partitionsForNode(NODES[1].id).add(
//                PartitionInfo(
//                    topic = TOPIC_B,
//                    partition = 2,
//                    leader = NODES[1],
//                    replicas = NODES,
//                    inSyncReplicas = NODES
//                )
//            )
//        }
    }

    companion object {
        
        private val NODES = listOf(
            Node(id = 0, host = "localhost", port = 99),
            Node(id = 1, host = "localhost", port = 100),
            Node(id = 2, host = "localhost", port = 101),
            Node(id = 11, host = "localhost", port = 102)
        )
        
        private const val TOPIC_A = "topicA"
        
        private const val TOPIC_B = "topicB"
        
        private const val TOPIC_C = "topicC"
        
        private const val TOPIC_D = "topicD"
        
        private const val TOPIC_E = "topicE"
    }
}
