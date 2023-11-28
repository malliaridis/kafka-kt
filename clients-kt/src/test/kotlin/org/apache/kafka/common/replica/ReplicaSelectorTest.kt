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

package org.apache.kafka.common.replica

import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.test.TestUtils.assertNullable
import org.junit.jupiter.api.Test
import java.net.InetAddress
import kotlin.test.assertEquals

class ReplicaSelectorTest {

    @Test
    fun testSameRackSelector() {
        val tp = TopicPartition("test", 0)
        val replicaViewSet = replicaInfoSet()
        val leader = replicaViewSet[0]
        val partitionView = partitionInfo(replicaViewSet.toSet(), leader)
        val selector: ReplicaSelector = RackAwareReplicaSelector()
        var selected = selector.select(tp, metadata("rack-b"), partitionView)
        assertNullable(selected) { replicaInfo ->
            assertEquals("rack-b", replicaInfo.endpoint().rack, "Expect replica to be in rack-b")
            assertEquals(3, replicaInfo.endpoint().id, "Expected replica 3 since it is more caught-up")
        }
        selected = selector.select(tp, metadata("not-a-rack"), partitionView)
        assertNullable(selected) { replicaInfo ->
            assertEquals(leader, replicaInfo, "Expect leader when we can't find any nodes in given rack")
        }
        selected = selector.select(tp, metadata("rack-a"), partitionView)
        assertNullable(selected) { replicaInfo ->
            assertEquals("rack-a", replicaInfo.endpoint().rack, "Expect replica to be in rack-a")
            assertEquals(leader, replicaInfo, "Expect the leader since it's in rack-a")
        }
    }

    companion object {

        fun replicaInfoSet(): List<ReplicaView> {
            return listOf(
                replicaInfo(
                    node = Node(id = 0, host = "host0", port = 1234, rack = "rack-a"),
                    logOffset = 4,
                    timeSinceLastCaughtUpMs = 0,
                ),
                replicaInfo(
                    node = Node(id = 1, host = "host1", port = 1234, rack = "rack-a"),
                    logOffset = 2,
                    timeSinceLastCaughtUpMs = 5,
                ),
                replicaInfo(
                    node = Node(id = 2, host = "host2", port = 1234, rack = "rack-b"),
                    logOffset = 3,
                    timeSinceLastCaughtUpMs = 3,
                ),
                replicaInfo(
                    node = Node(id = 3, host = "host3", port = 1234, rack = "rack-b"),
                    logOffset = 4,
                    timeSinceLastCaughtUpMs = 2,
                ),
            )
        }

        fun replicaInfo(node: Node, logOffset: Long, timeSinceLastCaughtUpMs: Long): ReplicaView {
            return DefaultReplicaView(node, logOffset, timeSinceLastCaughtUpMs)
        }

        fun partitionInfo(replicaViewSet: Set<ReplicaView>, leader: ReplicaView): PartitionView {
            return DefaultPartitionView(replicaViewSet, leader)
        }

        fun metadata(rack: String): ClientMetadata {
            return DefaultClientMetadata(
                rackId = rack,
                clientId = "test-client",
                clientAddress = InetAddress.getLoopbackAddress(),
                principal = KafkaPrincipal.ANONYMOUS,
                listenerName = "TEST",
            )
        }
    }
}
