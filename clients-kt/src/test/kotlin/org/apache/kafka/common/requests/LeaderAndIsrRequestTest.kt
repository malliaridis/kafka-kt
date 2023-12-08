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

package org.apache.kafka.common.requests

import org.apache.kafka.common.Node
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.test.TestUtils.generateRandomTopicPartitions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LeaderAndIsrRequestTest {
    
    @Test
    fun testUnsupportedVersion() {
        val builder = LeaderAndIsrRequest.Builder(
            version = (ApiKeys.LEADER_AND_ISR.latestVersion() + 1).toShort(),
            controllerId = 0,
            controllerEpoch = 0,
            brokerEpoch = 0,
            partitionStates = emptyList(),
            topicIds = emptyMap(),
            liveLeaders = emptySet(),
        )
        assertFailsWith<UnsupportedVersionException> { builder.build() }
    }

    @Test
    fun testGetErrorResponse() {
        val topicId = Uuid.randomUuid()
        val topicName = "topic"
        val partition = 0
        for (version in ApiKeys.LEADER_AND_ISR.allVersions()) {
            val request = LeaderAndIsrRequest.Builder(
                version = version,
                controllerId = 0,
                controllerEpoch = 0,
                brokerEpoch = 0,
                partitionStates = listOf(
                    LeaderAndIsrPartitionState()
                        .setTopicName(topicName)
                        .setPartitionIndex(partition),
                ),
                topicIds = mapOf(topicName to topicId),
                liveLeaders = emptySet(),
            ).build(version)
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = ClusterAuthorizationException("Not authorized"),
            )
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error())
            if (version < 5) {
                assertEquals(
                    expected = listOf(
                        LeaderAndIsrPartitionError()
                            .setTopicName(topicName)
                            .setPartitionIndex(partition)
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
                    ),
                    actual = response.data().partitionErrors,
                )
                assertEquals(0, response.data().topics.size)
            } else {
                val topicState = response.topics().find(topicId)!!
                assertEquals(topicId, topicState.topicId)
                assertEquals(
                    expected = listOf(
                        LeaderAndIsrPartitionError()
                            .setPartitionIndex(partition)
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
                    ),
                    actual = topicState.partitionErrors,
                )
                assertEquals(0, response.data().partitionErrors.size)
            }
        }
    }

    /**
     * Verifies the logic we have in LeaderAndIsrRequest to present a unified interface across the various versions
     * works correctly. For example, `LeaderAndIsrPartitionState.topicName` is not serialiazed/deserialized in
     * recent versions, but we set it manually so that we can always present the ungrouped partition states
     * independently of the version.
     */
    @Test
    fun testVersionLogic() {
        for (version in ApiKeys.LEADER_AND_ISR.allVersions()) {
            val partitionStates = listOf(
                LeaderAndIsrPartitionState()
                    .setTopicName("topic0")
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(0)
                    .setLeaderEpoch(10)
                    .setIsr(intArrayOf(0, 1))
                    .setPartitionEpoch(10)
                    .setReplicas(intArrayOf(0, 1, 2))
                    .setAddingReplicas(intArrayOf(3))
                    .setRemovingReplicas(intArrayOf(2)),
                LeaderAndIsrPartitionState()
                    .setTopicName("topic0")
                    .setPartitionIndex(1)
                    .setControllerEpoch(2)
                    .setLeader(1)
                    .setLeaderEpoch(11)
                    .setIsr(intArrayOf(1, 2, 3))
                    .setPartitionEpoch(11)
                    .setReplicas(intArrayOf(1, 2, 3))
                    .setAddingReplicas(intArrayOf())
                    .setRemovingReplicas(intArrayOf()),
                LeaderAndIsrPartitionState()
                    .setTopicName("topic1")
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(2)
                    .setLeaderEpoch(11)
                    .setIsr(intArrayOf(2, 3, 4))
                    .setPartitionEpoch(11)
                    .setReplicas(intArrayOf(2, 3, 4))
                    .setAddingReplicas(intArrayOf())
                    .setRemovingReplicas(intArrayOf())
            )
            val liveNodes = listOf(
                Node(id = 0, host = "host0", port = 9090),
                Node(id = 1, host = "host1", port = 9091),
            )
            var topicIds = mutableMapOf(
                "topic0" to Uuid.randomUuid(),
                "topic1" to Uuid.randomUuid(),
            )
            val request = LeaderAndIsrRequest.Builder(
                version = version,
                controllerId = 1,
                controllerEpoch = 2,
                brokerEpoch = 3,
                partitionStates = partitionStates,
                topicIds = topicIds,
                liveLeaders = liveNodes,
            ).build()
            val liveLeaders = liveNodes.map { (id, host, port) ->
                LeaderAndIsrLiveLeader()
                    .setBrokerId(id)
                    .setHostName(host)
                    .setPort(port)
            }
            assertEquals(HashSet(partitionStates), request.partitionStates().toSet())
            assertEquals(liveLeaders, request.liveLeaders())
            assertEquals(1, request.controllerId())
            assertEquals(2, request.controllerEpoch())
            assertEquals(3, request.brokerEpoch())
            val byteBuffer = request.serialize()
            val deserializedRequest = LeaderAndIsrRequest(
                data = LeaderAndIsrRequestData(ByteBufferAccessor(byteBuffer), version),
                version = version,
            )

            // Adding/removing replicas is only supported from version 3, so the deserialized request won't have
            // them for earlier versions.
            if (version < 3) partitionStates[0]
                .setAddingReplicas(intArrayOf())
                .setRemovingReplicas(intArrayOf())

            // Prior to version 2, there were no TopicStates, so a map of Topic Ids from a list of
            // TopicStates is an empty map.
            if (version < 2) topicIds = HashMap()

            //  In versions 2-4 there are TopicStates, but no topicIds, so deserialized requests will have
            //  Zero Uuids in place.
            if (version in 2..4) {
                topicIds["topic0"] = Uuid.ZERO_UUID
                topicIds["topic1"] = Uuid.ZERO_UUID
            }
            assertEquals(HashSet(partitionStates), deserializedRequest.partitionStates().toSet())
            assertEquals(topicIds, deserializedRequest.topicIds())
            assertEquals(liveLeaders, deserializedRequest.liveLeaders())
            assertEquals(1, request.controllerId())
            assertEquals(2, request.controllerEpoch())
            assertEquals(3, request.brokerEpoch())
        }
    }

    @Test
    fun testTopicPartitionGroupingSizeReduction() {
        val tps = generateRandomTopicPartitions(numTopic = 10, numPartitionPerTopic = 10)
        val partitionStates = mutableListOf<LeaderAndIsrPartitionState>()
        val topicIds = mutableMapOf<String, Uuid>()
        for ((topic, partition) in tps) {
            partitionStates.add(
                LeaderAndIsrPartitionState()
                    .setTopicName(topic)
                    .setPartitionIndex(partition)
            )
            topicIds[topic] = Uuid.randomUuid()
        }
        val builder = LeaderAndIsrRequest.Builder(
            version = 2,
            controllerId = 0,
            controllerEpoch = 0,
            brokerEpoch = 0,
            partitionStates = partitionStates,
            topicIds = topicIds,
            liveLeaders = emptySet(),
        )
        val v2 = builder.build(2)
        val v1 = builder.build(1)
        assertTrue(
            v2.sizeInBytes() < v1.sizeInBytes(),
            "Expected v2 < v1: v2=${v2.sizeInBytes()}, v1=${v1.sizeInBytes()}",
        )
    }
}
