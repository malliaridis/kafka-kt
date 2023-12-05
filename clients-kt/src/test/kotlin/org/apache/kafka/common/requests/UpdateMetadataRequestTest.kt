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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataTopicState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.test.TestUtils.generateRandomTopicPartitions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class UpdateMetadataRequestTest {
    
    @Test
    fun testUnsupportedVersion() {
        val builder = UpdateMetadataRequest.Builder(
            version = (ApiKeys.UPDATE_METADATA.latestVersion() + 1).toShort(),
            controllerId = 0,
            controllerEpoch = 0,
            brokerEpoch = 0,
            partitionStates = emptyList(),
            liveBrokers = emptyList(),
            topicIds = emptyMap()
        )
        assertFailsWith<UnsupportedVersionException> { builder.build() }
    }

    @Test
    fun testGetErrorResponse() {
        for (version in ApiKeys.UPDATE_METADATA.allVersions()) {
            val builder = UpdateMetadataRequest.Builder(
                version = version,
                controllerId = 0,
                controllerEpoch = 0,
                brokerEpoch = 0,
                partitionStates = emptyList(),
                liveBrokers = emptyList(),
                topicIds = emptyMap(),
            )
            val request = builder.build()
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = ClusterAuthorizationException("Not authorized"),
            )
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error())
        }
    }

    /**
     * Verifies the logic we have in UpdateMetadataRequest to present a unified interface across the various versions
     * works correctly. For example, `UpdateMetadataPartitionState.topicName` is not serialiazed/deserialized in
     * recent versions, but we set it manually so that we can always present the ungrouped partition states
     * independently of the version.
     */
    @Test
    fun testVersionLogic() {
        val topic0 = "topic0"
        val topic1 = "topic1"
        for (version in ApiKeys.UPDATE_METADATA.allVersions()) {
            val partitionStates = listOf(
                UpdateMetadataPartitionState()
                    .setTopicName(topic0)
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(0)
                    .setLeaderEpoch(10)
                    .setIsr(intArrayOf(0, 1))
                    .setZkVersion(10)
                    .setReplicas(intArrayOf(0, 1, 2))
                    .setOfflineReplicas(intArrayOf(2)),
                UpdateMetadataPartitionState()
                    .setTopicName(topic0)
                    .setPartitionIndex(1)
                    .setControllerEpoch(2)
                    .setLeader(1)
                    .setLeaderEpoch(11)
                    .setIsr(intArrayOf(1, 2, 3))
                    .setZkVersion(11)
                    .setReplicas(intArrayOf(1, 2, 3))
                    .setOfflineReplicas(intArrayOf()),
                UpdateMetadataPartitionState()
                    .setTopicName(topic1)
                    .setPartitionIndex(0)
                    .setControllerEpoch(2)
                    .setLeader(2)
                    .setLeaderEpoch(11)
                    .setIsr(intArrayOf(2, 3))
                    .setZkVersion(11)
                    .setReplicas(intArrayOf(2, 3, 4))
                    .setOfflineReplicas(intArrayOf())
            )
            val broker0Endpoints = mutableListOf<UpdateMetadataEndpoint>()
            broker0Endpoints.add(
                UpdateMetadataEndpoint()
                    .setHost("host0")
                    .setPort(9090)
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
            )

            // Non plaintext endpoints are only supported from version 1
            if (version >= 1) {
                broker0Endpoints.add(
                    UpdateMetadataEndpoint()
                        .setHost("host0")
                        .setPort(9091)
                        .setSecurityProtocol(SecurityProtocol.SSL.id)
                )
            }

            // Custom listeners are only supported from version 3
            if (version >= 3) {
                broker0Endpoints[0].setListener("listener0")
                broker0Endpoints[1].setListener("listener1")
            }
            val liveBrokers = listOf(
                UpdateMetadataBroker()
                    .setId(0)
                    .setRack("rack0")
                    .setEndpoints(broker0Endpoints),
                UpdateMetadataBroker()
                    .setId(1)
                    .setEndpoints(
                        listOf(
                            UpdateMetadataEndpoint()
                                .setHost("host1")
                                .setPort(9090)
                                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                                .setListener("PLAINTEXT")
                        )
                    )
            )
            val topicIds = mapOf(
                topic0 to Uuid.randomUuid(),
                topic1 to Uuid.randomUuid(),
            )
            val request = UpdateMetadataRequest.Builder(
                version = version,
                controllerId = 1,
                controllerEpoch = 2,
                brokerEpoch = 3,
                partitionStates = partitionStates,
                liveBrokers = liveBrokers,
                topicIds = topicIds,
            ).build()

            assertEquals(partitionStates.toSet(), request.partitionStates().toSet())
            assertEquals(liveBrokers, request.liveBrokers())
            assertEquals(1, request.controllerId())
            assertEquals(2, request.controllerEpoch())
            assertEquals(3, request.brokerEpoch())
            val byteBuffer = request.serialize()
            val deserializedRequest = UpdateMetadataRequest(
                data = UpdateMetadataRequestData(
                    readable = ByteBufferAccessor(byteBuffer),
                    version = version,
                ),
                version = version
            )

            // Unset fields that are not supported in this version as the deserialized request won't have them

            // Rack is only supported from version 2
            if (version < 2) {
                for (liveBroker in liveBrokers) liveBroker.setRack("")
            }

            // Non plaintext listener name is only supported from version 3
            if (version < 3) {
                for (liveBroker in liveBrokers) {
                    for (endpoint in liveBroker.endpoints) {
                        val securityProtocol = SecurityProtocol.forId(endpoint.securityProtocol)
                        endpoint.setListener(ListenerName.forSecurityProtocol(securityProtocol!!).value)
                    }
                }
            }

            // Offline replicas are only supported from version 4
            if (version < 4) partitionStates[0].setOfflineReplicas(intArrayOf())
            assertEquals(partitionStates.toSet(), deserializedRequest.partitionStates().toSet())
            assertEquals(liveBrokers, deserializedRequest.liveBrokers())
            assertEquals(1, deserializedRequest.controllerId())
            assertEquals(2, deserializedRequest.controllerEpoch())

            // Broker epoch is only supported from version 5
            if (version >= 5) assertEquals(3, deserializedRequest.brokerEpoch())
            else assertEquals(-1, deserializedRequest.brokerEpoch())

            val topicIdCount = deserializedRequest.data().topicStates
                .map(UpdateMetadataTopicState::topicId)
                .count { topicId -> Uuid.ZERO_UUID != topicId }

            if (version >= 7) assertEquals(2, topicIdCount)
            else assertEquals(0, topicIdCount)
        }
    }

    @Test
    fun testTopicPartitionGroupingSizeReduction() {
        val tps = generateRandomTopicPartitions(numTopic = 10, numPartitionPerTopic = 10)
        val partitionStates = tps.map { (topic, partition) ->
            UpdateMetadataPartitionState()
                .setTopicName(topic)
                .setPartitionIndex(partition)
        }
        val builder = UpdateMetadataRequest.Builder(
            version = 5.toShort(),
            controllerId = 0,
            controllerEpoch = 0,
            brokerEpoch = 0,
            partitionStates = partitionStates,
            liveBrokers = emptyList(),
            topicIds = emptyMap(),
        )
        assertTrue(builder.build(version = 5).sizeInBytes() < builder.build(version = 4).sizeInBytes())
    }
}
