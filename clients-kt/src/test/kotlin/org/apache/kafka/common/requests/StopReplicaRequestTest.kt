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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StopReplicaRequestTest {
    
    @Test
    fun testUnsupportedVersion() {
        val builder = StopReplicaRequest.Builder(
            version = (ApiKeys.STOP_REPLICA.latestVersion() + 1).toShort(),
            controllerId = 0,
            controllerEpoch = 0,
            brokerEpoch = 0L,
            deletePartitions = false,
            topicStates = emptyList(),
        )
        assertFailsWith<UnsupportedVersionException> { builder.build() }
    }

    @Test
    fun testGetErrorResponse() {
        val topicStates = topicStates(true)
        val expectedPartitions = mutableSetOf<StopReplicaPartitionError>()
        for (topicState in topicStates) {
            for (partitionState in topicState.partitionStates) {
                expectedPartitions.add(
                    StopReplicaPartitionError()
                        .setTopicName(topicState.topicName)
                        .setPartitionIndex(partitionState.partitionIndex)
                        .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
                )
            }
        }
        for (version in ApiKeys.STOP_REPLICA.allVersions()) {
            val builder = StopReplicaRequest.Builder(
                version = version,
                controllerId = 0,
                controllerEpoch = 0,
                brokerEpoch = 0L,
                deletePartitions = false,
                topicStates = topicStates,
            )
            val request = builder.build()
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = ClusterAuthorizationException("Not authorized"),
            )
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error())
            assertEquals(expectedPartitions, HashSet(response.partitionErrors()))
        }
    }

    @Test
    fun testBuilderNormalizationWithAllDeletePartitionEqualToTrue() {
        testBuilderNormalization(deletePartitions = true)
    }

    @Test
    fun testBuilderNormalizationWithAllDeletePartitionEqualToFalse() {
        testBuilderNormalization(deletePartitions = false)
    }

    private fun testBuilderNormalization(deletePartitions: Boolean) {
        val topicStates = topicStates(deletePartitions)
        val expectedPartitionStates = partitionStates(topicStates)
        for (version in ApiKeys.STOP_REPLICA.allVersions()) {
            val request = StopReplicaRequest.Builder(
                version = version,
                controllerId = 0,
                controllerEpoch = 1,
                brokerEpoch = 0,
                deletePartitions = deletePartitions,
                topicStates = topicStates,
            ).build(version)
            val data = request.data()
            if (version < 1) {
                val partitions = data.ungroupedPartitions.map { partition ->
                    TopicPartition(partition.topicName, partition.partitionIndex)
                }.toSet()
                assertEquals(expectedPartitionStates.keys, partitions)
                assertEquals(deletePartitions, data.deletePartitions)
            } else if (version < 3) {
                val partitions = mutableSetOf<TopicPartition>()
                for (topic in data.topics) {
                    for (partition in topic.partitionIndexes) {
                        partitions.add(TopicPartition(topic.name, partition))
                    }
                }
                assertEquals(expectedPartitionStates.keys, partitions)
                assertEquals(deletePartitions, data.deletePartitions)
            } else {
                val partitionStates = partitionStates(data.topicStates)
                assertEquals(expectedPartitionStates, partitionStates)
                // Always false from V3 on
                assertFalse(data.deletePartitions)
            }
        }
    }

    @Test
    fun testTopicStatesNormalization() {
        val topicStates = topicStates(true)
        for (version in ApiKeys.STOP_REPLICA.allVersions()) {
            // Create a request for version to get its serialized form
            val baseRequest = StopReplicaRequest.Builder(
                version = version,
                controllerId = 0,
                controllerEpoch = 1,
                brokerEpoch = 0,
                deletePartitions = true,
                topicStates = topicStates,
            ).build(version)

            // Construct the request from the buffer
            val request = StopReplicaRequest.parse(baseRequest.serialize(), version)
            val partitionStates = partitionStates(request.topicStates())
            assertEquals(6, partitionStates.size)
            for (expectedTopicState in topicStates) {
                for (expectedPartitionState in expectedTopicState.partitionStates) {
                    val tp = TopicPartition(
                        topic = expectedTopicState.topicName,
                        partition = expectedPartitionState.partitionIndex,
                    )
                    val partitionState = partitionStates[tp]
                    assertEquals(expectedPartitionState.partitionIndex, partitionState!!.partitionIndex)
                    assertTrue(partitionState.deletePartition)

                    if (version >= 3) assertEquals(expectedPartitionState.leaderEpoch, partitionState.leaderEpoch)
                    else assertEquals(-1, partitionState.leaderEpoch)
                }
            }
        }
    }

    @Test
    fun testPartitionStatesNormalization() {
        val topicStates = topicStates(true)
        for (version in ApiKeys.STOP_REPLICA.allVersions()) {
            // Create a request for version to get its serialized form
            val baseRequest = StopReplicaRequest.Builder(
                version = version,
                controllerId = 0,
                controllerEpoch = 1,
                brokerEpoch = 0,
                deletePartitions = true,
                topicStates = topicStates,
            ).build(version)

            // Construct the request from the buffer
            val request = StopReplicaRequest.parse(baseRequest.serialize(), version)
            val partitionStates = request.partitionStates()
            assertEquals(6, partitionStates.size)
            for (expectedTopicState in topicStates) {
                for (expectedPartitionState in expectedTopicState.partitionStates) {
                    val tp = TopicPartition(
                        topic = expectedTopicState.topicName,
                        partition = expectedPartitionState.partitionIndex,
                    )
                    val partitionState = partitionStates[tp]
                    assertEquals(expectedPartitionState.partitionIndex, partitionState!!.partitionIndex)
                    assertTrue(partitionState.deletePartition)

                    if (version >= 3) assertEquals(expectedPartitionState.leaderEpoch, partitionState.leaderEpoch)
                    else assertEquals(-1, partitionState.leaderEpoch)
                }
            }
        }
    }

    private fun topicStates(deletePartition: Boolean): List<StopReplicaTopicState> {
        val topicStates: MutableList<StopReplicaTopicState> = ArrayList()
        val topic0 = StopReplicaTopicState()
            .setTopicName("topic0")
            .setPartitionStates(
                listOf(
                    StopReplicaPartitionState()
                        .setPartitionIndex(0)
                        .setLeaderEpoch(0)
                        .setDeletePartition(deletePartition),
                    StopReplicaPartitionState()
                        .setPartitionIndex(1)
                        .setLeaderEpoch(1)
                        .setDeletePartition(deletePartition),
                )
            )
        topicStates.add(topic0)
        val topic1 = StopReplicaTopicState()
            .setTopicName("topic1")
            .setPartitionStates(
                listOf(
                    StopReplicaPartitionState()
                        .setPartitionIndex(2)
                        .setLeaderEpoch(2)
                        .setDeletePartition(deletePartition),
                    StopReplicaPartitionState()
                        .setPartitionIndex(3)
                        .setLeaderEpoch(3)
                        .setDeletePartition(deletePartition)
                )
            )
        topicStates.add(topic1)
        val topic3 = StopReplicaTopicState()
            .setTopicName("topic1")
            .setPartitionStates(
                listOf(
                    StopReplicaPartitionState()
                        .setPartitionIndex(4)
                        .setLeaderEpoch(-2)
                        .setDeletePartition(deletePartition),
                    StopReplicaPartitionState()
                        .setPartitionIndex(5)
                        .setLeaderEpoch(-2)
                        .setDeletePartition(deletePartition),
                )
            )
        topicStates.add(topic3)
        return topicStates
    }

    companion object {

        fun partitionStates(
            topicStates: Iterable<StopReplicaTopicState>,
        ): Map<TopicPartition, StopReplicaPartitionState> {
            val partitionStates = mutableMapOf<TopicPartition, StopReplicaPartitionState>()
            for (topicState in topicStates) {
                for (partitionState in topicState.partitionStates) {
                    val tp = TopicPartition(topicState.topicName, partitionState.partitionIndex)
                    partitionStates[tp] = partitionState
                }
            }
            return partitionStates
        }
    }
}
