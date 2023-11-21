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
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicErrorCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

class LeaderAndIsrResponseTest {

    @Test
    fun testErrorCountsFromGetErrorResponse() {
        val partitionStates: MutableList<LeaderAndIsrPartitionState> = ArrayList()
        partitionStates.add(
            LeaderAndIsrPartitionState()
                .setTopicName("foo")
                .setPartitionIndex(0)
                .setControllerEpoch(15)
                .setLeader(1)
                .setLeaderEpoch(10)
                .setIsr(intArrayOf(10))
                .setPartitionEpoch(20)
                .setReplicas(intArrayOf(10))
                .setIsNew(false)
        )
        partitionStates.add(
            LeaderAndIsrPartitionState()
                .setTopicName("foo")
                .setPartitionIndex(1)
                .setControllerEpoch(15)
                .setLeader(1)
                .setLeaderEpoch(10)
                .setIsr(intArrayOf(10))
                .setPartitionEpoch(20)
                .setReplicas(intArrayOf(10))
                .setIsNew(false)
        )
        val topicIds = mapOf("foo" to Uuid.randomUuid())
        val request = LeaderAndIsrRequest.Builder(
            version = ApiKeys.LEADER_AND_ISR.latestVersion(),
            controllerId = 15,
            controllerEpoch = 20,
            brokerEpoch = 0,
            partitionStates = partitionStates,
            topicIds = topicIds,
            liveLeaders = emptySet(),
        ).build()
        val response = request.getErrorResponse(
            throttleTimeMs = 0,
            e = Errors.CLUSTER_AUTHORIZATION_FAILED.exception!!,
        )
        assertEquals(
            expected = mapOf(Errors.CLUSTER_AUTHORIZATION_FAILED to 3),
            actual = response.errorCounts(),
        )
    }

    @Test
    fun testErrorCountsWithTopLevelError() {
        for (version in ApiKeys.LEADER_AND_ISR.allVersions()) {
            val response: LeaderAndIsrResponse = if (version < 5) {
                val partitions = createPartitions(
                    topicName = "foo",
                    errors = listOf(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER),
                )
                LeaderAndIsrResponse(
                    data = LeaderAndIsrResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
                        .setPartitionErrors(partitions),
                    version = version,
                )
            } else {
                val id = Uuid.randomUuid()
                val topics = createTopic(
                    id = id,
                    errors = listOf(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER),
                )
                LeaderAndIsrResponse(
                    data = LeaderAndIsrResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
                        .setTopics(topics),
                    version = version,
                )
            }
            assertEquals(mapOf(Errors.UNKNOWN_SERVER_ERROR to 3), response.errorCounts())
        }
    }

    @Test
    fun testErrorCountsNoTopLevelError() {
        for (version in ApiKeys.LEADER_AND_ISR.allVersions()) {
            val response: LeaderAndIsrResponse = if (version < 5) {
                val partitions = createPartitions(
                    topicName = "foo",
                    errors = listOf(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED),
                )
                LeaderAndIsrResponse(
                    data = LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setPartitionErrors(partitions),
                    version = version,
                )
            } else {
                val id = Uuid.randomUuid()
                val topics = createTopic(
                    id = id,
                    errors = listOf(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED),
                )
                LeaderAndIsrResponse(
                    data = LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setTopics(topics),
                    version = version,
                )
            }
            val errorCounts = response.errorCounts()
            assertEquals(2, errorCounts.size)
            assertEquals(2, errorCounts[Errors.NONE])
            assertEquals(1, errorCounts[Errors.CLUSTER_AUTHORIZATION_FAILED])
        }
    }

    @Test
    fun testToString() {
        for (version in ApiKeys.LEADER_AND_ISR.allVersions()) {
            var response: LeaderAndIsrResponse
            if (version < 5) {
                val partitions = createPartitions(
                    topicName = "foo",
                    errors = listOf(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED),
                )
                response = LeaderAndIsrResponse(
                    data = LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setPartitionErrors(partitions),
                    version = version,
                )
                val responseStr = response.toString()
                assertContains(responseStr, LeaderAndIsrResponse::class.java.getSimpleName())
                assertContains(responseStr, partitions.toString())
                assertContains(responseStr, "errorCode=" + Errors.NONE.code)
            } else {
                val id = Uuid.randomUuid()
                val topics = createTopic(
                    id = id,
                    errors = listOf(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED),
                )
                response = LeaderAndIsrResponse(
                    data = LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code)
                        .setTopics(topics),
                    version = version,
                )
                val responseStr = response.toString()
                assertContains(responseStr, LeaderAndIsrResponse::class.java.getSimpleName())
                assertContains(responseStr, topics.toString())
                assertContains(responseStr, id.toString())
                assertContains(responseStr, "errorCode=" + Errors.NONE.code)
            }
        }
    }

    private fun createPartitions(topicName: String, errors: List<Errors>): List<LeaderAndIsrPartitionError> {
        return errors.withIndex().map { (partitionIndex, error) ->
            LeaderAndIsrPartitionError()
                .setTopicName(topicName)
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code)
        }
    }

    private fun createTopic(id: Uuid, errors: List<Errors>): LeaderAndIsrTopicErrorCollection {
        val topics = LeaderAndIsrTopicErrorCollection()
        val topic = LeaderAndIsrTopicError()
        topic.setTopicId(id)
        val partitions = errors.withIndex().map { (partitionIndex, error) ->
            LeaderAndIsrPartitionError()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code)
        }
        topic.setPartitionErrors(partitions)
        topics.add(topic)
        return topics
    }
}
