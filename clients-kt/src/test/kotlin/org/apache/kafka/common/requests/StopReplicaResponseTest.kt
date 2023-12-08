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

import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState
import org.apache.kafka.common.message.StopReplicaResponseData
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

class StopReplicaResponseTest {

    @Test
    fun testErrorCountsFromGetErrorResponse() {
        val topicStates = listOf(
            StopReplicaTopicState()
                .setTopicName("foo")
                .setPartitionStates(
                    listOf(
                        StopReplicaPartitionState().setPartitionIndex(0),
                        StopReplicaPartitionState().setPartitionIndex(1),
                    )
                ),
        )
        for (version in ApiKeys.STOP_REPLICA.allVersions()) {
            val request = StopReplicaRequest.Builder(
                version = version,
                controllerId = 15,
                controllerEpoch = 20,
                brokerEpoch = 0,
                deletePartitions = false,
                topicStates = topicStates,
            ).build(version)
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = Errors.CLUSTER_AUTHORIZATION_FAILED.exception!!
            )
            assertEquals(mapOf(Errors.CLUSTER_AUTHORIZATION_FAILED to 3), response.errorCounts())
        }
    }

    @Test
    fun testErrorCountsWithTopLevelError() {
        val errors = listOf(
            StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(0),
            StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(1)
                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code),
        )
        val response = StopReplicaResponse(
            StopReplicaResponseData()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
                .setPartitionErrors(errors)
        )
        assertEquals(mapOf(Errors.UNKNOWN_SERVER_ERROR to 3), response.errorCounts())
    }

    @Test
    fun testErrorCountsNoTopLevelError() {
        val errors = listOf(
            StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(0),
            StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(1)
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code),
        )
        val response = StopReplicaResponse(
            StopReplicaResponseData()
                .setErrorCode(Errors.NONE.code)
                .setPartitionErrors(errors)
        )
        val errorCounts = response.errorCounts()
        assertEquals(2, errorCounts.size)
        assertEquals(2, errorCounts[Errors.NONE])
        assertEquals(1, errorCounts[Errors.CLUSTER_AUTHORIZATION_FAILED])
    }

    @Test
    fun testToString() {
        val errors: MutableList<StopReplicaPartitionError> = ArrayList()
        errors.add(StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(0))
        errors.add(
            StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(1)
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
        )
        val response = StopReplicaResponse(StopReplicaResponseData().setPartitionErrors(errors))
        val responseStr = response.toString()
        assertContains(responseStr, StopReplicaResponse::class.java.getSimpleName())
        assertContains(responseStr, errors.toString())
        assertContains(responseStr, "errorCode=${Errors.NONE.code}")
    }
}
