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

import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.message.ListOffsetsResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ListOffsetsRequestTest {

    @Test
    fun testDuplicatePartitions() {
        val topics = listOf(
            ListOffsetsTopic()
                .setName("topic")
                .setPartitions(
                    listOf(
                        ListOffsetsPartition()
                            .setPartitionIndex(0),
                        ListOffsetsPartition()
                            .setPartitionIndex(0),
                    )
                )
        )
        val data = ListOffsetsRequestData()
            .setTopics(topics)
            .setReplicaId(-1)
        val request = ListOffsetsRequest.parse(
            buffer = toByteBuffer(message = data, version = 0),
            version = 0,
        )
        assertEquals(
            expected = setOf(TopicPartition(topic = "topic", partition = 0)),
            actual = request.duplicatePartitions,
        )
    }

    @Test
    fun testGetErrorResponse() {
        for (version in 1..ApiKeys.LIST_OFFSETS.latestVersion()) {
            val topics = listOf(
                ListOffsetsTopic()
                    .setName("topic")
                    .setPartitions(
                        listOf(
                            ListOffsetsPartition()
                                .setPartitionIndex(0),
                        )
                    ),
            )
            val request = ListOffsetsRequest.Builder
                .forConsumer(
                    requireTimestamp = true,
                    isolationLevel = IsolationLevel.READ_COMMITTED,
                    requireMaxTimestamp = false,
                )
                .setTargetTimes(topics)
                .build(version.toShort())
            val response = request.getErrorResponse(
                throttleTimeMs = 0,
                e = Errors.NOT_LEADER_OR_FOLLOWER.exception!!,
            ) as ListOffsetsResponse
            val v = listOf(
                ListOffsetsTopicResponse()
                    .setName("topic")
                    .setPartitions(
                        listOf(
                            ListOffsetsPartitionResponse()
                                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
                                .setLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
                                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                                .setPartitionIndex(0)
                                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP),
                        )
                    ),
            )
            val data = ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(v)
            val expectedResponse = ListOffsetsResponse(data)
            assertEquals(expectedResponse.data().topics, response.data().topics)
            assertEquals(expectedResponse.throttleTimeMs(), response.throttleTimeMs())
        }
    }

    @Test
    fun testGetErrorResponseV0() {
        val topics = listOf(
            ListOffsetsTopic()
                .setName("topic")
                .setPartitions(
                    listOf(
                        ListOffsetsPartition()
                            .setPartitionIndex(0)
                    )
                )
        )
        val request = ListOffsetsRequest.Builder
            .forConsumer(
                requireTimestamp = true,
                isolationLevel = IsolationLevel.READ_UNCOMMITTED,
                requireMaxTimestamp = false
            )
            .setTargetTimes(topics)
            .build(version = 0)
        val response = request.getErrorResponse(
            throttleTimeMs = 0,
            e = Errors.NOT_LEADER_OR_FOLLOWER.exception!!,
        ) as ListOffsetsResponse
        val v = listOf(
            ListOffsetsTopicResponse()
                .setName("topic")
                .setPartitions(
                    listOf(
                        ListOffsetsPartitionResponse()
                            .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
                            .setOldStyleOffsets(longArrayOf())
                            .setPartitionIndex(0)
                    )
                )
        )
        val data = ListOffsetsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(v)
        val expectedResponse = ListOffsetsResponse(data)
        assertEquals(expectedResponse.data().topics, response.data().topics)
        assertEquals(expectedResponse.throttleTimeMs(), response.throttleTimeMs())
    }

    @Test
    fun testToListOffsetsTopics() {
        val lop0 = ListOffsetsPartition()
            .setPartitionIndex(0)
            .setCurrentLeaderEpoch(1)
            .setMaxNumOffsets(2)
            .setTimestamp(123L)
        val lop1 = ListOffsetsPartition()
            .setPartitionIndex(1)
            .setCurrentLeaderEpoch(3)
            .setMaxNumOffsets(4)
            .setTimestamp(567L)
        val timestampsToSearch = mapOf(
            TopicPartition(topic = "topic", partition = 0) to lop0,
            TopicPartition(topic = "topic", partition = 1) to lop1,
        )
        val listOffsetTopics = ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch)
        assertEquals(1, listOffsetTopics.size)
        val topic = listOffsetTopics[0]
        assertEquals("topic", topic.name)
        assertEquals(2, topic.partitions.size)
        assertTrue(topic.partitions.contains(lop0))
        assertTrue(topic.partitions.contains(lop1))
    }
}
