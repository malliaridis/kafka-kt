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
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetCommitRequest.Companion.getErrorResponse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

open class OffsetCommitRequestTest {

    private lateinit var data: OffsetCommitRequestData

    private lateinit var topics: List<OffsetCommitRequestTopic>
    
    @BeforeEach
    open fun setUp() {
        topics = listOf(
            OffsetCommitRequestTopic()
                .setName(TOPIC_ONE)
                .setPartitions(
                    listOf(
                        OffsetCommitRequestPartition()
                            .setPartitionIndex(PARTITION_ONE)
                            .setCommittedOffset(OFFSET)
                            .setCommittedLeaderEpoch(LEADER_EPOCH.toInt())
                            .setCommittedMetadata(METADATA)
                    )
                ),
            OffsetCommitRequestTopic()
                .setName(TOPIC_TWO)
                .setPartitions(
                    listOf(
                        OffsetCommitRequestPartition()
                            .setPartitionIndex(PARTITION_TWO)
                            .setCommittedOffset(OFFSET)
                            .setCommittedLeaderEpoch(LEADER_EPOCH.toInt())
                            .setCommittedMetadata(METADATA)
                    )
                ),
        )
        data = OffsetCommitRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(topics)
    }

    @Test
    open fun testConstructor() {
        val expectedOffsets = mapOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE) to OFFSET,
            TopicPartition(TOPIC_TWO, PARTITION_TWO) to OFFSET,
        )
        val builder = OffsetCommitRequest.Builder(data)
        for (version in ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            val request = builder.build(version)
            assertEquals(expectedOffsets, request.offsets())
            val response = request.getErrorResponse(
                throttleTimeMs = THROTTLE_TIME_MS,
                e = Errors.NOT_COORDINATOR.exception!!,
            )
            assertEquals(mapOf(Errors.NOT_COORDINATOR to 2), response.errorCounts())
            assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
        }
    }

    @Test
    fun testVersionSupportForGroupInstanceId() {
        val builder = OffsetCommitRequest.Builder(
            OffsetCommitRequestData()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID)
                .setGroupInstanceId(GROUP_INSTANCE_ID)
        )
        for (version in ApiKeys.OFFSET_COMMIT.allVersions()) {
            if (version >= 7) builder.build(version)
            else assertFailsWith<UnsupportedVersionException> { builder.build(version) }
        }
    }

    @Test
    fun testGetErrorResponse() {
        val expectedResponse = OffsetCommitResponseData()
            .setTopics(
                listOf(
                    OffsetCommitResponseTopic()
                        .setName(TOPIC_ONE)
                        .setPartitions(
                            listOf(
                                OffsetCommitResponsePartition()
                                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
                                    .setPartitionIndex(PARTITION_ONE)
                            )
                        ),
                    OffsetCommitResponseTopic()
                        .setName(TOPIC_TWO)
                        .setPartitions(
                            listOf(
                                OffsetCommitResponsePartition()
                                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
                                    .setPartitionIndex(PARTITION_TWO)
                            )
                        )
                )
            )

        assertEquals(expectedResponse, getErrorResponse(data, Errors.UNKNOWN_MEMBER_ID))
    }

    companion object {

        internal const val GROUP_ID = "groupId"

        internal const val MEMBER_ID = "consumerId"

        internal const val GROUP_INSTANCE_ID = "groupInstanceId"

        internal const val TOPIC_ONE = "topicOne"

        internal const val TOPIC_TWO = "topicTwo"

        internal const val PARTITION_ONE = 1

        internal const val PARTITION_TWO = 2

        internal const val OFFSET = 100L

        internal const val LEADER_EPOCH: Short = 20

        internal const val METADATA = "metadata"

        internal const val THROTTLE_TIME_MS = 10
    }
}
