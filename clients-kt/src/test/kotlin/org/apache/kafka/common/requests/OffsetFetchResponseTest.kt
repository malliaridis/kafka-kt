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
import org.apache.kafka.common.message.OffsetFetchResponseData
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OffsetFetchResponseTest {

    private lateinit var partitionDataMap: MutableMap<TopicPartition, OffsetFetchResponse.PartitionData>

    @BeforeEach
    fun setUp() {
        partitionDataMap = mutableMapOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE) to OffsetFetchResponse.PartitionData(
                offset = OFFSET.toLong(),
                leaderEpoch = LEADER_EPOCH_ONE,
                metadata = METADATA,
                error = Errors.TOPIC_AUTHORIZATION_FAILED,
            ),
            TopicPartition(TOPIC_TWO, PARTITION_TWO) to OffsetFetchResponse.PartitionData(
                offset = OFFSET.toLong(),
                leaderEpoch = LEADER_EPOCH_TWO,
                metadata = METADATA,
                error = Errors.UNKNOWN_TOPIC_OR_PARTITION,
            ),
        )
    }

    @Test
    fun testConstructor() {
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                val response = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    error = Errors.NOT_COORDINATOR,
                    responseData = partitionDataMap,
                )
                assertEquals(Errors.NOT_COORDINATOR, response.error())
                assertEquals(3, response.errorCounts().size)
                assertEquals(
                    expected = mapOf(
                        Errors.NOT_COORDINATOR to 1,
                        Errors.TOPIC_AUTHORIZATION_FAILED to 1,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION to 1,
                    ),
                    actual = response.errorCounts(),
                )
                assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
                val responseData = response.responseDataV0ToV7()
                assertEquals(partitionDataMap, responseData)
                responseData.forEach { (_, data) -> assertTrue(data.hasError()) }
            } else {
                val response = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    errors = mapOf(GROUP_ONE to Errors.NOT_COORDINATOR),
                    responseData = mapOf(GROUP_ONE to partitionDataMap)
                )
                assertEquals(Errors.NOT_COORDINATOR, response.groupLevelError(GROUP_ONE))
                assertEquals(3, response.errorCounts().size)
                assertEquals(
                    expected = mapOf(
                        Errors.NOT_COORDINATOR to 1,
                        Errors.TOPIC_AUTHORIZATION_FAILED to 1,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION to 1,
                    ),
                    actual = response.errorCounts(),
                )
                assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())
                val responseData = response.partitionDataMap(GROUP_ONE)
                assertEquals(partitionDataMap, responseData)
                responseData.forEach { (_, data) -> assertTrue(data.hasError()) }
            }
        }
    }

    @Test
    fun testConstructorWithMultipleGroups() {
        val pd1 = mapOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE) to OffsetFetchResponse.PartitionData(
                OFFSET.toLong(),
                LEADER_EPOCH_ONE,
                METADATA,
                Errors.TOPIC_AUTHORIZATION_FAILED
            ),
        )
        val pd2 = mapOf(
            TopicPartition(TOPIC_TWO, PARTITION_TWO) to OffsetFetchResponse.PartitionData(
                OFFSET.toLong(),
                LEADER_EPOCH_TWO,
                METADATA,
                Errors.UNKNOWN_TOPIC_OR_PARTITION
            ),
        )
        val pd3 = mapOf(
            TopicPartition(TOPIC_THREE, PARTITION_THREE) to OffsetFetchResponse.PartitionData(
                OFFSET.toLong(),
                LEADER_EPOCH_THREE,
                METADATA,
                Errors.NONE
            ),
        )
        val responseData = mapOf(
            GROUP_ONE to pd1,
            GROUP_TWO to pd2,
            GROUP_THREE to pd3,
        )
        val errorMap = mapOf(
            GROUP_ONE to Errors.NOT_COORDINATOR,
            GROUP_TWO to Errors.COORDINATOR_LOAD_IN_PROGRESS,
            GROUP_THREE to Errors.NONE,
        )
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) {
                val response = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    errors = errorMap,
                    responseData = responseData,
                )
                assertEquals(Errors.NOT_COORDINATOR, response.groupLevelError(GROUP_ONE))
                assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, response.groupLevelError(GROUP_TWO))
                assertEquals(Errors.NONE, response.groupLevelError(GROUP_THREE))
                assertTrue(response.groupHasError(GROUP_ONE))
                assertTrue(response.groupHasError(GROUP_TWO))
                assertFalse(response.groupHasError(GROUP_THREE))
                assertEquals(5, response.errorCounts().size)
                assertEquals(
                    expected = mapOf(
                        Errors.NOT_COORDINATOR to 1,
                        Errors.TOPIC_AUTHORIZATION_FAILED to 1,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION to 1,
                        Errors.COORDINATOR_LOAD_IN_PROGRESS to 1,
                        Errors.NONE to 2,
                    ),
                    actual = response.errorCounts(),
                )
                assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs())

                val responseData1 = response.partitionDataMap(GROUP_ONE)
                assertEquals(pd1, responseData1)
                responseData1.forEach { (_, data) -> assertTrue(data.hasError()) }

                val responseData2 = response.partitionDataMap(GROUP_TWO)
                assertEquals(pd2, responseData2)
                responseData2.forEach { (_, data) -> assertTrue(data.hasError()) }

                val responseData3 = response.partitionDataMap(GROUP_THREE)
                assertEquals(pd3, responseData3)
                responseData3.forEach { (_, data) -> assertFalse(data.hasError()) }
            }
        }
    }

    /**
     * Test behavior changes over the versions. Refer to resources.common.messages.OffsetFetchResponse.json
     */
    @Test
    fun testStructBuild() {
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                partitionDataMap[TopicPartition(TOPIC_TWO, PARTITION_TWO)] = OffsetFetchResponse.PartitionData(
                    offset = OFFSET.toLong(),
                    leaderEpoch = LEADER_EPOCH_TWO,
                    metadata = METADATA,
                    error = Errors.GROUP_AUTHORIZATION_FAILED,
                )
                val latestResponse = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    error = Errors.NONE,
                    responseData = partitionDataMap,
                )
                val data = OffsetFetchResponseData(
                    readable = ByteBufferAccessor(latestResponse.serialize(version)),
                    version = version,
                )
                val oldResponse = OffsetFetchResponse(data, version)
                if (version <= 1) {
                    assertEquals(Errors.NONE.code, data.errorCode)

                    // Partition level error populated in older versions.
                    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, oldResponse.error())
                    assertEquals(
                        expected = mapOf(
                            Errors.GROUP_AUTHORIZATION_FAILED to 2,
                            Errors.TOPIC_AUTHORIZATION_FAILED to 1,
                        ),
                        actual = oldResponse.errorCounts(),
                    )
                } else {
                    assertEquals(Errors.NONE.code, data.errorCode)
                    assertEquals(Errors.NONE, oldResponse.error())
                    assertEquals(
                        expected = mapOf(
                            Errors.NONE to 1,
                            Errors.GROUP_AUTHORIZATION_FAILED to 1,
                            Errors.TOPIC_AUTHORIZATION_FAILED to 1,
                        ),
                        actual = oldResponse.errorCounts(),
                    )
                }
                if (version <= 2) assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, oldResponse.throttleTimeMs())
                else assertEquals(THROTTLE_TIME_MS, oldResponse.throttleTimeMs())
                val expectedDataMap: MutableMap<TopicPartition, OffsetFetchResponse.PartitionData> = HashMap()
                for ((key, partitionData) in partitionDataMap) {
                    expectedDataMap[key] = OffsetFetchResponse.PartitionData(
                        offset = partitionData.offset,
                        leaderEpoch = if (version <= 4) null else partitionData.leaderEpoch,
                        metadata = partitionData.metadata,
                        error = partitionData.error,
                    )
                }
                val responseData = oldResponse.responseDataV0ToV7()
                assertEquals(expectedDataMap, responseData)
                responseData.forEach { (_, rdata) -> assertTrue(rdata.hasError()) }
            } else {
                partitionDataMap[TopicPartition(TOPIC_TWO, PARTITION_TWO)] = OffsetFetchResponse.PartitionData(
                    offset = OFFSET.toLong(),
                    leaderEpoch = LEADER_EPOCH_TWO,
                    metadata = METADATA,
                    error = Errors.GROUP_AUTHORIZATION_FAILED
                )
                val latestResponse = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    errors = mapOf(GROUP_ONE to Errors.NONE),
                    responseData = mapOf(GROUP_ONE to partitionDataMap),
                )
                val data = OffsetFetchResponseData(
                    ByteBufferAccessor(latestResponse.serialize(version)), version
                )
                val oldResponse = OffsetFetchResponse(data, version)
                assertEquals(Errors.NONE.code, data.groups[0].errorCode)
                assertEquals(Errors.NONE, oldResponse.groupLevelError(GROUP_ONE))
                assertEquals(
                    expected = mapOf(
                        Errors.NONE to 1,
                        Errors.GROUP_AUTHORIZATION_FAILED to 1,
                        Errors.TOPIC_AUTHORIZATION_FAILED to 1,
                    ),
                    actual = oldResponse.errorCounts(),
                )
                assertEquals(THROTTLE_TIME_MS, oldResponse.throttleTimeMs())
                val expectedDataMap = partitionDataMap.entries.associate { (key, partitionData) ->
                    key to OffsetFetchResponse.PartitionData(
                        offset = partitionData.offset,
                        leaderEpoch = partitionData.leaderEpoch,
                        metadata = partitionData.metadata,
                        error = partitionData.error,
                    )
                }
                val responseData = oldResponse.partitionDataMap(GROUP_ONE)
                assertEquals(expectedDataMap, responseData)
                responseData.forEach { (_, rdata) -> assertTrue(rdata.hasError()) }
            }
        }
    }

    @Test
    fun testShouldThrottle() {
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                val response = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    error = Errors.NONE,
                    responseData = partitionDataMap,
                )
                if (version >= 4) assertTrue(response.shouldClientThrottle(version))
                else assertFalse(response.shouldClientThrottle(version))
            } else {
                val response = OffsetFetchResponse(
                    throttleTimeMs = THROTTLE_TIME_MS,
                    errors = mapOf(GROUP_ONE to Errors.NOT_COORDINATOR),
                    responseData = mapOf(GROUP_ONE to partitionDataMap),
                )
                assertTrue(response.shouldClientThrottle(version))
            }
        }
    }

    @Test
    fun testNullableMetadataV0ToV7() {
        val pd = OffsetFetchResponse.PartitionData(
            offset = OFFSET.toLong(),
            leaderEpoch = LEADER_EPOCH_ONE,
            metadata = null,
            error = Errors.UNKNOWN_TOPIC_OR_PARTITION,
        )
        // test PartitionData.equals with null metadata
        assertEquals(pd, pd)
        partitionDataMap.clear()
        partitionDataMap[TopicPartition(TOPIC_ONE, PARTITION_ONE)] = pd
        val response = OffsetFetchResponse(
            throttleTimeMs = THROTTLE_TIME_MS,
            error = Errors.GROUP_AUTHORIZATION_FAILED,
            responseData = partitionDataMap,
        )
        val expectedData = OffsetFetchResponseData()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setTopics(
                listOf(
                    OffsetFetchResponseTopic()
                        .setName(TOPIC_ONE)
                        .setPartitions(
                            listOf(
                                OffsetFetchResponsePartition()
                                    .setPartitionIndex(PARTITION_ONE)
                                    .setCommittedOffset(OFFSET.toLong())
                                    .setCommittedLeaderEpoch(LEADER_EPOCH_ONE)
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                    .setMetadata(null)
                            )
                        )
                )
            )
        assertEquals(expectedData, response.data())
    }

    @Test
    fun testNullableMetadataV8AndAbove() {
        val pd = OffsetFetchResponse.PartitionData(
            offset = OFFSET.toLong(),
            leaderEpoch = LEADER_EPOCH_ONE,
            metadata = null,
            error = Errors.UNKNOWN_TOPIC_OR_PARTITION,
        )
        // test PartitionData.equals with null metadata
        assertEquals(pd, pd)
        partitionDataMap.clear()
        partitionDataMap[TopicPartition(TOPIC_ONE, PARTITION_ONE)] = pd
        val response = OffsetFetchResponse(
            throttleTimeMs = THROTTLE_TIME_MS,
            errors = mapOf(GROUP_ONE to Errors.GROUP_AUTHORIZATION_FAILED),
            responseData = mapOf(GROUP_ONE to partitionDataMap),
        )
        val expectedData = OffsetFetchResponseData()
            .setGroups(
                listOf(
                    OffsetFetchResponseGroup()
                        .setGroupId(GROUP_ONE)
                        .setTopics(
                            listOf(
                                OffsetFetchResponseTopics()
                                    .setName(TOPIC_ONE)
                                    .setPartitions(
                                        listOf(
                                            OffsetFetchResponsePartitions()
                                                .setPartitionIndex(PARTITION_ONE)
                                                .setCommittedOffset(OFFSET.toLong())
                                                .setCommittedLeaderEpoch(LEADER_EPOCH_ONE)
                                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                                .setMetadata(null)
                                        )
                                    )
                            )
                        )
                        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
                )
            )
            .setThrottleTimeMs(THROTTLE_TIME_MS)
        assertEquals(expectedData, response.data())
    }

    @Test
    fun testUseDefaultLeaderEpochV0ToV7() {
        val emptyLeaderEpoch: Int? = null
        partitionDataMap.clear()
        partitionDataMap[TopicPartition(TOPIC_ONE, PARTITION_ONE)] = OffsetFetchResponse.PartitionData(
            offset = OFFSET.toLong(),
            leaderEpoch = emptyLeaderEpoch,
            metadata = METADATA,
            error = Errors.UNKNOWN_TOPIC_OR_PARTITION
        )
        val response = OffsetFetchResponse(
            throttleTimeMs = THROTTLE_TIME_MS,
            error = Errors.NOT_COORDINATOR,
            responseData = partitionDataMap,
        )
        val expectedData = OffsetFetchResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code)
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setTopics(
                listOf(
                    OffsetFetchResponseTopic()
                        .setName(TOPIC_ONE)
                        .setPartitions(
                            listOf(
                                OffsetFetchResponsePartition()
                                    .setPartitionIndex(PARTITION_ONE)
                                    .setCommittedOffset(OFFSET.toLong())
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                    .setMetadata(METADATA)
                            )
                        )
                )
            )
        assertEquals(expectedData, response.data())
    }

    @Test
    fun testUseDefaultLeaderEpochV8() {
        val emptyLeaderEpoch: Int? = null
        partitionDataMap.clear()
        partitionDataMap[TopicPartition(TOPIC_ONE, PARTITION_ONE)] = OffsetFetchResponse.PartitionData(
            offset = OFFSET.toLong(),
            leaderEpoch = emptyLeaderEpoch,
            metadata = METADATA,
            error = Errors.UNKNOWN_TOPIC_OR_PARTITION,
        )
        val response = OffsetFetchResponse(
            throttleTimeMs = THROTTLE_TIME_MS,
            errors = mapOf(GROUP_ONE to Errors.NOT_COORDINATOR),
            responseData = mapOf(GROUP_ONE to partitionDataMap),
        )
        val expectedData = OffsetFetchResponseData()
            .setGroups(
                listOf(
                    OffsetFetchResponseGroup()
                        .setGroupId(GROUP_ONE)
                        .setTopics(
                            listOf(
                                OffsetFetchResponseTopics()
                                    .setName(TOPIC_ONE)
                                    .setPartitions(
                                        listOf(
                                            OffsetFetchResponsePartitions()
                                                .setPartitionIndex(PARTITION_ONE)
                                                .setCommittedOffset(OFFSET.toLong())
                                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                                .setMetadata(METADATA)
                                        )
                                    )
                            )
                        )
                        .setErrorCode(Errors.NOT_COORDINATOR.code)
                )
            )
            .setThrottleTimeMs(THROTTLE_TIME_MS)
        assertEquals(expectedData, response.data())
    }

    companion object {

        private const val THROTTLE_TIME_MS = 10

        private const val OFFSET = 100

        private const val METADATA = "metadata"

        private const val GROUP_ONE = "group1"

        private const val GROUP_TWO = "group2"

        private const val GROUP_THREE = "group3"

        private const val TOPIC_ONE = "topic1"

        private const val PARTITION_ONE = 1

        private const val LEADER_EPOCH_ONE = 1

        private const val TOPIC_TWO = "topic2"

        private const val PARTITION_TWO = 2

        private const val LEADER_EPOCH_TWO = 2

        private const val TOPIC_THREE = "topic3"

        private const val PARTITION_THREE = 3

        private const val LEADER_EPOCH_THREE = 3
    }
}
