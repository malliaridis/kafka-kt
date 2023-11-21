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
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetFetchRequest.NoBatchedOffsetFetchRequestException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OffsetFetchRequestTest {
    
    private lateinit var builder: OffsetFetchRequest.Builder
    
    @Test
    fun testConstructor() {
        val partitions = listOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE),
            TopicPartition(TOPIC_TWO, PARTITION_TWO),
        )
        val throttleTimeMs = 10
        val expectedData = partitions.associateWith {
            OffsetFetchResponse.PartitionData(
                offset = OffsetFetchResponse.INVALID_OFFSET,
                leaderEpoch = null,
                metadata = OffsetFetchResponse.NO_METADATA,
                error = Errors.NONE,
            )
        }
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                builder = OffsetFetchRequest.Builder(
                    groupId = GROUP_1,
                    requireStable = false,
                    partitions = partitions,
                    throwOnFetchStableOffsetsUnsupported = false,
                )
                assertFalse(builder.isAllTopicPartitions)
                val request = builder.build(version)
                assertFalse(request.isAllPartitions)
                assertEquals(GROUP_1, request.groupId())
                assertEquals(partitions, request.partitions())
                val response = request.getErrorResponse(throttleTimeMs, Errors.NONE)
                assertEquals(Errors.NONE, response.error())
                assertFalse(response.hasError())
                assertEquals(
                    expected = mapOf(Errors.NONE to if (version <= 1.toShort()) 3 else 1),
                    actual = response.errorCounts(),
                    message = "Incorrect error count for version $version",
                )
                if (version <= 1) assertEquals(expectedData, response.responseDataV0ToV7())
                if (version >= 3) assertEquals(throttleTimeMs, response.throttleTimeMs())
                else assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs())
            } else {
                builder = OffsetFetchRequest.Builder(
                    groupIdToTopicPartitionMap = mapOf(GROUP_1 to partitions),
                    requireStable = false,
                    throwOnFetchStableOffsetsUnsupported = false,
                )
                val request = builder.build(version)
                val groupToPartitionMap = request.groupIdsToPartitions()
                val groupToTopicMap = request.groupIdsToTopics()
                assertFalse(request.isAllPartitionsForGroup(GROUP_1))
                assertTrue(groupToPartitionMap.containsKey(GROUP_1) && groupToTopicMap.containsKey(GROUP_1))
                assertEquals(partitions, groupToPartitionMap[GROUP_1])
                val response = request.getErrorResponse(throttleTimeMs, Errors.NONE)
                assertEquals(Errors.NONE, response.groupLevelError(GROUP_1))
                assertFalse(response.groupHasError(GROUP_1))
                assertEquals(
                    expected = mapOf(Errors.NONE to 1),
                    actual = response.errorCounts(),
                    message = "Incorrect error count for version $version",
                )
                assertEquals(throttleTimeMs, response.throttleTimeMs())
            }
        }
    }

    @Test
    fun testConstructorWithMultipleGroups() {
        val topic1Partitions = listOf(
            TopicPartition(TOPIC_ONE, PARTITION_ONE),
            TopicPartition(TOPIC_ONE, PARTITION_TWO),
        )
        val topic2Partitions = listOf(
            TopicPartition(TOPIC_TWO, PARTITION_ONE),
            TopicPartition(TOPIC_TWO, PARTITION_TWO),
        )
        val topic3Partitions = listOf(
            TopicPartition(TOPIC_THREE, PARTITION_ONE),
            TopicPartition(TOPIC_THREE, PARTITION_TWO),
        )
        val groupToTp = mapOf(
            GROUP_1 to topic1Partitions,
            GROUP_2 to topic2Partitions,
            GROUP_3 to topic3Partitions,
            GROUP_4 to null,
            GROUP_5 to null,
        )
        val throttleTimeMs = 10
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) {
                builder = OffsetFetchRequest.Builder(
                    groupIdToTopicPartitionMap = groupToTp,
                    requireStable = false,
                    throwOnFetchStableOffsetsUnsupported = false,
                )
                val request = builder.build(version)
                val groupToPartitionMap = request.groupIdsToPartitions()
                val groupToTopicMap = request.groupIdsToTopics()
                assertEquals(groupToTp.keys, groupToTopicMap.keys)
                assertEquals(groupToTp.keys, groupToPartitionMap.keys)
                assertFalse(request.isAllPartitionsForGroup(GROUP_1))
                assertFalse(request.isAllPartitionsForGroup(GROUP_2))
                assertFalse(request.isAllPartitionsForGroup(GROUP_3))
                assertTrue(request.isAllPartitionsForGroup(GROUP_4))
                assertTrue(request.isAllPartitionsForGroup(GROUP_5))
                val response = request.getErrorResponse(throttleTimeMs, Errors.NONE)
                for (group in groups) {
                    assertEquals(Errors.NONE, response.groupLevelError(group))
                    assertFalse(response.groupHasError(group))
                }
                assertEquals(
                    expected = mapOf(Errors.NONE to 5),
                    actual = response.errorCounts(),
                    message = "Incorrect error count for version $version",
                )
                assertEquals(throttleTimeMs, response.throttleTimeMs())
            }
        }
    }

    @Test
    fun testBuildThrowForUnsupportedBatchRequest() {
        for (version in listOfVersionsNonBatchOffsetFetch) {
            val groupPartitionMap = mapOf(
                GROUP_1 to null,
                GROUP_2 to null,
            )
            builder = OffsetFetchRequest.Builder(
                groupIdToTopicPartitionMap = groupPartitionMap,
                requireStable = true,
                throwOnFetchStableOffsetsUnsupported = false
            )
            val finalVersion = version.toShort()
            assertFailsWith<NoBatchedOffsetFetchRequestException> { builder.build(finalVersion) }
        }
    }

    @Test
    fun testConstructorFailForUnsupportedRequireStable() {
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                // The builder needs to be initialized every cycle as the internal data `requireStable` flag is flipped.
                builder = OffsetFetchRequest.Builder(
                    groupId = GROUP_1,
                    requireStable = true,
                    partitions = null,
                    throwOnFetchStableOffsetsUnsupported = false,
                )
                if (version < 2) {
                    assertFailsWith<UnsupportedVersionException> { builder.build(version) }
                } else {
                    val request = builder.build(version)
                    assertEquals(GROUP_1, request.groupId())
                    assertNull(request.partitions())
                    assertTrue(request.isAllPartitions)
                    if (version < 7) assertFalse(request.requireStable())
                    else assertTrue(request.requireStable())
                }
            } else {
                builder = OffsetFetchRequest.Builder(
                    groupIdToTopicPartitionMap = mapOf(GROUP_1 to null),
                    requireStable = true,
                    throwOnFetchStableOffsetsUnsupported = false,
                )
                val request = builder.build(version)
                val groupToPartitionMap = request.groupIdsToPartitions()
                val groupToTopicMap = request.groupIdsToTopics()
                assertTrue(groupToPartitionMap.containsKey(GROUP_1) && groupToTopicMap.containsKey(GROUP_1))
                assertNull(groupToPartitionMap[GROUP_1])
                assertTrue(request.isAllPartitionsForGroup(GROUP_1))
                assertTrue(request.requireStable())
            }
        }
    }

    @Test
    fun testBuildThrowForUnsupportedRequireStable() {
        for (version in listOfVersionsNonBatchOffsetFetch) {
            builder = OffsetFetchRequest.Builder(
                groupId = GROUP_1,
                requireStable = true,
                partitions = null,
                throwOnFetchStableOffsetsUnsupported = true,
            )
            if (version < 7) {
                val finalVersion = version.toShort()
                assertFailsWith<UnsupportedVersionException> { builder.build(finalVersion) }
            } else {
                val request = builder.build(version.toShort())
                assertTrue(request.requireStable())
            }
        }
    }
    
    companion object {
        
        private const val TOPIC_ONE = "topic1"
        
        private const val PARTITION_ONE = 1
        
        private const val TOPIC_TWO = "topic2"
        
        private const val PARTITION_TWO = 2
        
        private const val TOPIC_THREE = "topic3"
        
        private const val GROUP_1 = "group1"
        
        private const val GROUP_2 = "group2"
        
        private const val GROUP_3 = "group3"
        
        private const val GROUP_4 = "group4"
        
        private const val GROUP_5 = "group5"
        
        private val groups = listOf(GROUP_1, GROUP_2, GROUP_3, GROUP_4, GROUP_5)
        
        private val listOfVersionsNonBatchOffsetFetch: List<Int> = mutableListOf(0, 1, 2, 3, 4, 5, 6, 7)
    }
}
