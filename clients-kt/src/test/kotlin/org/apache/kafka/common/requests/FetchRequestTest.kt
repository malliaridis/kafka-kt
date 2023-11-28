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

import java.util.stream.Stream
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class FetchRequestTest {

    @ParameterizedTest
    @MethodSource("fetchVersions")
    fun testToReplaceWithDifferentVersions(version: Short) {
        val fetchRequestUsesTopicIds = version >= 13
        val topicId = Uuid.randomUuid()
        val tp = TopicIdPartition(topicId = topicId, partition = 0, topic = "topic")
        val partitionData = mapOf(
            tp.topicPartition to PartitionData(
                topicId = topicId,
                fetchOffset = 0,
                logStartOffset = 0,
                maxBytes = 0,
                currentLeaderEpoch = null,
            )
        )
        val toReplace = listOf(tp)
        val fetchRequest = FetchRequest.Builder
            .forReplica(
                allowedVersion = version,
                replicaId = 0,
                maxWait = 1,
                minBytes = 1,
                fetchData = partitionData,
            )
            .removed(emptyList())
            .replaced(toReplace)
            .metadata(FetchMetadata.newIncremental(123)).build(version)

        // If version < 13, we should not see any partitions in forgottenTopics. This is because we can not
        // distinguish different topic IDs on versions earlier than 13.
        assertEquals(fetchRequestUsesTopicIds, fetchRequest.data().forgottenTopicsData.isNotEmpty())
        fetchRequest.data().forgottenTopicsData.forEach { forgottenTopic ->
            // Since we didn't serialize, we should see the topic name and ID regardless of the version.
            assertEquals(tp.topic, forgottenTopic.topic)
            assertEquals(topicId, forgottenTopic.topicId)
        }
        assertEquals(1, fetchRequest.data().topics.size)
        fetchRequest.data().topics.forEach { topic ->
            // Since we didn't serialize, we should see the topic name and ID regardless of the version.
            assertEquals(tp.topic, topic.topic)
            assertEquals(topicId, topic.topicId)
        }
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    fun testFetchData(version: Short) {
        val topicPartition0 = TopicPartition(topic = "topic", partition = 0)
        val topicPartition1 = TopicPartition(topic = "unknownIdTopic", partition = 0)
        val topicId0 = Uuid.randomUuid()
        val topicId1 = Uuid.randomUuid()

        // Only include topic IDs for the first topic partition.
        val topicNames = mapOf(topicId0 to topicPartition0.topic)
        val topicIdPartitions = listOf(
            TopicIdPartition(topicId = topicId0, topicPartition = topicPartition0),
            TopicIdPartition(topicId = topicId1, topicPartition = topicPartition1),
        )

        // Include one topic with topic IDs in the topic names map and one without.
        val partitionData = mapOf(
            topicPartition0 to PartitionData(
                topicId = topicId0,
                fetchOffset = 0,
                logStartOffset = 0,
                maxBytes = 0,
                currentLeaderEpoch = null,
            ),
            topicPartition1 to PartitionData(
                topicId = topicId1,
                fetchOffset = 0,
                logStartOffset = 0,
                maxBytes = 0,
                currentLeaderEpoch = null,
            ),
        )
        val fetchRequestUsesTopicIds = version >= 13
        val fetchRequest = FetchRequest.parse(
            buffer = FetchRequest.Builder
                .forReplica(
                    allowedVersion = version,
                    replicaId = 0,
                    maxWait = 1,
                    minBytes = 1,
                    fetchData = partitionData,
                )
                .removed(emptyList())
                .replaced(emptyList())
                .metadata(FetchMetadata.newIncremental(123))
                .build(version)
                .serialize(),
            version = version,
        )

        // For versions < 13, we will be provided a topic name and a zero UUID in FetchRequestData.
        // Versions 13+ will contain a valid topic ID but an empty topic name.
        val expectedData = mutableListOf<TopicIdPartition>()
        topicIdPartitions.forEach{ tidp: TopicIdPartition ->
            val expectedName = if (fetchRequestUsesTopicIds) "" else tidp.topic
            val expectedTopicId = if (fetchRequestUsesTopicIds) tidp.topicId else Uuid.ZERO_UUID
            expectedData.add(TopicIdPartition(expectedTopicId, tidp.partition, expectedName))
        }

        // Build the list of TopicIdPartitions based on the FetchRequestData that was serialized and parsed.
        val convertedFetchData = mutableListOf<TopicIdPartition>()
        fetchRequest.data().topics.forEach { topic ->
            topic.partitions.forEach { partition ->
                convertedFetchData.add(
                    TopicIdPartition(
                        topicId = topic.topicId,
                        partition = partition.partition,
                        topic = topic.topic,
                    )
                )
            }
        }
        // The TopicIdPartitions built from the request data should match what we expect.
        assertEquals(expectedData, convertedFetchData)

        // For fetch request version 13+ we expect topic names to be filled in for all topics in the topicNames map.
        // Otherwise, the topic name should be null.
        // For earlier request versions, we expect topic names and zero Uuids.
        val expectedFetchData = mutableMapOf<TopicIdPartition, PartitionData>()
        // Build the expected map based on fetchRequestUsesTopicIds.
        expectedData.forEach { tidp ->
            val expectedName = if (fetchRequestUsesTopicIds) topicNames[tidp.topicId] else tidp.topic
            val tpKey = TopicIdPartition(
                topicId = tidp.topicId,
                topicPartition = TopicPartition(
                    // Kotlin Migration: Use empty string to avoid nullable topic name
                    topic = expectedName ?: "",
                    partition = tidp.partition
                ),
            )
            // logStartOffset was not a valid field in versions 4 and earlier.
            val logStartOffset = if (version > 4) 0 else -1
            expectedFetchData[tpKey] = PartitionData(
                topicId = tidp.topicId,
                fetchOffset = 0,
                logStartOffset = logStartOffset.toLong(),
                maxBytes = 0,
                currentLeaderEpoch = null,
            )
        }
        assertEquals(expectedFetchData, fetchRequest.fetchData(topicNames))
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    fun testForgottenTopics(version: Short) {
        // Forgotten topics are not allowed prior to version 7
        if (version >= 7) {
            val topicPartition0 = TopicPartition(topic = "topic", partition = 0)
            val topicPartition1 = TopicPartition(topic = "unknownIdTopic", partition = 0)
            val topicId0 = Uuid.randomUuid()
            val topicId1 = Uuid.randomUuid()
            // Only include topic IDs for the first topic partition.
            val topicNames = mapOf(topicId0 to topicPartition0.topic)

            // Include one topic with topic IDs in the topic names map and one without.
            val toForgetTopics = mutableListOf<TopicIdPartition>()
            toForgetTopics.add(TopicIdPartition(topicId0, topicPartition0))
            toForgetTopics.add(TopicIdPartition(topicId1, topicPartition1))
            val fetchRequestUsesTopicIds = version >= 13
            val fetchRequest = FetchRequest.parse(
                buffer = FetchRequest.Builder
                    .forReplica(
                        allowedVersion = version,
                        replicaId = 0,
                        maxWait = 1,
                        minBytes = 1,
                        fetchData = emptyMap(),
                    )
                    .removed(toForgetTopics)
                    .replaced(emptyList())
                    .metadata(FetchMetadata.newIncremental(123))
                    .build(version)
                    .serialize(),
                version = version,
            )

            // For versions < 13, we will be provided a topic name and a zero Uuid in FetchRequestData.
            // Versions 13+ will contain a valid topic ID but an empty topic name.
            val expectedForgottenTopicData = toForgetTopics.map { tidp ->
                val expectedName = if (fetchRequestUsesTopicIds) "" else tidp.topic
                val expectedTopicId = if (fetchRequestUsesTopicIds) tidp.topicId else Uuid.ZERO_UUID

                TopicIdPartition(expectedTopicId, tidp.partition, expectedName)
            }

            // Build the list of TopicIdPartitions based on the FetchRequestData that was serialized and parsed.
            val convertedForgottenTopicData = mutableListOf<TopicIdPartition>()
            fetchRequest.data().forgottenTopicsData.forEach { forgottenTopic ->
                forgottenTopic.partitions.forEach { partition ->
                    convertedForgottenTopicData.add(
                        TopicIdPartition(
                            topicId = forgottenTopic.topicId,
                            partition = partition,
                            topic = forgottenTopic.topic
                        )
                    )
                }
            }
            // The TopicIdPartitions built from the request data should match what we expect.
            assertEquals(expectedForgottenTopicData, convertedForgottenTopicData)

            // Get the forgottenTopics from the request data.
            val forgottenTopics = fetchRequest.forgottenTopics(topicNames)

            // For fetch request version 13+ we expect topic names to be filled in for all topics in the topicNames map.
            // Otherwise, the topic name should be null.
            // For earlier request versions, we expect topic names and zero Uuids.
            // Build the list of expected TopicIdPartitions. These are different from the earlier expected
            // topicIdPartitions as empty strings are converted to nulls.
            assertEquals(expectedForgottenTopicData.size, forgottenTopics!!.size)
            val expectedForgottenTopics = expectedForgottenTopicData.map { tidp ->
                val expectedName = if (fetchRequestUsesTopicIds) topicNames[tidp.topicId] else tidp.topic

                TopicIdPartition(
                    topicId = tidp.topicId,
                    topicPartition = TopicPartition(
                        // Kotlin Migration: Use empty string to avoid nullable topic name
                        topic = expectedName ?: "",
                        partition = tidp.partition,
                    )
                )
            }
            assertEquals(expectedForgottenTopics, forgottenTopics)
        }
    }

    @Test
    fun testPartitionDataEquals() {
        assertEquals(
            PartitionData(
                topicId = Uuid.ZERO_UUID,
                fetchOffset = 300,
                logStartOffset = 0L,
                maxBytes = 300,
                currentLeaderEpoch = 300,
            ),
            PartitionData(
                topicId = Uuid.ZERO_UUID,
                fetchOffset = 300,
                logStartOffset = 0L,
                maxBytes = 300,
                currentLeaderEpoch = 300,
            )
        )
        assertNotEquals(
            PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 300,
                logStartOffset = 0L,
                maxBytes = 300,
                currentLeaderEpoch = 300,
            ),
            PartitionData(
                topicId = Uuid.randomUuid(),
                fetchOffset = 300,
                logStartOffset = 0L,
                maxBytes = 300,
                currentLeaderEpoch = 300,
            )
        )
    }

    companion object {

        @JvmStatic
        private fun fetchVersions(): Stream<Arguments> {
            return ApiKeys.FETCH.allVersions().stream().map { version -> Arguments.of(version) }
        }
    }
}
