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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.requests.RequestTestUtils.metadataResponse
import org.apache.kafka.common.requests.RequestTestUtils.metadataUpdateWithIds
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import java.util.regex.Pattern
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ConsumerMetadataTest {
    
    private val node = Node(id = 1, host = "localhost", port = 9092)
    
    private val subscription = SubscriptionState(LogContext(), OffsetResetStrategy.EARLIEST)
    
    private val time: Time = MockTime()
    
    @Test
    fun testPatternSubscriptionNoInternalTopics() {
        testPatternSubscription(false)
    }

    @Test
    fun testPatternSubscriptionIncludeInternalTopics() {
        testPatternSubscription(true)
    }

    private fun testPatternSubscription(includeInternalTopics: Boolean) {
        subscription.subscribe(Pattern.compile("__.*"), NoOpConsumerRebalanceListener())
        val metadata = newConsumerMetadata(includeInternalTopics)
        val builder = metadata.newMetadataRequestBuilder()
        assertTrue(builder.isAllTopics)
        val topics = listOf(
            topicMetadata(topic = "__consumer_offsets", isInternal = true),
            topicMetadata(topic = "__matching_topic", isInternal = false),
            topicMetadata(topic = "non_matching_topic", isInternal = false),
        )
        val response = metadataResponse(
            brokers = listOf(node),
            clusterId = "clusterId",
            controllerId = node.id,
            topicMetadataList = topics,
        )
        metadata.updateWithCurrentRequestVersion(
            response = response,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        if (includeInternalTopics) assertEquals(
            expected = setOf(
                "__matching_topic",
                "__consumer_offsets"
            ),
            actual = metadata.fetch().topics,
        ) else assertEquals(
            expected = setOf("__matching_topic"),
            actual = metadata.fetch().topics,
        )
    }

    @Test
    fun testUserAssignment() {
        subscription.assignFromUser(
            setOf(
                TopicPartition(topic = "foo", partition = 0),
                TopicPartition(topic = "bar", partition = 0),
                TopicPartition(topic = "__consumer_offsets", partition = 0),
            )
        )
        testBasicSubscription(
            expectedTopics = setOf("foo", "bar"),
            expectedInternalTopics = setOf("__consumer_offsets"),
        )
        subscription.assignFromUser(
            setOf(
                TopicPartition(topic = "baz", partition = 0),
                TopicPartition(topic = "__consumer_offsets", partition = 0),
            )
        )
        testBasicSubscription(
            expectedTopics = setOf("baz"),
            expectedInternalTopics = setOf("__consumer_offsets"),
        )
    }

    @Test
    fun testNormalSubscription() {
        subscription.subscribe(
            topics = setOf("foo", "bar", "__consumer_offsets"),
            listener = NoOpConsumerRebalanceListener(),
        )
        subscription.groupSubscribe(setOf("baz", "foo", "bar", "__consumer_offsets"))
        testBasicSubscription(
            expectedTopics = setOf("foo", "bar", "baz"),
            expectedInternalTopics = setOf("__consumer_offsets"),
        )
        subscription.resetGroupSubscription()
        testBasicSubscription(
            expectedTopics = setOf("foo", "bar"),
            expectedInternalTopics = setOf("__consumer_offsets"),
        )
    }

    @Test
    fun testTransientTopics() {
        val topicIds = mutableMapOf<String, Uuid>()
        topicIds["foo"] = Uuid.randomUuid()
        subscription.subscribe(setOf("foo"), NoOpConsumerRebalanceListener())
        val metadata = newConsumerMetadata(false)
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = mapOf("foo" to 1),
                topicIds = topicIds,
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        assertEquals(topicIds["foo"], metadata.topicIds()["foo"])
        assertFalse(metadata.updateRequested())
        metadata.addTransientTopics(setOf("foo"))
        assertFalse(metadata.updateRequested())
        metadata.addTransientTopics(setOf("bar"))
        assertTrue(metadata.updateRequested())
        val topicPartitionCounts: MutableMap<String, Int> = HashMap()
        topicPartitionCounts["foo"] = 1
        topicPartitionCounts["bar"] = 1
        topicIds["bar"] = Uuid.randomUuid()
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = topicPartitionCounts,
                topicIds = topicIds
            ),
            isPartialUpdate = false,
            nowMs = time.milliseconds()
        )
        val metadataTopicIds: Map<String, Uuid> = metadata.topicIds()
        topicIds.forEach { (topicName, topicId) ->
            assertEquals(
                expected = topicId,
                actual = metadataTopicIds[topicName],
            )
        }
        assertFalse(metadata.updateRequested())
        assertEquals(setOf("foo", "bar"), HashSet(metadata.fetch().topics))
        metadata.clearTransientTopics()
        topicIds.remove("bar")
        metadata.updateWithCurrentRequestVersion(
            response = metadataUpdateWithIds(
                numNodes = 1,
                topicPartitionCounts = topicPartitionCounts,
                topicIds = topicIds,
            ), isPartialUpdate = false, nowMs = time.milliseconds()
        )
        assertEquals(setOf("foo"), HashSet(metadata.fetch().topics))
        assertEquals(topicIds["foo"], metadata.topicIds()["foo"])
        assertEquals(topicIds["bar"], null)
    }

    private fun testBasicSubscription(
        expectedTopics: Set<String>,
        expectedInternalTopics: Set<String>,
    ) {
        val allTopics: MutableSet<String> = HashSet()
        allTopics.addAll(expectedTopics)
        allTopics.addAll(expectedInternalTopics)
        val metadata = newConsumerMetadata(false)
        val builder = metadata.newMetadataRequestBuilder()
        assertEquals(allTopics, HashSet(builder.topics()))
        val topics: MutableList<MetadataResponse.TopicMetadata> = ArrayList()
        for (expectedTopic in expectedTopics) topics.add(topicMetadata(expectedTopic, false))
        for (expectedInternalTopic in expectedInternalTopics) topics.add(
            topicMetadata(
                expectedInternalTopic,
                true
            )
        )
        val response = metadataResponse(
            brokers = listOf(node),
            clusterId = "clusterId",
            controllerId = node.id,
            topicMetadataList = topics,
        )
        metadata.updateWithCurrentRequestVersion(
            response = response,
            isPartialUpdate = false,
            nowMs = time.milliseconds(),
        )
        assertEquals(allTopics, metadata.fetch().topics)
    }

    private fun topicMetadata(topic: String, isInternal: Boolean): MetadataResponse.TopicMetadata {
        val partitionMetadata = PartitionMetadata(
            error = Errors.NONE,
            topicPartition = TopicPartition(topic, 0),
            leaderId = node.id,
            leaderEpoch = 5,
            replicaIds = listOf(node.id),
            inSyncReplicaIds = listOf(node.id),
            offlineReplicaIds = listOf(node.id),
        )
        return MetadataResponse.TopicMetadata(
            error = Errors.NONE,
            topic = topic,
            isInternal = isInternal,
            partitionMetadata = listOf(partitionMetadata),
        )
    }

    private fun newConsumerMetadata(includeInternalTopics: Boolean): ConsumerMetadata {
        val refreshBackoffMs: Long = 50
        val expireMs: Long = 50000
        return ConsumerMetadata(
            refreshBackoffMs, expireMs, includeInternalTopics, false,
            subscription, LogContext(), ClusterResourceListeners()
        )
    }
}

