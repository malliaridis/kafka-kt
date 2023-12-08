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

package org.apache.kafka.clients.consumer

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.StickyAssignor.Companion.serializeTopicPartitionAssignment
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.MemberData
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.utils.CollectionUtils.groupPartitionsByTopic
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StickyAssignorTest : AbstractStickyAssignorTest() {

    public override fun createAssignor(): AbstractStickyAssignor = StickyAssignor()

    public override fun buildSubscriptionV0(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): Subscription {
        return Subscription(
            topics = topics,
            userData = serializeTopicPartitionAssignment(
                MemberData(partitions, generationId)
            ),
            ownedPartitions = emptyList(),
            generationId = AbstractStickyAssignor.DEFAULT_GENERATION,
            rackId = null,
        )
    }

    public override fun buildSubscriptionV1(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): Subscription {
        return Subscription(
            topics = topics,
            userData = serializeTopicPartitionAssignment(MemberData(partitions, generationId)),
            ownedPartitions = partitions,
            generationId = AbstractStickyAssignor.DEFAULT_GENERATION,
            rackId = null,
        )
    }

    public override fun buildSubscriptionV2Above(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): Subscription {
        return Subscription(
            topics = topics,
            userData = serializeTopicPartitionAssignment(MemberData(partitions, generationId)),
            ownedPartitions = partitions,
            generationId = generationId,
            rackId = null,
        )
    }

    public override fun generateUserData(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generation: Int,
    ): ByteBuffer = serializeTopicPartitionAssignment(MemberData(partitions, generation))

    @Test
    fun testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithEqualPartitionsPerConsumer() {
        val partitionsPerTopic: MutableMap<String, Int> = HashMap()
        partitionsPerTopic[topic] = 3
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
            ),
            generationId = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
            ),
            generationId = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generationId = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 1)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 2)),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 0)),
            actual = assignment[consumer3]!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithUnequalPartitionsPerConsumer() {
        val partitionsPerTopic = mapOf(topic to 4)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
            ),
            generationId = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
            ),
            generationId = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generationId = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = mutableListOf(
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic, partition = 3),
            ),
            actual = assignment[consumer1],
        )
        assertEquals(
            expected = mutableListOf(TopicPartition(topic = topic, partition = 2)),
            actual = assignment[consumer2],
        )
        assertEquals(
            expected = mutableListOf(TopicPartition(topic = topic, partition = 0)),
            actual = assignment[consumer3],
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = "testAssignmentWithMultipleGenerations1 with isAllSubscriptionsEqual: {0}")
    @ValueSource(booleans = [true, false])
    fun testAssignmentWithMultipleGenerations1(isAllSubscriptionsEqual: Boolean) {
        val allTopics = listOf(topic, topic2)
        val consumer2SubscribedTopics = if (isAllSubscriptionsEqual) allTopics else listOf(topic)
        val partitionsPerTopic = mapOf(topic to 6, topic2 to 6)
        subscriptions[consumer1] = Subscription(allTopics)
        subscriptions[consumer2] = Subscription(consumer2SubscribedTopics)
        subscriptions[consumer3] = Subscription(allTopics)
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val r1partitions1 = assignment[consumer1]!!
        val r1partitions2 = assignment[consumer2]!!
        val r1partitions3 = assignment[consumer3]!!
        assertTrue(r1partitions1.size == 4 && r1partitions2.size == 4 && r1partitions3.size == 4)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
        subscriptions[consumer1] = buildSubscriptionV2Above(allTopics, r1partitions1, generationId)
        subscriptions[consumer2] = buildSubscriptionV2Above(consumer2SubscribedTopics, r1partitions2, generationId)
        subscriptions.remove(consumer3)
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val r2partitions1 = assignment[consumer1]!!
        val r2partitions2 = assignment[consumer2]!!
        assertTrue(r2partitions1.size == 6 && r2partitions2.size == 6)
        if (isAllSubscriptionsEqual) {
            // only true in all subscription equal case
            assertTrue(r2partitions1.containsAll(r1partitions1))
        }
        assertTrue(r2partitions2.containsAll(r1partitions2))
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
        assertFalse(r2partitions2.none { it in r1partitions3 })
        subscriptions.remove(consumer1)
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = consumer2SubscribedTopics,
            partitions = r2partitions2,
            generationId = 2,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = r1partitions3,
            generationId = 1,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val r3partitions2 = assignment[consumer2]!!
        val r3partitions3 = assignment[consumer3]!!
        assertTrue(r3partitions2.size == 6 && r3partitions3.size == 6)
        assertTrue(r3partitions2.none { it in r3partitions3 })
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = "testAssignmentWithMultipleGenerations2 with isAllSubscriptionsEqual: {0}")
    @ValueSource(booleans = [true, false])
    fun testAssignmentWithMultipleGenerations2(isAllSubscriptionsEqual: Boolean) {
        val allTopics = listOf(topic, topic2, topic3)
        val consumer1SubscribedTopics = if (isAllSubscriptionsEqual) allTopics else listOf(topic)
        val consumer3SubscribedTopics = if (isAllSubscriptionsEqual) allTopics else listOf(topic, topic2)
        val partitionsPerTopic = mapOf(
            topic to 4,
            topic2 to 4,
            topic3 to 4,
        )
        subscriptions[consumer1] = Subscription(consumer1SubscribedTopics)
        subscriptions[consumer2] = Subscription(allTopics)
        subscriptions[consumer3] = Subscription(consumer3SubscribedTopics)
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val r1partitions1 = assignment[consumer1]!!
        val r1partitions2 = assignment[consumer2]!!
        val r1partitions3 = assignment[consumer3]!!
        assertTrue(r1partitions1.size == 4 && r1partitions2.size == 4 && r1partitions3.size == 4)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))

        subscriptions.remove(consumer1)
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = r1partitions2,
            generationId = 1,
        )
        subscriptions.remove(consumer3)
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val r2partitions2 = assignment[consumer2]!!
        assertEquals(12, r2partitions2.size)
        assertTrue(r2partitions2.containsAll(r1partitions2))
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = consumer1SubscribedTopics,
            partitions = r1partitions1,
            generationId = 1,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = r2partitions2,
            generationId = 2,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = consumer3SubscribedTopics,
            partitions = r1partitions3,
            generationId = 1,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val r3partitions1 = assignment[consumer1]!!
        val r3partitions2 = assignment[consumer2]!!
        val r3partitions3 = assignment[consumer3]!!
        assertTrue(r3partitions1.size == 4 && r3partitions2.size == 4 && r3partitions3.size == 4)
        verifyValidityAndBalance(
            subscriptions = subscriptions,
            assignments = assignment,
            partitionsPerTopic = partitionsPerTopic,
        )
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = "testAssignmentWithConflictingPreviousGenerations with isAllSubscriptionsEqual: {0}")
    @ValueSource(booleans = [true, false])
    fun testAssignmentWithConflictingPreviousGenerations(isAllSubscriptionsEqual: Boolean) {
        val partitionsPerTopic: MutableMap<String, Int> = HashMap()
        partitionsPerTopic[topic] = 4
        partitionsPerTopic[topic2] = 4
        partitionsPerTopic[topic3] = 4
        val allTopics = listOf(topic, topic2, topic3)
        val consumer1SubscribedTopics = if (isAllSubscriptionsEqual) allTopics else listOf(topic)
        val consumer2SubscribedTopics = if (isAllSubscriptionsEqual) allTopics else listOf(topic, topic2)
        subscriptions[consumer1] = Subscription(consumer1SubscribedTopics)
        subscriptions[consumer2] = Subscription(consumer2SubscribedTopics)
        subscriptions[consumer3] = Subscription(allTopics)
        val tp0 = TopicPartition(topic = topic, partition = 0)
        val tp1 = TopicPartition(topic = topic, partition = 1)
        val tp2 = TopicPartition(topic = topic, partition = 2)
        val tp3 = TopicPartition(topic = topic, partition = 3)
        val t2p0 = TopicPartition(topic = topic2, partition = 0)
        val t2p1 = TopicPartition(topic = topic2, partition = 1)
        val t2p2 = TopicPartition(topic = topic2, partition = 2)
        val t2p3 = TopicPartition(topic = topic2, partition = 3)
        val t3p0 = TopicPartition(topic = topic3, partition = 0)
        val t3p1 = TopicPartition(topic = topic3, partition = 1)
        val t3p2 = TopicPartition(topic = topic3, partition = 2)
        val t3p3 = TopicPartition(topic = topic3, partition = 3)
        val c1partitions0 =
            if (isAllSubscriptionsEqual) listOf(tp0, tp1, tp2, t2p2, t2p3, t3p0)
            else listOf(tp0, tp1, tp2, tp3)
        val c2partitions0 = listOf(tp0, tp1, t2p0, t2p1, t2p2, t2p3)
        val c3partitions0 = listOf(tp2, tp3, t3p0, t3p1, t3p2, t3p3)
        subscriptions[consumer1] = buildSubscriptionV2Above(consumer1SubscribedTopics, c1partitions0, 1)
        subscriptions[consumer2] = buildSubscriptionV2Above(consumer2SubscribedTopics, c2partitions0, 2)
        subscriptions[consumer3] = buildSubscriptionV2Above(allTopics, c3partitions0, 2)
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val c1partitions = assignment[consumer1]!!
        val c2partitions = assignment[consumer2]!!
        val c3partitions = assignment[consumer3]!!
        assertTrue(c1partitions.size == 4 && c2partitions.size == 4 && c3partitions.size == 4)
        assertTrue(c2partitions0.containsAll(c2partitions))
        assertTrue(c3partitions0.containsAll(c3partitions))
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testSchemaBackwardCompatibility() {
        val partitionsPerTopic = mapOf(topic to 3)
        subscriptions[consumer1] = Subscription(listOf(topic))
        subscriptions[consumer2] = Subscription(listOf(topic))
        subscriptions[consumer3] = Subscription(listOf(topic))
        val tp0 = TopicPartition(topic = topic, partition = 0)
        val tp1 = TopicPartition(topic = topic, partition = 1)
        val tp2 = TopicPartition(topic = topic, partition = 2)
        val c1partitions0 = listOf(tp0, tp2)
        val c2partitions0 = listOf(tp1)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = c1partitions0,
            generationId = 1,
        )
        subscriptions[consumer2] = buildSubscriptionWithOldSchema(
            topics = listOf(topic),
            partitions = c2partitions0,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val c1partitions = assignment[consumer1]!!
        val c2partitions = assignment[consumer2]!!
        val c3partitions = assignment[consumer3]!!
        assertTrue(c1partitions.size == 1 && c2partitions.size == 1 && c3partitions.size == 1)
        assertTrue(c1partitions0.containsAll(c1partitions))
        assertTrue(c2partitions0.containsAll(c2partitions))
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testMemberDataWithInconsistentData() {
        val ownedPartitionsInUserdata = listOf(tp1)
        val ownedPartitionsInSubscription = listOf(tp0)
        assignor.onAssignment(
            ConsumerPartitionAssignor.Assignment(ownedPartitionsInUserdata),
            ConsumerGroupMetadata(
                groupId = groupId,
                generationId = generationId,
                memberId = consumer1,
                groupInstanceId = null,
            )
        )
        val userDataWithHigherGenerationId = assignor.subscriptionUserData(topics = setOf(topic))
        val subscription = Subscription(
            topics = listOf(topic),
            userData = userDataWithHigherGenerationId,
            ownedPartitions = ownedPartitionsInSubscription,
        )
        val (partitions, generation) = memberData(subscription)

        // In StickyAssignor, we'll serialize owned partition in assignment into userData and always honor userData
        assertEquals(
            expected = ownedPartitionsInUserdata,
            actual = partitions,
            message = "subscription: $subscription doesn't have expected owned partition",
        )
        assertEquals(
            expected = generationId,
            actual = generation ?: -1,
            message = "subscription: $subscription doesn't have expected generation id",
        )
    }

    @Test
    fun testMemberDataWillHonorUserData() {
        val topics = listOf(topic)
        val ownedPartitions = listOf(
            TopicPartition(topic = topic1, partition = 0),
            TopicPartition(topic = topic2, partition = 1),
        )
        val generationIdInUserData = generationId - 1
        val subscription = Subscription(
            topics = topics,
            userData = generateUserData(
                topics = topics,
                partitions = ownedPartitions,
                generation = generationIdInUserData,
            ),
            ownedPartitions = emptyList(),
            generationId = generationId,
            rackId = null,
        )
        val (partitions, generation) = memberData(subscription)
        // in StickyAssignor with eager rebalance protocol, we'll always honor data in user data
        assertEquals(
            expected = ownedPartitions,
            actual = partitions,
            message = "subscription: $subscription doesn't have expected owned partition",
        )
        assertEquals(
            expected = generationIdInUserData,
            actual = generation ?: -1,
            message = "subscription: $subscription doesn't have expected generation id",
        )
    }

    @Test
    fun testAssignorWithOldVersionSubscriptions() {
        val partitionsPerTopic = mapOf(topic1 to 3)
        val subscribedTopics = listOf(topic1)
        subscriptions[consumer1] = buildSubscriptionV0(
            topics = subscribedTopics,
            partitions = listOf(TopicPartition(topic = topic1, partition = 0)),
            generationId = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV1(
            topics = subscribedTopics,
            partitions = listOf(TopicPartition(topic = topic1, partition = 1)),
            generationId = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generationId = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = mutableListOf(TopicPartition(topic =topic1, partition = 0)),
            actual = assignment[consumer1],
        )
        assertEquals(
            expected = mutableListOf(TopicPartition(topic =topic1, partition = 1)),
            actual = assignment[consumer2],
        )
        assertEquals(
            expected = mutableListOf(TopicPartition(topic =topic1, partition = 2)),
            actual = assignment[consumer3],
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    companion object {
        private fun buildSubscriptionWithOldSchema(
            topics: List<String>,
            partitions: List<TopicPartition>,
        ): Subscription {
            val struct = Struct(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0)
            val topicAssignments = mutableListOf<Struct>()
            for ((key, value) in groupPartitionsByTopic(partitions)) {
                val topicAssignment = Struct(StickyAssignor.TOPIC_ASSIGNMENT)
                topicAssignment[StickyAssignor.TOPIC_KEY_NAME] = key
                topicAssignment[StickyAssignor.PARTITIONS_KEY_NAME] = value.toTypedArray()
                topicAssignments.add(topicAssignment)
            }
            struct[StickyAssignor.TOPIC_PARTITIONS_KEY_NAME] = topicAssignments.toTypedArray()
            val buffer = ByteBuffer.allocate(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct))
            StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct)
            buffer.flip()
            return Subscription(topics, buffer)
        }
    }
}
