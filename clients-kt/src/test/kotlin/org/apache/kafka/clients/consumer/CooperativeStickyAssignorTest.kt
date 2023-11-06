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

import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CooperativeStickyAssignorTest : AbstractStickyAssignorTest() {
    
    override fun createAssignor(): AbstractStickyAssignor = CooperativeStickyAssignor()

    override fun buildSubscriptionV0(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): ConsumerPartitionAssignor.Subscription {
        error("cooperative sticky assignor only supports ConsumerProtocolSubscription V1 or above")
    }

    override fun buildSubscriptionV1(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): ConsumerPartitionAssignor.Subscription {
        assignor.onAssignment(
            assignment = ConsumerPartitionAssignor.Assignment(partitions),
            metadata = ConsumerGroupMetadata(
                groupId = groupId,
                generationId = generationId,
                memberId = consumer1,
                groupInstanceId = null,
            )
        )
        return ConsumerPartitionAssignor.Subscription(
            topics = topics,
            userData = assignor.subscriptionUserData(HashSet(topics)),
            ownedPartitions = partitions,
            generationId = AbstractStickyAssignor.DEFAULT_GENERATION,
            rackId = null,
        )
    }

    override fun buildSubscriptionV2Above(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): ConsumerPartitionAssignor.Subscription {
        return ConsumerPartitionAssignor.Subscription(
            topics = topics,
            userData = assignor.subscriptionUserData(HashSet(topics)),
            ownedPartitions = partitions,
            generationId = generationId,
            rackId = null,
        )
    }

    override fun generateUserData(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generation: Int,
    ): ByteBuffer? {
        assignor.onAssignment(
            assignment = ConsumerPartitionAssignor.Assignment(partitions),
            metadata = ConsumerGroupMetadata(
                groupId = groupId,
                generationId = generationId,
                memberId = consumer1,
                groupInstanceId = null,
            ),
        )
        return assignor.subscriptionUserData(topics.toSet())
    }

    @Test
    fun testEncodeAndDecodeGeneration() {
        var subscription = ConsumerPartitionAssignor.Subscription(
            topics(topic),
            assignor.subscriptionUserData(topics(topic).toSet())
        )
        var encodedGeneration = assignor.memberData(subscription).generation
        assertNotNull(encodedGeneration)
        assertEquals(AbstractStickyAssignor.DEFAULT_GENERATION, encodedGeneration)
        val generation = 10
        assignor.onAssignment(
            assignment = ConsumerPartitionAssignor.Assignment(emptyList()),
            metadata = ConsumerGroupMetadata(
                groupId = "dummy-group-id",
                generationId = generation,
                memberId = "dummy-member-id",
                groupInstanceId = null,
            ),
        )
        subscription = ConsumerPartitionAssignor.Subscription(
            topics = topics(topic),
            userData = assignor.subscriptionUserData(topics(topic).toSet()),
        )
        encodedGeneration = assignor.memberData(subscription).generation
        assertNotNull(encodedGeneration)
        assertEquals(generation, encodedGeneration)
    }

    @Test
    fun testDecodeGeneration() {
        val subscription = ConsumerPartitionAssignor.Subscription(topics(topic))
        assertNull(assignor.memberData(subscription).generation)
    }

    @Test
    fun testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithEqualPartitionsPerConsumer() {
        val partitionsPerTopic = mapOf(topic to 3)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 1)),
            generationId = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 2)),
            generationId = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = emptyList(),
            generationId = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(listOf(tp(topic, 1)), assignment[consumer1]!!)
        assertEquals(listOf(tp(topic, 2)), assignment[consumer2]!!)
        // In the cooperative assignor, topic-0 has to be considered "owned" and so it cant be assigned until
        // both have "revoked" it
        assertTrue(assignment[consumer3]!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithUnequalPartitionsPerConsumer() {
        val partitionsPerTopic: MutableMap<String, Int> = HashMap()
        partitionsPerTopic[topic] = 4
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 1)),
            generationId = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 2)),
            generationId = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = emptyList(),
            generationId = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(listOf(tp(topic, 1), tp(topic, 3)), assignment[consumer1]!!)
        assertEquals(listOf(tp(topic, 2)), assignment[consumer2]!!)
        // In the cooperative assignor, topic-0 has to be considered "owned" and so it cant be assigned until
        // both have "revoked" it
        assertTrue(assignment[consumer3]!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testMemberDataWithInconsistentData() {
        val partitionsPerTopic: MutableMap<String, Int> = HashMap()
        partitionsPerTopic[topic] = 2
        val ownedPartitionsInUserdata = listOf(tp1)
        val ownedPartitionsInSubscription = listOf(tp0)
        assignor.onAssignment(
            assignment = ConsumerPartitionAssignor.Assignment(ownedPartitionsInUserdata),
            metadata = ConsumerGroupMetadata(
                groupId = groupId,
                generationId = generationId,
                memberId = consumer1,
                groupInstanceId = null,
            )
        )
        val userDataWithHigherGenerationId = assignor.subscriptionUserData(topics(topic).toSet())
        // The owned partitions and generation id are provided in user data and different owned partition
        // is provided in subscription without generation id.
        // If subscription provides no generation id, we'll honor the generation id in userData and
        // owned partitions in subscription
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = topics(topic),
            userData = userDataWithHigherGenerationId,
            ownedPartitions = ownedPartitionsInSubscription,
        )
        val (partitions, generation) = memberData(subscription)
        // In CooperativeStickyAssignor, we only serialize generation id into userData
        assertEquals(
            expected = ownedPartitionsInSubscription,
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
    fun testMemberDataWithEmptyPartitionsAndEqualGeneration() {
        val topics = topics(topic)
        val ownedPartitions = listOf(tp(topic1, 0), tp(topic2, 1))

        // subscription containing empty owned partitions and the same generation id, and non-empty owned partition
        // in user data, member data should honor the one in subscription since cooperativeStickyAssignor
        // only supports ConsumerProtocolSubscription v1 and above
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = topics,
            userData = generateUserData(
                topics = topics,
                partitions = ownedPartitions,
                generation = generationId,
            ),
            ownedPartitions = emptyList(),
            generationId = generationId,
            rackId = null
        )
        val (partitions, generation) = memberData(subscription)
        assertEquals(
            expected = emptyList(),
            actual = partitions,
            message = "subscription: $subscription doesn't have expected owned partition"
        )
        assertEquals(
            expected = generationId,
            actual = generation ?: -1,
            message = "subscription: $subscription doesn't have expected generation id",
        )
    }

    @Test
    fun testMemberDataWithEmptyPartitionsAndHigherGeneration() {
        val topics = topics(topic)
        val ownedPartitions = listOf(tp(topic1, 0), tp(topic2, 1))

        // subscription containing empty owned partitions and a higher generation id, and non-empty owned partition
        // in user data, member data should honor the one in subscription since generation id is higher
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = topics,
            userData = generateUserData(
                topics = topics,
                partitions = ownedPartitions,
                generation = generationId - 1,
            ),
            ownedPartitions = emptyList(),
            generationId = generationId,
            rackId = null,
        )
        val (partitions, generation) = memberData(subscription)
        assertEquals(
            expected = emptyList(),
            actual = partitions,
            message = "subscription: $subscription doesn't have expected owned partition"
        )
        assertEquals(
            expected = generationId,
            actual = generation ?: -1,
            message = "subscription: $subscription doesn't have expected generation id",
        )
    }

    @Test
    fun testAssignorWithOldVersionSubscriptions() {
        val partitionsPerTopic: MutableMap<String, Int> = HashMap()
        partitionsPerTopic[topic1] = 3
        val subscribedTopics = topics(topic1)

        // cooperative sticky assignor only supports ConsumerProtocolSubscription V1 or above
        subscriptions[consumer1] = buildSubscriptionV1(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 0)),
            generationId = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV1(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 1)),
            generationId = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generationId = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(listOf(tp(topic1, 0)), assignment[consumer1]!!)
        assertEquals(listOf(tp(topic1, 1)), assignment[consumer2]!!)
        assertEquals(listOf(tp(topic1, 2)), assignment[consumer3]!!)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * The cooperative assignor must do some additional work and verification of some assignments relative to the eager
     * assignor, since it may or may not need to trigger a second follow-up rebalance.
     *
     * In addition to the validity requirements described in
     * [org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest.verifyValidityAndBalance],
     * we must verify that no partition is being revoked and reassigned during the same rebalance. This means the
     * initial assignment may be unbalanced, so if we do detect partitions being revoked we should trigger a second
     * "rebalance" to get the final assignment and then verify that it is both valid and balanced.
     */
    fun verifyValidityAndBalance(
        subscriptions: MutableMap<String, ConsumerPartitionAssignor.Subscription>,
        assignments: MutableMap<String, List<TopicPartition>>,
        partitionsPerTopic: Map<String, Int>,
    ) {
        var rebalances = 0
        // partitions are being revoked, we must go through another assignment to get the final state
        while (verifyCooperativeValidity(subscriptions, assignments)) {

            // update the subscriptions with the now owned partitions
            for ((consumer, value) in assignments) {
                val oldSubscription = assertNotNull(subscriptions[consumer])
                subscriptions[consumer] = buildSubscriptionV2Above(
                    topics = oldSubscription.topics,
                    partitions = value,
                    generationId = generationId,
                )
            }
            assignments.clear()
            assignments.putAll(assignor.assign(partitionsPerTopic, subscriptions))
            ++rebalances
            assertTrue(rebalances <= 4)
        }

        // Check the validity and balance of the final assignment
        super.verifyValidityAndBalance(
            subscriptions = subscriptions,
            assignments = assignments,
            partitionsPerTopic = partitionsPerTopic,
        )
    }

    // Returns true if partitions are being revoked, indicating a second rebalance will be triggered
    private fun verifyCooperativeValidity(
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
        assignments: Map<String, List<TopicPartition>>,
    ): Boolean {
        val allAddedPartitions = mutableSetOf<TopicPartition>()
        val allRevokedPartitions = mutableSetOf<TopicPartition>()
        for ((key, assignedPartitions) in assignments) {
            val ownedPartitions = assertNotNull(subscriptions[key]).ownedPartitions
            val revokedPartitions = ownedPartitions - assignedPartitions.toSet()
            val addedPartitions = assignedPartitions.toSet() - ownedPartitions.toSet()
            allAddedPartitions.addAll(addedPartitions)
            allRevokedPartitions.addAll(revokedPartitions)
        }
        val intersection = allAddedPartitions.toMutableSet()
        intersection.retainAll(allRevokedPartitions)
        assertTrue(
            intersection.isEmpty(),
            "Error: Some partitions were assigned to a new consumer during the same rebalance they are " +
                    "being revoked from their previous owner. Partitions: $intersection"
        )
        return allRevokedPartitions.isNotEmpty()
    }
}
