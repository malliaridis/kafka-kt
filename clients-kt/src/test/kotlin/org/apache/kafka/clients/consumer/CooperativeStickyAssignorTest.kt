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

import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.ALL_RACKS
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.RackConfig
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
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
        consumerIndex: Int,
    ): ConsumerPartitionAssignor.Subscription? {
        // cooperative sticky assignor only supports ConsumerProtocolSubscription V1 or above
        return null
    }

    override fun buildSubscriptionV1(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
        consumerIndex: Int,
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
            userData = assignor.subscriptionUserData(topics.toSet()),
            ownedPartitions = partitions,
            generationId = AbstractStickyAssignor.DEFAULT_GENERATION,
            rackId = null,
        )
    }

    override fun buildSubscriptionV2Above(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
        consumerIndex: Int,
    ): ConsumerPartitionAssignor.Subscription {
        return ConsumerPartitionAssignor.Subscription(
            topics = topics,
            userData = assignor.subscriptionUserData(topics.toHashSet()),
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
            listOf(topic),
            assignor.subscriptionUserData(listOf(topic).toSet())
        )
        var encodedGeneration = assignor.memberData(subscription).generation
        assertNotNull(encodedGeneration)
        assertEquals(AbstractStickyAssignor.DEFAULT_GENERATION, encodedGeneration)
        val generation = 10
        assignor.onAssignment(
            assignment = null,
            metadata = ConsumerGroupMetadata(
                groupId = "dummy-group-id",
                generationId = generation,
                memberId = "dummy-member-id",
                groupInstanceId = null,
            ),
        )
        subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf(topic),
            userData = assignor.subscriptionUserData(listOf(topic).toSet()),
        )
        encodedGeneration = assignor.memberData(subscription).generation
        assertNotNull(encodedGeneration)
        assertEquals(generation, encodedGeneration)
    }

    @Test
    fun testDecodeGeneration() {
        val subscription = ConsumerPartitionAssignor.Subscription(listOf(topic))
        assertNull(assignor.memberData(subscription).generation)
    }

    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithEqualPartitionsPerConsumer(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic to partitionInfos(topic, 3),
        )
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1)
            ),
            generationId = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2)
            ),
            generationId = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generationId = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(listOf(TopicPartition(topic = topic, partition = 1)), assignment[consumer1]!!)
        assertEquals(listOf(TopicPartition(topic = topic, partition = 2)), assignment[consumer2]!!)
        // In the cooperative assignor, topic-0 has to be considered "owned" and so it cant be assigned until
        // both have "revoked" it
        assertTrue(assignment[consumer3]!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithUnequalPartitionsPerConsumer(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 4))
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1)
            ),
            generationId = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2)
            ),
            generationId = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generationId = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic, partition = 3),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(listOf(TopicPartition(topic = topic, partition = 2)), assignment[consumer2]!!)
        // In the cooperative assignor, topic-0 has to be considered "owned" and so it cant be assigned until
        // both have "revoked" it
        assertTrue(assignment[consumer3]!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testMemberDataWithInconsistentData() {
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
        val userDataWithHigherGenerationId = assignor.subscriptionUserData(listOf(topic).toSet())
        // The owned partitions and generation id are provided in user data and different owned partition
        // is provided in subscription without generation id.
        // If subscription provides no generation id, we'll honor the generation id in userData and
        // owned partitions in subscription
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf(topic),
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
        val topics = listOf(topic)
        val ownedPartitions =
            listOf(TopicPartition(topic = topic1, partition = 0), TopicPartition(topic = topic2, partition = 1))

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
        val topics = listOf(topic)
        val ownedPartitions = listOf(
            TopicPartition(topic = topic1, partition = 0),
            TopicPartition(topic = topic2, partition = 1),
        )

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
        val partitionsPerTopic = mutableMapOf(topic1 to partitionInfos(topic1, 3))
        val subscribedTopics = listOf(topic1)

        // cooperative sticky assignor only supports ConsumerProtocolSubscription V1 or above
        subscriptions[consumer1] = buildSubscriptionV1(
            topics = subscribedTopics,
            partitions = listOf(TopicPartition(topic = topic1, partition = 0)),
            generationId = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV1(
            topics = subscribedTopics,
            partitions = listOf(TopicPartition(topic = topic1, partition = 1)),
            generationId = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generationId = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(listOf(TopicPartition(topic = topic1, partition = 0)), assignment[consumer1]!!)
        assertEquals(listOf(TopicPartition(topic = topic1, partition = 1)), assignment[consumer2]!!)
        assertEquals(listOf(TopicPartition(topic = topic1, partition = 2)), assignment[consumer3]!!)
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
    override fun verifyValidityAndBalance(
        subscriptions: MutableMap<String, ConsumerPartitionAssignor.Subscription>,
        assignments: MutableMap<String, MutableList<TopicPartition>>,
        partitionsPerTopic: MutableMap<String, MutableList<PartitionInfo>>,
    ) {
        var rebalances = 0
        // partitions are being revoked, we must go through another assignment to get the final state
        while (verifyCooperativeValidity(subscriptions, assignments)) {

            // update the subscriptions with the now owned partitions
            for ((consumer, value) in assignments) {
                val oldSubscription = assertNotNull(subscriptions[consumer])
                val rackIndex =
                    if (oldSubscription.rackId != null) ALL_RACKS.indexOf(oldSubscription.rackId)
                    else -1
                subscriptions[consumer] = buildSubscriptionV2Above(
                    topics = oldSubscription.topics,
                    partitions = value,
                    generationId = generationId,
                    consumerIndex = rackIndex,
                )
            }
            assignments.clear()
            assignments.putAll(assignor.assignPartitions(partitionsPerTopic, subscriptions))
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
            val revokedPartitions = ownedPartitions.toSet() - assignedPartitions.toSet()
            val addedPartitions = assignedPartitions.toSet() - ownedPartitions.toSet()
            allAddedPartitions.addAll(addedPartitions)
            allRevokedPartitions.addAll(revokedPartitions)
        }
        val intersection = allAddedPartitions.intersect(allRevokedPartitions)
        assertTrue(
            intersection.isEmpty(),
            "Error: Some partitions were assigned to a new consumer during the same rebalance they are " +
                    "being revoked from their previous owner. Partitions: $intersection"
        )
        return allRevokedPartitions.isNotEmpty()
    }
}
