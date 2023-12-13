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

import java.nio.ByteBuffer
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.StickyAssignor
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.ALL_RACKS
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.TEST_NAME_WITH_RACK_CONFIG
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.nullRacks
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.preferRackAwareLogic
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.racks
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.verifyRackAssignment
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.RackConfig
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.CollectionUtils.groupPartitionsByTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

abstract class AbstractStickyAssignorTest {

    protected lateinit var assignor: AbstractStickyAssignor

    protected var consumerId = "consumer"

    protected var consumer1 = "consumer1"

    protected var consumer2 = "consumer2"

    protected var consumer3 = "consumer3"

    protected var consumer4 = "consumer4"

    protected lateinit var subscriptions: MutableMap<String, Subscription>

    protected var topic = "topic"

    protected var topic1 = "topic1"

    protected var topic2 = "topic2"

    protected var topic3 = "topic3"

    protected var tp0 = TopicPartition(topic = topic, partition = 0)

    protected var tp1 = TopicPartition(topic = topic, partition = 1)

    protected var tp2 = TopicPartition(topic = topic, partition = 2)

    protected var groupId = "group"

    protected var generationId = 1

    protected var numBrokerRacks = 0

    protected var hasConsumerRack = false

    protected var replicationFactor = 2

    private var nextPartitionIndex = 0

    protected abstract fun createAssignor(): AbstractStickyAssignor

    // simulate ConsumerProtocolSubscription V0 protocol
    protected abstract fun buildSubscriptionV0(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
        consumerIndex: Int,
    ): Subscription?

    // simulate ConsumerProtocolSubscription V1 protocol
    protected abstract fun buildSubscriptionV1(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
        consumerIndex: Int,
    ): Subscription

    // simulate ConsumerProtocolSubscription V2 or above protocol
    protected abstract fun buildSubscriptionV2Above(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generation: Int,
        consumerIndex: Int,
    ): Subscription

    protected abstract fun generateUserData(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generation: Int,
    ): ByteBuffer?

    @BeforeEach
    fun setUp() {
        assignor = createAssignor()
        if (::subscriptions.isInitialized) subscriptions.clear()
        else subscriptions = HashMap()
    }

    @Test
    fun testMemberData() {
        val topics = listOf(topic)
        val ownedPartitions = listOf(
            TopicPartition(topic = topic1, partition = 0),
            TopicPartition(topic = topic2, partition = 1),
        )
        val subscriptions = mutableListOf<Subscription?>()
        // add subscription in all ConsumerProtocolSubscription versions
        subscriptions.add(buildSubscriptionV0(topics, ownedPartitions, generationId, 0))
        subscriptions.add(buildSubscriptionV1(topics, ownedPartitions, generationId, 1))
        subscriptions.add(buildSubscriptionV2Above(topics, ownedPartitions, generationId, 2))
        for (subscription in subscriptions) {
            // Kotlin Migration: skip currently only v0 cooperative sticky assignor subscription that
            // is not supported
            if (subscription == null) return
            val memberData = assignor.memberData(subscription)
            assertEquals(
                expected = ownedPartitions,
                actual = memberData.partitions,
                message = "subscription: $subscription doesn't have expected owned partition",
            )
            assertEquals(
                expected = generationId,
                actual = memberData.generation ?: -1,
                message = "subscription: $subscription doesn't have expected generation id",
            )
        }
    }

    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [false, true])
    fun testOneConsumerNoTopic() {
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        subscriptions = mutableMapOf(consumerId to subscription(emptyList(), 0))
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(assignment[consumerId]!!.isEmpty())
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [false, true])
    fun testOneConsumerNonexistentTopic(hasConsumerRack: Boolean) {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        val partitionsPerTopic = mutableMapOf(topic to mutableListOf<PartitionInfo>())
        subscriptions = mutableMapOf(consumerId to subscription(listOf(topic), 0))
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(assignment[consumerId]!!.isEmpty())
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOneConsumerOneTopic(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 3))
        subscriptions = mutableMapOf(consumerId to subscription(listOf(topic), 0))

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic, partition = 2),
            ),
            actual = assignment[consumerId]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOnlyAssignsPartitionsFromSubscribedTopics(rackConfig: RackConfig) {
        val otherTopic = "other"

        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 2))
        subscriptions = mutableMapOf(
            consumerId to buildSubscriptionV2Above(
                topics = listOf(topic),
                partitions = listOf(
                    TopicPartition(topic = topic, partition = 0),
                    TopicPartition(topic = topic, partition = 1),
                    TopicPartition(topic = otherTopic, partition = 0),
                    TopicPartition(topic = otherTopic, partition = 1),
                ),
                generation = generationId,
                consumerIndex = 0,
            ),
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
            ),
            actual = assignment[consumerId]!!
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOneConsumerMultipleTopics(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 1),
            topic2 to partitionInfos(topic2, 2),
        )
        subscriptions = mutableMapOf(consumerId to subscription(listOf(topic1, topic2), 0))

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumerId]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoConsumersOneTopicOnePartition(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 1))
        subscriptions[consumer1] = subscription(listOf(topic), 0)
        subscriptions[consumer2] = subscription(listOf(topic), 1)

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoConsumersOneTopicTwoPartitions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 2))
        subscriptions[consumer1] = subscription(listOf(topic), 0)
        subscriptions[consumer2] = subscription(listOf(topic), 1)

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(listOf(TopicPartition(topic = topic, partition = 0)), assignment[consumer1]!!)
        assertEquals(listOf(TopicPartition(topic = topic, partition = 1)), assignment[consumer2]!!)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testMultipleConsumersMixedTopicSubscriptions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 3),
            topic2 to partitionInfos(topic2, 2),
        )
        subscriptions[consumer1] = subscription(listOf(topic1), 0)
        subscriptions[consumer2] = subscription(listOf(topic1, topic2), 1)
        subscriptions[consumer3] = subscription(listOf(topic1), 2)

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 2),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer2]!!,
        )
        assertEquals(listOf(TopicPartition(topic = topic1, partition = 1)), assignment[consumer3]!!)
        assertNull(assignor.partitionsTransferringOwnership)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoConsumersTwoTopicsSixPartitions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 3),
            topic2 to partitionInfos(topic2, 3),
        )
        subscriptions[consumer1] = subscription(listOf(topic1, topic2), 0)
        subscriptions[consumer2] = subscription(listOf(topic1, topic2), 1)

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test is testing consumer owned minQuota partitions, and expected to have maxQuota partitions situation
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testConsumerOwningMinQuotaExpectedMaxQuota(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 2),
            topic2 to partitionInfos(topic2, 3),
        )
        val subscribedTopics = listOf(topic1, topic2)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 2),
            ),
            generation = generationId,
            consumerIndex = 1,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test is testing consumers owned maxQuota partitions are more than numExpectedMaxCapacityMembers situation
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testMaxQuotaConsumerMoreThanNumExpectedMaxCapacityMembers(rackConfig: RackConfig) {
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 2),
            topic2 to partitionInfos(topic2, 2),
        )
        val subscribedTopics = listOf(topic1, topic2)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 0),
            ),
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 1),
            ),
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = mapOf(TopicPartition(topic = topic2, partition = 0) to consumer3),
            actual = assignor.partitionsTransferringOwnership!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic2, partition = 0)),
            actual = assignment[consumer3]!!,
        )
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test is testing all consumers owned less than minQuota partitions situation
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAllConsumersAreUnderMinQuota() {
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 2),
            topic2 to partitionInfos(topic2, 3),
        )
        val subscribedTopics = listOf(topic1, topic2)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(TopicPartition(topic = topic1, partition = 0)),
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(TopicPartition(topic = topic1, partition = 1)),
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic2, partition = 0)),
            actual = assignment[consumer3]!!,
        )
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAddRemoveConsumerOneTopic(rackConfig: RackConfig) {
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 3))
        subscriptions[consumer1] = subscription(listOf(topic), 0)
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic, partition = 2),
            ),
            actual = assignment[consumer1]!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = assignment[consumer1]!!,
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 1,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = mapOf(TopicPartition(topic = topic, partition = 2) to consumer2),
            actual = assignor.partitionsTransferringOwnership!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 2)),
            actual = assignment[consumer2]!!,
        )
        assertTrue(isFullyBalanced(assignment))

        subscriptions.remove(consumer1)
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = assignment[consumer2]!!,
            generation = generationId,
            consumerIndex = 1,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic, partition = 0),
            ),
            actual = assignment[consumer2]!!.toHashSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAddRemoveTwoConsumersTwoTopics(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val allTopics = listOf(topic1, topic2)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 3),
            topic2 to partitionInfos(topic2, 4),
        )
        subscriptions[consumer1] = subscription(allTopics, 0)
        subscriptions[consumer2] = subscription(allTopics, 1)
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
                TopicPartition(topic = topic2, partition = 3),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))

        // add 2 consumers
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = assignment[consumer1]!!,
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = assignment[consumer2]!!,
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 2,
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 3,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        val expectedPartitionsTransferringOwnership = mutableMapOf(
            TopicPartition(topic = topic2, partition = 1) to consumer3,
            TopicPartition(topic = topic2, partition = 3) to consumer3,
            TopicPartition(topic = topic2, partition = 2) to consumer4,
        )

        assertEquals(expectedPartitionsTransferringOwnership, assignor.partitionsTransferringOwnership)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 2),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
            ),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            listOf(
                TopicPartition(topic = topic2, partition = 1),
                TopicPartition(topic = topic2, partition = 3),
            ),
            assignment[consumer3]!!,
        )
        assertEquals(
            listOf(TopicPartition(topic = topic2, partition = 2)),
            assignment[consumer4]!!
        )
        assertTrue(isFullyBalanced(assignment))

        // remove 2 consumers
        subscriptions.remove(consumer1)
        subscriptions.remove(consumer2)
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = assignment[consumer3]!!,
            generation = generationId,
            consumerIndex = 2,
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = assignment[consumer4]!!,
            generation = generationId,
            consumerIndex = 3,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic2, partition = 1),
                TopicPartition(topic = topic2, partition = 3),
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 0),
            ),
            actual = assignment[consumer3]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic2, partition = 2),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic1, partition = 2),
            ),
            actual = assignment[consumer4]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test performs sticky assignment for a scenario that round robin assignor handles poorly.
     *
     * Topics (partitions per topic): topic1 (2), topic2 (1), topic3 (2), topic4 (1), topic5 (2)
     *
     * Subscriptions:
     * - consumer1: topic1, topic2, topic3, topic4, topic5
     * - consumer2: topic1, topic3, topic5
     * - consumer3: topic1, topic3, topic5
     * - consumer4: topic1, topic2, topic3, topic4, topic5
     *
     * Round Robin Assignment Result:
     * - consumer1: topic1-0, topic3-0, topic5-0
     * - consumer2: topic1-1, topic3-1, topic5-1
     * - consumer3:
     * - consumer4: topic2-0, topic4-0
     *
     * Sticky Assignment Result:
     * - consumer1: topic2-0, topic3-0
     * - consumer2: topic1-0, topic3-1
     * - consumer3: topic1-1, topic5-0
     * - consumer4: topic4-0, topic5-1
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testPoorRoundRobinAssignmentScenario(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 1..5) {
            val topicName = "topic$i"
            partitionsPerTopic[topicName] = partitionInfos(topicName, (i % 2) + 1)
        }
        subscriptions["consumer1"] = subscription(listOf("topic1", "topic2", "topic3", "topic4", "topic5"), 0)
        subscriptions["consumer2"] = subscription(listOf("topic1", "topic3", "topic5"), 1)
        subscriptions["consumer3"] = subscription(listOf("topic1", "topic3", "topic5"), 2)
        subscriptions["consumer4"] = subscription(listOf("topic1", "topic2", "topic3", "topic4", "topic5"), 3)
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAddRemoveTopicTwoConsumers() {
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 3))
        subscriptions[consumer1] = subscription(listOf(topic), 0)
        subscriptions[consumer2] = subscription(listOf(topic), 1)

        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        // verify balance
        assertTrue(isFullyBalanced(assignment))
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        // verify stickiness
        val consumer1Assignment1 = assignment[consumer1]!!
        val consumer2Assignment1 = assignment[consumer2]!!

        assertTrue(
            consumer1Assignment1.size == 1
                    && consumer2Assignment1.size == 2
                    || consumer1Assignment1.size == 2
                    && consumer2Assignment1.size == 1
        )

        partitionsPerTopic[topic2] = partitionInfos(topic2, 3)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = assignment[consumer1]!!,
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = assignment[consumer2]!!,
            generation = generationId,
            consumerIndex = 1,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())

        // verify balance
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))

        // verify stickiness
        val consumer1assignment = assignment[consumer1]!!
        val consumer2assignment = assignment[consumer2]!!
        assertTrue(consumer1assignment.size == 3 && consumer2assignment.size == 3)
        assertTrue(consumer1assignment.containsAll(consumer1Assignment1))
        assertTrue(consumer2assignment.containsAll(consumer2Assignment1))

        partitionsPerTopic.remove(topic)

        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic2),
            partitions = assignment[consumer1]!!,
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic2),
            partitions = assignment[consumer2]!!,
            generation = generationId,
            consumerIndex = 1,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())

        // verify balance
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))

        // verify stickiness
        val consumer1Assignment3 = assignment[consumer1]!!
        val consumer2Assignment3 = assignment[consumer2]!!
        assertTrue(
            consumer1Assignment3.size == 1 && consumer2Assignment3.size == 2
                    || consumer1Assignment3.size == 2 && consumer2Assignment3.size == 1
        )
        assertTrue(consumer1assignment.containsAll(consumer1Assignment3))
        assertTrue(consumer2assignment.containsAll(consumer2Assignment3))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testReassignmentAfterOneConsumerLeaves(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 1..19) {
            val topicName = getTopicName(i, 20)
            partitionsPerTopic[topicName] = partitionInfos(topicName, i)
        }
        for (i in 1..19) {
            val topics = mutableListOf<String>()
            for (j in 1..i) topics.add(getTopicName(j, 20))
            subscriptions[getConsumerName(i, 20)] = subscription(topics, i)
        }
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        for (i in 1..19) {
            val consumer = getConsumerName(i, 20)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = subscriptions[consumer]!!.topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
        }
        subscriptions.remove("consumer10")
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(assignor.isSticky)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testReassignmentAfterOneConsumerAdded(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf("topic" to partitionInfos("topic", 20))
        for (i in 1..9)
            subscriptions[getConsumerName(i, 10)] = subscription(listOf("topic"), i)

        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        // add a new consumer
        subscriptions[getConsumerName(10, 10)] = subscription(listOf("topic"), 10)
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testSameSubscriptions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 1..14) {
            val topicName = getTopicName(i, 15)
            partitionsPerTopic[topicName] = partitionInfos(topicName, i)
        }
        for (i in 1..8) {
            val topics = mutableListOf<String>()
            for (j in 1..partitionsPerTopic.size) topics.add(getTopicName(j, 15))
            subscriptions[getConsumerName(i, 9)] = subscription(topics, i)
        }
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        for (i in 1..8) {
            val consumer = getConsumerName(i, 9)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = subscriptions[consumer]!!.topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
        }
        subscriptions.remove(getConsumerName(5, 9))
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @Timeout(30)
    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [false, true])
    fun testLargeAssignmentAndGroupWithUniformSubscription(hasConsumerRack: Boolean) {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        // 1 million partitions for non-rack-aware! For rack-aware, use smaller number of partitions to reduce test run time.
        val topicCount = if (hasConsumerRack) 50 else 500
        val partitionCount = 2000
        val consumerCount = 2000
        val topics = mutableListOf<String>()
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()

        repeat(topicCount) { i ->
            val topicName = getTopicName(i, topicCount)
            topics.add(topicName)
            partitionsPerTopic[topicName] = partitionInfos(topicName, partitionCount)
        }
        repeat(consumerCount) { i ->
            subscriptions[getConsumerName(i, consumerCount)] = subscription(topics, i)
        }

        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        for (i in 1..<consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
        }
        assignor.assignPartitions(partitionsPerTopic, subscriptions)
    }

    @Timeout(90)
    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [false, true])
    fun testLargeAssignmentAndGroupWithNonEqualSubscription() {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        // 1 million partitions for non-rack-aware! For rack-aware, use smaller number of partitions to reduce test run time.
        val topicCount = if (hasConsumerRack) 50 else 500
        val partitionCount = 2000
        val consumerCount = 2000
        val topics = mutableListOf<String>()
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        repeat(topicCount) { i ->
            val topicName = getTopicName(i, topicCount)
            topics.add(topicName)
            partitionsPerTopic[topicName] = partitionInfos(topicName, partitionCount)
        }
        repeat(consumerCount) { i ->
            if (i == consumerCount - 1)
                subscriptions[getConsumerName(i, consumerCount)] = subscription(topics.subList(0, 1), i)
            else subscriptions[getConsumerName(i, consumerCount)] = subscription(topics, i)
        }
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        for (i in 1..<consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            if (i == consumerCount - 1) subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics.subList(0, 1),
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
            else subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
        }
        assignor.assignPartitions(partitionsPerTopic, subscriptions)
    }

    @Timeout(90)
    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [false, true])
    fun testAssignmentAndGroupWithNonEqualSubscriptionNotTimeout(hasConsumerRack: Boolean) {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        val topicCount = if (hasConsumerRack) 50 else 100
        val partitionCount = 200
        val consumerCount = 500
        val topics = mutableListOf<String>()
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 0..<topicCount) {
            val topicName = getTopicName(i, topicCount)
            topics.add(topicName)
            partitionsPerTopic[topicName] = partitionInfos(topicName, partitionCount)
        }
        for (i in 0..<consumerCount) {
            if (i % 4 == 0) subscriptions[getConsumerName(i, consumerCount)] =
                subscription(topics.subList(0, topicCount / 2), i)
            else subscriptions[getConsumerName(i, consumerCount)] =
                subscription(topics.subList(topicCount / 2, topicCount), i)
        }
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        for (i in 1..<consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            if (i % 4 == 0) subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics.subList(0, topicCount / 2),
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
            else subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics.subList(topicCount / 2, topicCount),
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
        }
        assignor.assignPartitions(partitionsPerTopic, subscriptions)
    }

    @Test
    fun testSubscriptionNotEqualAndAssignSamePartitionWith3Generation() {
        val partitionsPerTopic = mutableMapOf(
            topic to partitionInfos(topic, 6),
            topic1 to partitionInfos(topic1, 1),
        )
        
        val sequence = arrayOf(
            intArrayOf(1, 2, 3),
            intArrayOf(1, 3, 2),
            intArrayOf(2, 1, 3),
            intArrayOf(2, 3, 1),
            intArrayOf(3, 1, 2),
            intArrayOf(3, 2, 1)
        )
        for (ints in sequence) {
            subscriptions[consumer1] = buildSubscriptionV2Above(
                topics = listOf(topic),
                partitions = listOf(
                    TopicPartition(topic, 0),
                    TopicPartition(topic, 2)
                ),
                generation = ints[0],
                consumerIndex = 0,
            )
            subscriptions[consumer2] = buildSubscriptionV2Above(
                topics = listOf(topic),
                partitions = listOf(
                    TopicPartition(topic, 1),
                    TopicPartition(topic, 2),
                    TopicPartition(topic, 3),
                ),
                generation = ints[1],
                consumerIndex = 1,
            )
            subscriptions[consumer3] = buildSubscriptionV2Above(
                topics = listOf(topic),
                partitions = listOf(
                    TopicPartition(topic, 2),
                    TopicPartition(topic, 4),
                    TopicPartition(topic, 5),
                ),
                generation = ints[2],
                consumerIndex = 2,
            )
            subscriptions[consumer4] = buildSubscriptionV2Above(
                topics = listOf(topic1),
                partitions = listOf(TopicPartition(topic1, 0)),
                generation = 2,
                consumerIndex = 3,
            )
            val assign = assignor.assignPartitions(partitionsPerTopic, subscriptions)
            assertEquals(
                expected = assign.values.sumOf { it.size },
                actual = assign.values.flatten().toSet().size,
            )
            for (list in assign.values) {
                assertTrue(list.isNotEmpty())
                assertTrue(list.size <= 2)
            }
        }
    }

    @Timeout(60)
    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [false, true])
    fun testLargeAssignmentWithMultipleConsumersLeavingAndRandomSubscription(hasConsumerRack: Boolean) {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        val topicCount = 40
        val consumerCount = 200
        
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 0..<topicCount) {
            val topicName = getTopicName(i, topicCount)
            partitionsPerTopic[topicName] = partitionInfos(topicName, Random.nextInt(10) + 1)
        }
        
        for (i in 0..<consumerCount) {
            val topics = mutableListOf<String>()
            repeat(Random.nextInt(20)) {
                topics.add(getTopicName(Random.nextInt(topicCount), topicCount))
            }
            subscriptions[getConsumerName(i, consumerCount)] = subscription(topics, i)
        }
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        for (i in 1..<consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = subscriptions[consumer]!!.topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
                consumerIndex = i,
            )
        }
        repeat(50) {
            val c = getConsumerName(Random.nextInt(consumerCount), consumerCount)
            subscriptions.remove(c)
        }
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(assignor.isSticky)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testNewSubscription(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 1..4) {
            val topicName = getTopicName(i, 5)
            partitionsPerTopic[topicName] = partitionInfos(topicName, 1)
        }
        for (i in 0..2) {
            val topics = mutableListOf<String>()
            for (j in i..3 * i - 2) topics.add(getTopicName(j, 5))
            subscriptions[getConsumerName(i, 3)] = subscription(topics, i)
        }
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        val subKey = getConsumerName(0, 3)
        val newTopics = subscriptions[subKey]!!.topics + getTopicName(1, 5)
        subscriptions[subKey] = Subscription(newTopics)
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(assignor.isSticky)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testMoveExistingAssignments(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val topic4 = "topic4"
        val topic5 = "topic5"
        val topic6 = "topic6"
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (i in 1..6) {
            val topicName = String.format("topic%d", i)
            partitionsPerTopic[topicName] = partitionInfos(topicName, 1)
        }

        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic1, topic2),
            partitions = listOf(TopicPartition(topic = topic1, partition = 0)),
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic1, topic2, topic3, topic4),
            partitions = listOf(
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic3, partition = 0),
            ),
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic2, topic3, topic4, topic5, topic6),
            partitions = listOf(
                TopicPartition(topic = topic4, partition = 0),
                TopicPartition(topic = topic5, partition = 0),
                TopicPartition(topic = topic6, partition = 0),
            ),
            generation = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertNull(assignor.partitionsTransferringOwnership)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testStickiness(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        partitionsPerTopic[topic1] = partitionInfos(topic1, 3)
        
        subscriptions[consumer1] = subscription(listOf(topic1), 0)
        subscriptions[consumer2] = subscription(listOf(topic1), 1)
        subscriptions[consumer3] = subscription(listOf(topic1), 2)
        subscriptions[consumer4] = subscription(listOf(topic1), 3)
        
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        val partitionsAssigned = mutableMapOf<String, TopicPartition?>()
        var assignments = assignment.entries
        for ((consumer, topicPartitions) in assignments) {
            val size = topicPartitions.size
            assertTrue(
                actual = size <= 1,
                message = "Consumer $consumer is assigned more topic partitions than expected.",
            )
            if (size == 1) partitionsAssigned[consumer] = topicPartitions[0]
        }

        // removing the potential group leader
        subscriptions.remove(consumer1)
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic1),
            partitions = assignment[consumer2]!!,
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic1),
            partitions = assignment[consumer3]!!,
            generation = generationId,
            consumerIndex = 2,
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = listOf(topic1),
            partitions = assignment[consumer4]!!,
            generation = generationId,
            consumerIndex = 3,
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        assignments = assignment.entries

        for ((consumer, topicPartitions) in assignments) {
            assertEquals(
                expected = 1, actual = topicPartitions.size,
                message = "Consumer $consumer is assigned more topic partitions than expected.",
            )
            assertTrue(
                actual = !partitionsAssigned.containsKey(consumer)
                        || assignment[consumer]!!.contains(partitionsAssigned[consumer]!!),
                message = "Stickiness was not honored for consumer $consumer",
            )
        }
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAssignmentUpdatedForDeletedTopic(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        partitionsPerTopic[topic1] = partitionInfos(topic1, 1)
        partitionsPerTopic[topic3] = partitionInfos(topic3, 100)
        subscriptions = mutableMapOf(
            consumerId to subscription(listOf(topic1, topic2, topic3), 0)
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        assertEquals(
            expected = assignment.values.stream().mapToInt { it.size }.sum(),
            actual = 1 + 100,
        )
        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testNoExceptionThrownWhenOnlySubscribedTopicDeleted(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        partitionsPerTopic[topic] = partitionInfos(topic, 3)
        subscriptions[consumerId] = subscription(listOf(topic), 0)
        var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())

        subscriptions[consumerId] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = assignment[consumerId]!!,
            generation = generationId,
            consumerIndex = 0,
        )
        assignment = assignor.assign(emptyMap(), subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        assertEquals(assignment.size, 1)
        assertTrue(assignment[consumerId]!!.isEmpty())
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testReassignmentWithRandomSubscriptionsAndChanges(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val minNumConsumers = 20
        val maxNumConsumers = 40
        val minNumTopics = 10
        val maxNumTopics = 20
        for (round in 1..100) {
            val numTopics = minNumTopics + Random.nextInt(maxNumTopics - minNumTopics)
            val topics = ArrayList<String>()
            
            val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
            for (i in 0..<numTopics) {
                val topicName = getTopicName(i, maxNumTopics)
                topics.add(topicName)
                partitionsPerTopic[topicName] = partitionInfos(topicName, i + 1)
            }
            
            val numConsumers = minNumConsumers + Random.nextInt(maxNumConsumers - minNumConsumers)
            for (i in 0..<numConsumers) {
                val sub = getRandomSublist(topics).sorted()
                subscriptions[getConsumerName(i, maxNumConsumers)] = subscription(sub, i)
            }
            assignor = createAssignor()
            var assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

            verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

            subscriptions.clear()

            for (i in 0..<numConsumers) {
                val sub = getRandomSublist(topics).sorted()
                val consumer = getConsumerName(i, maxNumConsumers)
                subscriptions[consumer] = buildSubscriptionV2Above(
                    topics = sub,
                    partitions = assignment[consumer]!!,
                    generation = generationId,
                    consumerIndex = i,
                )
            }
            assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

            verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
            assertTrue(assignor.isSticky)
        }
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testAllConsumersReachExpectedQuotaAndAreConsideredFilled(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(topic to partitionInfos(topic, 4))
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
            ),
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(TopicPartition(topic = topic, partition = 2)),
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(listOf(TopicPartition(topic = topic, partition = 2)), assignment[consumer2]!!)
        assertEquals(listOf(TopicPartition(topic = topic, partition = 3)), assignment[consumer3]!!)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOwnedPartitionsAreInvalidatedForConsumerWithStaleGeneration() {
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        partitionsPerTopic[topic] = partitionInfos(topic, 3)
        partitionsPerTopic[topic2] = partitionInfos(topic2, 3)
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            generation = currentGeneration,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            generation = currentGeneration - 1,
            consumerIndex = 1,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = setOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer1]!!.toSet(),
        )
        assertEquals(
            expected = setOf(
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!.toSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOwnedPartitionsAreInvalidatedForConsumerWithNoGeneration(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        partitionsPerTopic[topic] = partitionInfos(topic, 3)
        partitionsPerTopic[topic2] = partitionInfos(topic, 3)
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            generation = currentGeneration,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            generation = AbstractStickyAssignor.DEFAULT_GENERATION,
            consumerIndex = 1,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer1]!!.toHashSet(),
        )
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!.toHashSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testPartitionsTransferringOwnershipIncludeThePartitionClaimedByMultipleConsumersInSameGeneration(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic to partitionInfos(topic, 3)
        )

        // partition topic-0 is owned by multiple consumer
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1)
            ),
            generation = generationId,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 2)
            ),
            generation = generationId,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic),
            partitions = emptyList(),
            generation = generationId,
            consumerIndex = 2,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        // we should include the partitions claimed by multiple consumers in partitionsTransferringOwnership
        assertEquals(
            expected = mapOf(TopicPartition(topic = topic, partition = 0) to consumer3),
            actual = assignor.partitionsTransferringOwnership!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
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
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testEnsurePartitionsAssignedToHighestGeneration(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic to partitionInfos(topic, 3),
            topic2 to partitionInfos(topic2, 3),
            topic3 to partitionInfos(topic3, 3),
        )
        val currentGeneration = 10

        // ensure partitions are always assigned to the member with the highest generation
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3),
            partitions = listOf(
                TopicPartition(topic, 0),
                TopicPartition(topic2, 0),
                TopicPartition(topic3, 0)
            ),
            generation = currentGeneration,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3),
            partitions = listOf(
                TopicPartition(topic, 1),
                TopicPartition(topic2, 1),
                TopicPartition(topic3, 1)
            ),
            generation = currentGeneration - 1,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3),
            partitions = listOf(
                TopicPartition(topic2, 1),
                TopicPartition(topic3, 0),
                TopicPartition(topic3, 2)
            ),
            generation = currentGeneration - 2,
            consumerIndex = 1,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic, 0),
                TopicPartition(topic2, 0),
                TopicPartition(topic3, 0),
            ),
            actual = assignment[consumer1]!!.toHashSet(),
        )
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic, 1),
                TopicPartition(topic2, 1),
                TopicPartition(topic3, 1),
            ),
            actual = assignment[consumer2]!!.toHashSet(),
        )
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic, 2),
                TopicPartition(topic2, 2),
                TopicPartition(topic3, 2),
            ),
            actual = assignment[consumer3]!!.toHashSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testNoReassignmentOnCurrentMembers(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        partitionsPerTopic[topic] = partitionInfos(topic, 3)
        partitionsPerTopic[topic1] = partitionInfos(topic1, 3)
        partitionsPerTopic[topic2] = partitionInfos(topic2, 3)
        partitionsPerTopic[topic3] = partitionInfos(topic3, 3)
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3, topic1),
            partitions = emptyList(),
            generation = AbstractStickyAssignor.DEFAULT_GENERATION,
            consumerIndex = 0
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3, topic1),
            partitions = listOf(
                TopicPartition(topic, 0),
                TopicPartition(topic2, 0),
                TopicPartition(topic1, 0),
            ),
            generation = currentGeneration - 1,
            consumerIndex = 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3, topic1),
            partitions = listOf(
                TopicPartition(topic3, 2),
                TopicPartition(topic2, 2),
                TopicPartition(topic1, 1),
            ),
            generation = currentGeneration - 2,
            consumerIndex = 2,
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2, topic3, topic1),
            partitions = listOf(
                TopicPartition(topic3, 1),
                TopicPartition(topic, 1),
                TopicPartition(topic, 2),
            ),
            generation = currentGeneration - 3,
            consumerIndex = 3,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        // ensure assigned partitions don't get reassigned
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic1, 2),
                TopicPartition(topic2, 1),
                TopicPartition(topic3, 0),
            ),
            actual = assignment[consumer1]!!.toHashSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOwnedPartitionsAreInvalidatedForConsumerWithMultipleGeneration(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic to partitionInfos(topic, 3),
            topic2 to partitionInfos(topic2, 3),
        )
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = listOf(
                TopicPartition(topic, 0),
                TopicPartition(topic2, 1),
                TopicPartition(topic, 1),
            ),
            generation = currentGeneration,
            consumerIndex = 0,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = listOf(topic, topic2),
            partitions = listOf(
                TopicPartition(topic, 0),
                TopicPartition(topic2, 1),
                TopicPartition(topic2, 2),
            ),
            generation = currentGeneration - 2,
            consumerIndex = 1,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic, 0),
                TopicPartition(topic2, 1),
                TopicPartition(topic, 1)
            ),
            actual = assignment[consumer1]!!.toHashSet(),
        )
        assertEquals(
            expected = hashSetOf(
                TopicPartition(topic, 2),
                TopicPartition(topic2, 2),
                TopicPartition(topic2, 0)
            ),
            actual = assignment[consumer2]!!.toHashSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testRackAwareAssignmentWithUniformSubscription() {
        val topics = mapOf("t1" to 6, "t2" to 7, "t3" to 2)
        val allTopics = listOf("t1", "t2", "t3")
        val consumerTopics = listOf(allTopics, allTopics, allTopics)
        val nonRackAwareAssignment = listOf(
            "t1-0, t1-3, t2-0, t2-3, t2-6",
            "t1-1, t1-4, t2-1, t2-4, t3-0",
            "t1-2, t1-5, t2-2, t2-5, t3-1",
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = nullRacks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = nullRacks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        preferRackAwareLogic(assignor, true)
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        preferRackAwareLogic(assignor, true)
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 4,
            brokerRacks = racks(4),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        preferRackAwareLogic(assignor, false)
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = mutableListOf<String?>("d", "e", "f"),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = mutableListOf<String?>(null, "e", "f"),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )

        // Verify assignment is rack-aligned for lower replication factor where brokers have a subset of partitions
        var assignment = listOf(
            "t1-0, t1-3, t2-0, t2-3, t2-6",
            "t1-1, t1-4, t2-1, t2-4, t3-0",
            "t1-2, t1-5, t2-2, t2-5, t3-1",
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 0
        )
        assignment = listOf(
            "t1-0, t1-3, t2-0, t2-3, t2-6",
            "t1-1, t1-4, t2-1, t2-4, t3-0",
            "t1-2, t1-5, t2-2, t2-5, t3-1",
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 0
        )

        // One consumer on a rack with no partitions. We allocate with misaligned rack to this consumer to maintain balance.
        assignment = listOf(
            "t1-0, t1-3, t2-0, t2-3, t2-6",
            "t1-1, t1-4, t2-1, t2-4, t3-0",
            "t1-2, t1-5, t2-2, t2-5, t3-1"
        )
        verifyUniformSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 5
        )

        // Verify that rack-awareness is improved if already owned partitions are misaligned
        assignment = listOf(
            "t1-0, t1-3, t2-0, t2-3, t2-6",
            "t1-1, t1-4, t2-1, t2-4, t3-0",
            "t1-2, t1-5, t2-2, t2-5, t3-1",
        )
        val owned = listOf(
            "t1-0, t1-1, t1-2, t1-3, t1-4",
            "t1-5, t2-0, t2-1, t2-2, t2-3",
            "t2-4, t2-5, t2-6, t3-0, t3-1",
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = owned,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 0,
        )

        // Verify that stickiness is retained when racks match
        preferRackAwareLogic(assignor, true)
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = assignment,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 0,
        )
    }

    private fun verifyUniformSubscription(
        assignor: AbstractStickyAssignor,
        numPartitionsPerTopic: Map<String, Int>,
        replicationFactor: Int,
        brokerRacks: List<String?>,
        consumerRacks: List<String?>,
        consumerTopics: List<List<String>>,
        expectedAssignments: List<String>,
        numPartitionsWithRackMismatch: Int,
    ) {
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = numPartitionsPerTopic,
            replicationFactor = replicationFactor,
            brokerRacks = brokerRacks,
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = null,
            expectedAssignments = expectedAssignments,
            numPartitionsWithRackMismatch = numPartitionsWithRackMismatch,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = numPartitionsPerTopic,
            replicationFactor = replicationFactor,
            brokerRacks = brokerRacks,
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = expectedAssignments,
            expectedAssignments = expectedAssignments,
            numPartitionsWithRackMismatch = numPartitionsWithRackMismatch,
        )
    }

    @Test
    fun testRackAwareAssignmentWithNonEqualSubscription() {
        val topics = mapOf("t1" to 6, "t2" to 7, "t3" to 2)
        val allTopics = listOf("t1", "t2", "t3")
        val consumerTopics = listOf(allTopics, allTopics, listOf("t1", "t3"))
        val nonRackAwareAssignment = listOf(
            "t1-5, t2-0, t2-2, t2-4, t2-6",
            "t1-3, t2-1, t2-3, t2-5, t3-0",
            "t1-0, t1-1, t1-2, t1-4, t3-1",
        )
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = nullRacks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = nullRacks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        preferRackAwareLogic(assignor, true)
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        preferRackAwareLogic(assignor, true)
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 4,
            brokerRacks = racks(4),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        preferRackAwareLogic(assignor, false)
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = mutableListOf<String?>("d", "e", "f"),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = listOf(null, "e", "f"),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )

        // Verify assignment is rack-aligned for lower replication factor where brokers have a subset of partitions
        // Rack-alignment is best-effort, misalignments can occur when number of rack choices is low.
        var assignment = listOf(
            "t1-3, t2-0, t2-2, t2-3, t2-6",
            "t1-4, t2-1, t2-4, t2-5, t3-0",
            "t1-0, t1-1, t1-2, t1-5, t3-1",
        )
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 4,
        )
        assignment = listOf(
            "t1-3, t2-0, t2-2, t2-5, t2-6",
            "t1-0, t2-1, t2-3, t2-4, t3-0",
            "t1-1, t1-2, t1-4, t1-5, t3-1",
        )
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = assignment,
            numPartitionsWithRackMismatch = 0,
        )

        // One consumer on a rack with no partitions. We allocate with misaligned rack to this consumer to maintain balance.
        verifyNonEqualSubscription(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-5, t2-0, t2-2, t2-4, t2-6",
                "t1-3, t2-1, t2-3, t2-5, t3-0",
                "t1-0, t1-1, t1-2, t1-4, t3-1",
            ),
            numPartitionsWithRackMismatch = 5,
        )

        // Verify that rack-awareness is improved if already owned partitions are misaligned.
        // Rack alignment is attempted, but not guaranteed.
        val owned: List<String> = mutableListOf(
            "t1-0, t1-1, t1-2, t1-3, t1-4",
            "t1-5, t2-0, t2-1, t2-2, t2-3",
            "t2-4, t2-5, t2-6, t3-0, t3-1"
        )
        if (assignor is StickyAssignor) {
            assignment = listOf(
                "t1-3, t2-0, t2-2, t2-3, t2-6",
                "t1-4, t2-1, t2-4, t2-5, t3-0",
                "t1-0, t1-1, t1-2, t1-5, t3-1"
            )
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 1,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = owned,
                expectedAssignments = assignment,
                numPartitionsWithRackMismatch = 4,
            )
        } else {
            val intermediate: List<String> = listOf("t1-3", "t2-1", "t3-1")
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 1,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = owned,
                expectedAssignments = intermediate,
                numPartitionsWithRackMismatch = 0,
            )
            assignment = listOf(
                "t1-3, t2-0, t2-2, t2-3, t2-6",
                "t1-4, t2-1, t2-4, t2-5, t3-0",
                "t1-0, t1-1, t1-2, t1-5, t3-1"
            )
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 1,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = intermediate,
                expectedAssignments = assignment,
                numPartitionsWithRackMismatch = 4,
            )
        }

        // Verify that result is same as non-rack-aware assignment if all racks match
        if (assignor is StickyAssignor) {
            assignment = mutableListOf(
                "t1-5, t2-0, t2-2, t2-4, t2-6",
                "t1-3, t2-1, t2-3, t2-5, t3-0",
                "t1-0, t1-1, t1-2, t1-4, t3-1"
            )
            preferRackAwareLogic(assignor, false)
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 3,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = owned,
                expectedAssignments = assignment,
                numPartitionsWithRackMismatch = 0,
            )
            preferRackAwareLogic(assignor, true)
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 3,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = owned,
                expectedAssignments = assignment,
                numPartitionsWithRackMismatch = 0,
            )
        } else {
            assignment = mutableListOf(
                "t1-2, t1-3, t1-4, t2-4, t2-5",
                "t2-0, t2-1, t2-2, t2-3, t2-6",
                "t1-0, t1-1, t1-5, t3-0, t3-1"
            )
            val intermediate: List<String> = mutableListOf("t1-2, t1-3, t1-4", "t2-0, t2-1, t2-2, t2-3", "t3-0, t3-1")
            preferRackAwareLogic(assignor, false)
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 3,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = owned,
                expectedAssignments = intermediate,
                numPartitionsWithRackMismatch = 0,
            )
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 3,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = intermediate,
                expectedAssignments = assignment,
                numPartitionsWithRackMismatch = 0,
            )
            preferRackAwareLogic(assignor, true)
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 3,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = owned,
                expectedAssignments = intermediate,
                numPartitionsWithRackMismatch = 0,
            )
            verifyRackAssignment(
                assignor = assignor,
                numPartitionsPerTopic = topics,
                replicationFactor = 3,
                brokerRacks = racks(3),
                consumerRacks = racks(3),
                consumerTopics = consumerTopics,
                consumerOwnedPartitions = intermediate,
                expectedAssignments = assignment,
                numPartitionsWithRackMismatch = 0,
            )
        }
    }

    private fun verifyNonEqualSubscription(
        assignor: AbstractStickyAssignor,
        numPartitionsPerTopic: Map<String, Int>,
        replicationFactor: Int,
        brokerRacks: List<String?>,
        consumerRacks: List<String?>,
        consumerTopics: List<List<String>>,
        expectedAssignments: List<String>,
        numPartitionsWithRackMismatch: Int,
    ) {
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = numPartitionsPerTopic,
            replicationFactor = replicationFactor,
            brokerRacks = brokerRacks,
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = null,
            expectedAssignments = expectedAssignments,
            numPartitionsWithRackMismatch = numPartitionsWithRackMismatch,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = numPartitionsPerTopic,
            replicationFactor = replicationFactor,
            brokerRacks = brokerRacks,
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = expectedAssignments,
            expectedAssignments = expectedAssignments,
            numPartitionsWithRackMismatch = numPartitionsWithRackMismatch,
        )
    }

    private fun getTopicName(i: Int, maxNum: Int): String = getCanonicalName("t", i, maxNum)

    private fun getConsumerName(i: Int, maxNum: Int): String = getCanonicalName("c", i, maxNum)

    private fun getCanonicalName(str: String, i: Int, maxNum: Int): String =
        str + pad(i, maxNum.toString().length)

    private fun pad(num: Int, digits: Int): String {
        val sb = StringBuilder()
        val iDigits = num.toString().length

        for (i in 1..digits - iDigits) sb.append("0")

        sb.append(num)

        return sb.toString()
    }

    protected fun consumerRackId(consumerIndex: Int): String? {
        val numRacks =
            if (numBrokerRacks > 0) numBrokerRacks
            else ALL_RACKS.size
        return if (hasConsumerRack) ALL_RACKS[consumerIndex % numRacks] else null
    }

    protected fun subscription(topics: List<String>, consumerIndex: Int): Subscription {
        return Subscription(
            topics = topics,
            userData = null,
            ownedPartitions = emptyList(),
            generationId = AbstractStickyAssignor.DEFAULT_GENERATION,
            rackId = consumerRackId(consumerIndex),
        )
    }

    /**
     * Verifies that the given assignment is valid with respect to the given subscriptions
     * Validity requirements:
     * - each consumer is subscribed to topics of all partitions assigned to it, and
     * - each partition is assigned to no more than one consumer
     * Balance requirements:
     * - the assignment is fully balanced (the numbers of topic partitions assigned to consumers differ by at most one), or
     * - there is no topic partition that can be moved from one consumer to another with 2+ fewer topic partitions
     *
     * @param subscriptions topic subscriptions of each consumer
     * @param assignments given assignment for balance check
     * @param partitionsPerTopic number of partitions per topic
     */
    internal open fun verifyValidityAndBalance(
        subscriptions: MutableMap<String, Subscription>,
        assignments: MutableMap<String, MutableList<TopicPartition>>,
        partitionsPerTopic: MutableMap<String, MutableList<PartitionInfo>>,
    ) {
        val size = subscriptions.size
        assert(size == assignments.size)
        val consumers = assignments.keys.sorted()
        for (i in 0..<size) {
            val consumer = consumers[i]
            val partitions = assignments[consumer]!!
            for (partition in partitions) assertTrue(
                actual = subscriptions[consumer]!!.topics.contains(partition.topic),
                message = "Error: Partition ${partition}is assigned to c$i, but it is not subscribed to " +
                        "Topic t${partition.topic}\nSubscriptions: $subscriptions\nAssignments: $assignments",
            )
            if (i == size - 1) continue
            for (j in i + 1..<size) {
                val otherConsumer = consumers[j]
                val otherPartitions = assignments[otherConsumer]!!
                val intersection = partitions.intersect(otherPartitions.toSet())
                assertTrue(
                    actual = intersection.isEmpty(),
                    message = "Error: Consumers c$i and c$j have common partitions assigned to them: " +
                            "$intersection\nSubscriptions: $subscriptions\nAssignments: $assignments"
                )
                val len = partitions.size
                val otherLen = otherPartitions.size
                if ((len - otherLen).absoluteValue <= 1) continue
                val map = groupPartitionsByTopic(partitions)
                val otherMap = groupPartitionsByTopic(otherPartitions)
                val moreLoaded = if (len > otherLen) i else j
                val lessLoaded = if (len > otherLen) j else i

                // If there's any overlap in the subscribed topics, we should have been able to balance partitions
                for (topic in map.keys) assertFalse(
                    actual = otherMap.containsKey(topic),
                    message = "Error: Some partitions can be moved from c$moreLoaded to c$lessLoaded to achieve " +
                            "a better balance" +
                            "\nc$i has $len partitions, and c$j has $otherLen partitions." +
                            "\nSubscriptions: $subscriptions" +
                            "\nAssignments: $assignments"
                )
            }
        }
    }

    protected fun memberData(subscription: Subscription): AbstractStickyAssignor.MemberData =
        assignor.memberData(subscription)

    protected fun partitionInfos(topic: String, numberOfPartitions: Int): MutableList<PartitionInfo> {
        val nextIndex = nextPartitionIndex
        nextPartitionIndex += 1
        return AbstractPartitionAssignorTest.partitionInfos(
            topic = topic,
            numberOfPartitions = numberOfPartitions,
            replicationFactor = replicationFactor,
            numBrokerRacks = numBrokerRacks,
            nextNodeIndex = nextIndex,
        )
    }

    protected fun initializeRacks(rackConfig: RackConfig) {
        replicationFactor = 3
        numBrokerRacks = if (rackConfig != RackConfig.NO_BROKER_RACK) 3 else 0
        hasConsumerRack = rackConfig != RackConfig.NO_CONSUMER_RACK
        preferRackAwareLogic(assignor, true)
    }

    companion object {

        internal fun isFullyBalanced(assignment: Map<String, List<TopicPartition?>>): Boolean {
            var min = Int.MAX_VALUE
            var max = Int.MIN_VALUE
            for (topicPartitions in assignment.values) {
                val size = topicPartitions.size
                if (size < min) min = size
                if (size > max) max = size
            }
            return max - min <= 1
        }

        internal fun getRandomSublist(list: List<String>): List<String> {
            val selectedItems = list.toMutableList()
            val len = list.size
            val howManyToRemove = Random.nextInt(len)

            for (i in 1..howManyToRemove) selectedItems.removeAt(Random.nextInt(selectedItems.size))

            return selectedItems
        }
    }
}
