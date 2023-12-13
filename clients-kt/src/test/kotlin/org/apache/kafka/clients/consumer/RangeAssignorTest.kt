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

import java.util.Arrays
import java.util.Optional
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.TEST_NAME_WITH_RACK_CONFIG
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.nullRacks
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.racks
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.Companion.verifyRackAssignment
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.RackConfig
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RangeAssignorTest {

    private val assignor = RangeAssignor()

    // For plural tests
    private val topic1 = "topic1"

    private val topic2 = "topic2"

    private val consumer1 = "consumer1"

    private val instance1 = "instance1"

    private val consumer2 = "consumer2"

    private val instance2 = "instance2"

    private val consumer3 = "consumer3"

    private val instance3 = "instance3"

    private var numBrokerRacks = 0

    private var hasConsumerRack = false

    private lateinit var staticMemberInfos: List<MemberInfo>

    private var replicationFactor = 3

    @BeforeEach
    fun setUp() {
        staticMemberInfos = listOf(
            MemberInfo(consumer1, instance1),
            MemberInfo(consumer2, instance2),
            MemberInfo(consumer3, instance3),
        )
    }

    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [true, false])
    fun testOneConsumerNoTopic(hasConsumerRack: Boolean) {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()

        val assignment = assignor.assignPartitions(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to subscription(emptyList(), 0)),
        )

        assertEquals(setOf(consumer1), assignment.keys)
        assertTrue(assignment[consumer1]!!.isEmpty())
    }

    @ParameterizedTest(name = AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = [true, false])
    fun testOneConsumerNonexistentTopic(hasConsumerRack: Boolean) {
        initializeRacks(
            if (hasConsumerRack) RackConfig.BROKER_AND_CONSUMER_RACK
            else RackConfig.NO_CONSUMER_RACK,
        )
        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        val assignment = assignor.assignPartitions(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to subscription(listOf(topic1), 0)),
        )
        assertEquals(setOf(consumer1), assignment.keys)
        assertTrue(assignment[consumer1]!!.isEmpty())
    }

    @ParameterizedTest(name = "rackConfig = {0}")
    @EnumSource(RackConfig::class)
    fun testOneConsumerOneTopic(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 3),
        )
        val assignment = assignor.assignPartitions(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to subscription(topics = listOf(topic1), 0)),
        )
        assertEquals(setOf(consumer1), assignment.keys)
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic1, partition = 2),
            ),
            actual = assignment[consumer1]!!
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOnlyAssignsPartitionsFromSubscribedTopics(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val otherTopic = "other"

        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 3),
            otherTopic to partitionInfos(otherTopic, 3),
        )
        val assignment = assignor.assignPartitions(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to subscription(listOf(topic1), 0)),
        )
        assertEquals(setOf(consumer1), assignment.keys)
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic1, partition = 2),
            ),
            actual = assignment[consumer1]!!
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOneConsumerMultipleTopics(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 1,
            numberOfPartitions2 = 2,
        )
        val assignment = assignor.assignPartitions(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to subscription(listOf(topic1, topic2), 0)),
        )
        assertEquals(setOf(consumer1), assignment.keys)
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer1]!!
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoConsumersOneTopicOnePartition(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 1),
        )
        val consumers = mapOf(
            consumer1 to subscription(listOf(topic1), 0),
            consumer2 to subscription(listOf(topic1), 1),
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertAssignment(
            expected = emptyList(),
            actual = assignment[consumer2]!!,
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoConsumersOneTopicTwoPartitions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = mutableMapOf(
            topic1 to partitionInfos(topic1, 2),
        )
        val consumers = mapOf(
            consumer1 to subscription(listOf(topic1), 0),
            consumer2 to subscription(listOf(topic1), 1),
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 1)),
            actual = assignment[consumer2]!!,
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testMultipleConsumersMixedTopics(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 2,
        )
        val consumers = mapOf(
            consumer1 to subscription(listOf(topic1), 0),
            consumer2 to subscription(listOf(topic1, topic2), 1),
            consumer3 to subscription(listOf(topic1), 2),
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!
        )
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer2]!!,
        )
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 2)),
            actual = assignment[consumer3]!!,
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoConsumersTwoTopicsSixPartitions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        val topic1 = "topic1"
        val topic2 = "topic2"
        val consumer1 = "consumer1"
        val consumer2 = "consumer2"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumers = mapOf(
            consumer1 to subscription(listOf(topic1, topic2), 0),
            consumer2 to subscription(listOf(topic1, topic2), 1),
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumer2]!!,
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testTwoStaticConsumersTwoTopicsSixPartitions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        // although consumer high has a higher rank than consumer low, the comparison happens on
        // instance id level.
        val consumerIdLow = "consumer-b"
        val consumerIdHigh = "consumer-a"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumerLowSubscription = subscription(listOf(topic1, topic2), 0)
        consumerLowSubscription.groupInstanceId = instance1

        val consumerHighSubscription = subscription(listOf(topic1, topic2), 1)
        consumerHighSubscription.groupInstanceId = instance2
        val consumers = mapOf(
            consumerIdLow to consumerLowSubscription,
            consumerIdHigh to consumerHighSubscription,
        )
        val assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumerIdLow]!!
        )
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumerIdHigh]!!,
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testOneStaticConsumerAndOneDynamicConsumerTwoTopicsSixPartitions(rackConfig: RackConfig) {
        initializeRacks(rackConfig)
        // although consumer high has a higher rank than low, consumer low will win the comparison
        // because it has instance id while consumer 2 doesn't.
        val consumerIdLow = "consumer-b"
        val consumerIdHigh = "consumer-a"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3)
        val consumerLowSubscription = subscription(listOf(topic1, topic2), 0)
        consumerLowSubscription.groupInstanceId = instance1
        val consumers = mapOf(
            consumerIdLow to consumerLowSubscription,
            consumerIdHigh to subscription(topics = listOf(topic1, topic2), 1),
        )

        val assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumerIdLow]!!,
        )
        assertAssignment(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 2),
            ),
            actual = assignment[consumerIdHigh]!!,
        )
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testStaticMemberRangeAssignmentPersistent(rackConfig: RackConfig) {
        initializeRacks(rackConfig, 5)
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 5,
            numberOfPartitions2 = 4,
        )
        val consumers = mutableMapOf<String, Subscription>()
        var consumerIndex = 0
        for (m in staticMemberInfos) {
            val subscription = subscription(listOf(topic1, topic2), consumerIndex++)
            subscription.groupInstanceId = m.groupInstanceId
            consumers[m.memberId] = subscription
        }
        // Consumer 4 is a dynamic member.
        val consumer4 = "consumer4"
        consumers[consumer4] = subscription(listOf(topic1, topic2), consumerIndex++)
        val expectedAssignment = mutableMapOf(
            // Have 3 static members instance1, instance2, instance3 to be persistent
            // across generations. Their assignment shall be the same.
            consumer1 to mutableListOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
            ),
            consumer2 to mutableListOf(
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 1),
            ),
            consumer3 to mutableListOf(
                TopicPartition(topic = topic1, partition = 3),
                TopicPartition(topic = topic2, partition = 2),
            ),
            consumer4 to mutableListOf(
                TopicPartition(topic = topic1, partition = 4),
                TopicPartition(topic = topic2, partition = 3),
            ),
        )

        var assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertEquals(expectedAssignment, assignment)

        // Replace dynamic member 4 with a new dynamic member 5.
        consumers.remove(consumer4)
        val consumer5 = "consumer5"
        consumers[consumer5] = subscription(listOf(topic1, topic2), consumerIndex++)
        expectedAssignment.remove(consumer4)
        expectedAssignment[consumer5] = mutableListOf(
            TopicPartition(topic = topic1, partition = 4),
            TopicPartition(topic = topic2, partition = 3),
        )
        assignment = assignor.assignPartitions(partitionsPerTopic, consumers)
        assertEquals(expectedAssignment, assignment)
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig::class)
    fun testStaticMemberRangeAssignmentPersistentAfterMemberIdChanges() {
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 5,
            numberOfPartitions2 = 5,
        )
        val consumers = mutableMapOf<String, Subscription>()
        for ((consumerIndex, member) in staticMemberInfos.withIndex()) {
            val subscription = subscription(listOf(topic1, topic2), consumerIndex)
            subscription.groupInstanceId = member.groupInstanceId
            consumers[member.memberId] = subscription
        }
        val expectedInstanceAssignment = mapOf(
            instance1 to listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            instance2 to listOf(
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic1, partition = 3),
                TopicPartition(topic = topic2, partition = 2),
                TopicPartition(topic = topic2, partition = 3),
            ),
            instance3 to listOf(
                TopicPartition(topic = topic1, partition = 4),
                TopicPartition(topic = topic2, partition = 4),
            )
        )

        val staticAssignment = checkStaticAssignment(assignor, partitionsPerTopic, consumers)
        assertEquals(expectedInstanceAssignment, staticAssignment)

        // Now switch the member.id fields for each member info, the assignment should
        // stay the same as last time.
        val consumer4 = "consumer4"
        val consumer5 = "consumer5"
        consumers[consumer4] = consumers[consumer3]!!
        consumers.remove(consumer3)
        consumers[consumer5] = consumers[consumer2]!!
        consumers.remove(consumer2)
        val newStaticAssignment = checkStaticAssignment(assignor, partitionsPerTopic, consumers)
        assertEquals(staticAssignment, newStaticAssignment)
    }

    @Test
    fun testRackAwareStaticMemberRangeAssignmentPersistentAfterMemberIdChanges() {
        initializeRacks(RackConfig.BROKER_AND_CONSUMER_RACK)
        val replicationFactor = 2
        val numBrokerRacks = 3
        val partitionsPerTopic = mutableMapOf(
            topic1 to  AbstractPartitionAssignorTest.partitionInfos(
                topic = topic1,
                numberOfPartitions = 5,
                replicationFactor = replicationFactor,
                numBrokerRacks = numBrokerRacks,
                nextNodeIndex = 0,
            ),
            topic2 to AbstractPartitionAssignorTest.partitionInfos(
                topic = topic2,
                numberOfPartitions = 5,
                replicationFactor = replicationFactor,
                numBrokerRacks = numBrokerRacks,
                nextNodeIndex = 0,
            ),
        )
        val staticMemberInfos = listOf(
            MemberInfo(
                memberId = consumer1,
                groupInstanceId = instance1,
                rackId = AbstractPartitionAssignorTest.ALL_RACKS[0],
            ),
            MemberInfo(
                memberId = consumer2,
                groupInstanceId = instance2,
                rackId = AbstractPartitionAssignorTest.ALL_RACKS[1],
            ),
            MemberInfo(
                memberId = consumer3,
                groupInstanceId = instance3,
                rackId = AbstractPartitionAssignorTest.ALL_RACKS[2],
            ),
        )
        val consumers = mutableMapOf<String, Subscription>()
        for ((consumerIndex, member) in staticMemberInfos.withIndex()) {
            val subscription = subscription(listOf(topic1, topic2), consumerIndex)
            subscription.groupInstanceId = member.groupInstanceId
            consumers[member.memberId] = subscription
        }
        val expectedInstanceAssignment = mutableMapOf(
            instance1 to listOf(
                TopicPartition(topic1, 0),
                TopicPartition(topic1, 2),
                TopicPartition(topic2, 0),
                TopicPartition(topic2, 2)
            ),
            instance2 to listOf(
                TopicPartition(topic1, 1),
                TopicPartition(topic1, 3),
                TopicPartition(topic2, 1),
                TopicPartition(topic2, 3)
            ),
            instance3 to listOf(
                TopicPartition(topic1, 4),
                TopicPartition(topic2, 4),
            ),
        )
        val staticAssignment = checkStaticAssignment(assignor, partitionsPerTopic, consumers)
        assertEquals(expectedInstanceAssignment, staticAssignment)

        // Now switch the member.id fields for each member info, the assignment should
        // stay the same as last time.
        val consumer4 = "consumer4"
        val consumer5 = "consumer5"
        consumers[consumer4] = consumers[consumer3]!!
        consumers.remove(consumer3)
        consumers[consumer5] = consumers[consumer2]!!
        consumers.remove(consumer2)
        val newStaticAssignment = checkStaticAssignment(assignor, partitionsPerTopic, consumers)
        assertEquals(staticAssignment, newStaticAssignment)
    }

    @Test
    fun testRackAwareAssignmentWithUniformSubscription() {
        val topics = mapOf("t1" to 6, "t2" to 7, "t3" to 2)
        val allTopics = listOf("t1", "t2", "t3")
        val consumerTopics = listOf(allTopics, allTopics, allTopics)

        // Verify combinations where rack-aware logic is not used.
        verifyNonRackAwareAssignment(
            topics = topics,
            consumerTopics = consumerTopics,
            nonRackAwareAssignment = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t3-0",
                "t1-2, t1-3, t2-3, t2-4, t3-1",
                "t1-4, t1-5, t2-5, t2-6",
            ),
        )

        // Verify best-effort rack-aware assignment for lower replication factor where racks have a subset of partitions.
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-3, t2-0, t2-3, t2-6",
                "t1-1, t1-4, t2-1, t2-4, t3-0",
                "t1-2, t1-5, t2-2, t2-5, t3-1",
            ),
            numPartitionsWithRackMismatch = 0,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-2, t2-0, t2-2, t2-3, t3-1",
                "t1-1, t1-3, t2-1, t2-4, t3-0",
                "t1-4, t1-5, t2-5, t2-6",
            ),
            numPartitionsWithRackMismatch = 1,
        )

        // One consumer on a rack with no partitions
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t3-0",
                "t1-2, t1-3, t2-3, t2-4, t3-1",
                "t1-4, t1-5, t2-5, t2-6",
            ),
            numPartitionsWithRackMismatch = 4,
        )
    }

    @Test
    fun testRackAwareAssignmentWithNonEqualSubscription() {
        val topics = mapOf("t1" to 6, "t2" to 7, "t3" to 2)
        val allTopics = listOf("t1", "t2", "t3")
        val consumerTopics = listOf(allTopics, allTopics, listOf("t1", "t3"))

        // Verify combinations where rack-aware logic is not used.
        verifyNonRackAwareAssignment(
            topics = topics,
            consumerTopics = consumerTopics,
            nonRackAwareAssignment = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t2-3, t3-0",
                "t1-2, t1-3, t2-4, t2-5, t2-6, t3-1",
                "t1-4, t1-5",
            ),
        )

        // Verify best-effort rack-aware assignment for lower replication factor where racks have a subset of partitions.
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-3, t2-0, t2-2, t2-3, t2-6",
                "t1-1, t1-4, t2-1, t2-4, t2-5, t3-0",
                "t1-2, t1-5, t3-1",
            ),
            numPartitionsWithRackMismatch = 2,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-2, t2-0, t2-2, t2-3, t2-5, t3-1",
                "t1-1, t1-3, t2-1, t2-4, t2-6, t3-0",
                "t1-4, t1-5",
            ),
            numPartitionsWithRackMismatch = 0,
        )

        // One consumer on a rack with no partitions
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t2-3, t3-0",
                "t1-2, t1-3, t2-4, t2-5, t2-6, t3-1",
                "t1-4, t1-5",
            ),
            numPartitionsWithRackMismatch = 2,
        )
    }

    @Test
    fun testRackAwareAssignmentWithUniformPartitions() {
        val topics = mapOf("t1" to 5, "t2" to 5, "t3" to 5)
        val allTopics = listOf("t1", "t2", "t3")
        val consumerTopics = listOf(allTopics, allTopics, allTopics)
        val nonRackAwareAssignment = listOf(
            "t1-0, t1-1, t2-0, t2-1, t3-0, t3-1",
            "t1-2, t1-3, t2-2, t2-3, t3-2, t3-3",
            "t1-4, t2-4, t3-4",
        )

        // Verify combinations where rack-aware logic is not used.
        verifyNonRackAwareAssignment(topics, consumerTopics, nonRackAwareAssignment)

        // Verify that co-partitioning is prioritized over rack-alignment
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 10,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 5,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 3,
        )
    }

    @Test
    fun testRackAwareAssignmentWithUniformPartitionsNonEqualSubscription() {
        val topics = mapOf("t1" to 5, "t2" to 5, "t3" to 5)
        val allTopics = listOf("t1", "t2", "t3")
        val consumerTopics = listOf(allTopics, allTopics, listOf("t1", "t3"))

        // Verify combinations where rack-aware logic is not used.
        verifyNonRackAwareAssignment(
            topics = topics,
            consumerTopics = consumerTopics,
            nonRackAwareAssignment = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t3-0, t3-1",
                "t1-2, t1-3, t2-3, t2-4, t3-2, t3-3",
                "t1-4, t3-4",
            ),
        )

        // Verify that co-partitioning is prioritized over rack-alignment for topics with equal subscriptions
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-4, t3-0, t3-1",
                "t1-2, t1-3, t2-2, t2-3, t3-2, t3-3",
                "t1-4, t3-4",
            ),
            numPartitionsWithRackMismatch = 9,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-2, t2-0, t2-1, t2-3, t3-2",
                "t1-0, t1-3, t2-2, t2-4, t3-0, t3-3",
                "t1-1, t1-4, t3-1, t3-4",
            ),
            numPartitionsWithRackMismatch = 0,
        )

        // One consumer on a rack with no partitions
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t3-0, t3-1",
                "t1-2, t1-3, t2-3, t2-4, t3-2, t3-3",
                "t1-4, t3-4",
            ),
            numPartitionsWithRackMismatch = 2,
        )
    }

    @Test
    fun testRackAwareAssignmentWithCoPartitioning() {
        val topics = mapOf("t1" to 6, "t2" to 6, "t3" to 2, "t4" to 2)
        var consumerTopics = listOf(
            listOf("t1", "t2"),
            listOf("t1", "t2"),
            listOf("t3", "t4"),
            listOf("t3", "t4")
        )
        val consumerRacks = listOf(
            AbstractPartitionAssignorTest.ALL_RACKS[0],
            AbstractPartitionAssignorTest.ALL_RACKS[1],
            AbstractPartitionAssignorTest.ALL_RACKS[1],
            AbstractPartitionAssignorTest.ALL_RACKS[0],
        )
        var nonRackAwareAssignment = listOf(
            "t1-0, t1-1, t1-2, t2-0, t2-1, t2-2",
            "t1-3, t1-4, t1-5, t2-3, t2-4, t2-5",
            "t3-0, t4-0",
            "t3-1, t4-1",
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-2, t1-4, t2-0, t2-2, t2-4",
                "t1-1, t1-3, t1-5, t2-1, t2-3, t2-5",
                "t3-1, t4-1",
                "t3-0, t4-0",
            ),
            numPartitionsWithRackMismatch = 0,
        )
        val allTopics = listOf("t1", "t2", "t3", "t4")
        consumerTopics = listOf(allTopics, allTopics, allTopics, allTopics)
        nonRackAwareAssignment = listOf(
            "t1-0, t1-1, t2-0, t2-1, t3-0, t4-0",
            "t1-2, t1-3, t2-2, t2-3, t3-1, t4-1",
            "t1-4, t2-4",
            "t1-5, t2-5",
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(2),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-2, t2-0, t2-2, t3-0, t4-0",
                "t1-1, t1-3, t2-1, t2-3, t3-1, t4-1",
                "t1-5, t2-5",
                "t1-4, t2-4",
            ),
            numPartitionsWithRackMismatch = 0,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 1,
            brokerRacks = racks(3),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = listOf(
                "t1-0, t1-3, t2-0, t2-3, t3-0, t4-0",
                "t1-1, t1-4, t2-1, t2-4, t3-1, t4-1",
                "t1-2, t2-2",
                "t1-5, t2-5",
            ),
            numPartitionsWithRackMismatch = 6,
        )
    }

    @Test
    fun testCoPartitionedAssignmentWithSameSubscription() {
        val topics = mapOf(
            "t1" to 6, "t2" to 6,
            "t3" to 2, "t4" to 2,
            "t5" to 4, "t6" to 4,
        )
        val topicList = listOf("t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9")
        val consumerTopics = listOf(topicList, topicList, topicList)
        val consumerRacks = listOf(
            AbstractPartitionAssignorTest.ALL_RACKS[0],
            AbstractPartitionAssignorTest.ALL_RACKS[1],
            AbstractPartitionAssignorTest.ALL_RACKS[2],
        )
        val nonRackAwareAssignment = listOf(
            "t1-0, t1-1, t2-0, t2-1, t3-0, t4-0, t5-0, t5-1, t6-0, t6-1",
            "t1-2, t1-3, t2-2, t2-3, t3-1, t4-1, t5-2, t6-2",
            "t1-4, t1-5, t2-4, t2-5, t5-3, t6-3",
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = nullRacks(3),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true)
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        val rackAwareAssignment = listOf(
            "t1-0, t1-2, t2-0, t2-2, t3-0, t4-0, t5-1, t6-1",
            "t1-1, t1-3, t2-1, t2-3, t3-1, t4-1, t5-2, t6-2",
            "t1-4, t1-5, t2-4, t2-5, t5-0, t5-3, t6-0, t6-3",
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 2,
            brokerRacks = racks(3),
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            expectedAssignments = rackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
    }

    private fun verifyNonRackAwareAssignment(
        topics: Map<String, Int>,
        consumerTopics: List<List<String>>,
        nonRackAwareAssignment: List<String>,
    ) {
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = nullRacks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = nullRacks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 4,
            brokerRacks = racks(4),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = listOf("d", "e", "f"),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = listOf(null, "e", "f"),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = -1,
        )
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true)
        verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = topics,
            replicationFactor = 3,
            brokerRacks = racks(3),
            consumerRacks = racks(3),
            consumerTopics = consumerTopics,
            expectedAssignments = nonRackAwareAssignment,
            numPartitionsWithRackMismatch = 0,
        )
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false)
    }

    private fun assertAssignment(expected: List<TopicPartition>, actual: List<TopicPartition>) {
        // order doesn't matter for assignment, so convert to a set
        assertEquals(expected.toSet(), actual.toSet())
    }

    private fun setupPartitionsPerTopicWithTwoTopics(
        numberOfPartitions1: Int,
        numberOfPartitions2: Int,
    ): MutableMap<String, MutableList<PartitionInfo>> = mutableMapOf(
        topic1 to partitionInfos(topic1, numberOfPartitions1),
        topic2 to partitionInfos(topic2, numberOfPartitions2),
    )

    private fun partitionInfos(topic: String, numberOfPartitions: Int): MutableList<PartitionInfo> {
        return AbstractPartitionAssignorTest.partitionInfos(
            topic = topic,
            numberOfPartitions = numberOfPartitions,
            replicationFactor = replicationFactor,
            numBrokerRacks = numBrokerRacks,
            nextNodeIndex = 0,
        )
    }

    private fun subscription(topics: List<String>, consumerIndex: Int): Subscription {
        val numRacks =
            if (numBrokerRacks > 0) numBrokerRacks
            else AbstractPartitionAssignorTest.ALL_RACKS.size
        val rackId =
            if (hasConsumerRack) AbstractPartitionAssignorTest.ALL_RACKS[consumerIndex % numRacks]
            else null

        return Subscription(
            topics = topics,
            userData = null,
            ownedPartitions = emptyList(),
            generationId = -1,
            rackId = rackId,
        )
    }

    fun initializeRacks(rackConfig: RackConfig, maxConsumers: Int = 3) {
        replicationFactor = maxConsumers
        numBrokerRacks = if (rackConfig != RackConfig.NO_BROKER_RACK) maxConsumers else 0
        hasConsumerRack = rackConfig != RackConfig.NO_CONSUMER_RACK
        // Rack and consumer ordering are the same in all the tests, so we can verify
        // rack-aware logic using the same tests.
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true)
    }

    companion object {

        private fun checkStaticAssignment(
            assignor: AbstractPartitionAssignor,
            partitionsPerTopic: MutableMap<String, MutableList<PartitionInfo>>,
            consumers: Map<String, Subscription>,
        ): Map<String, List<TopicPartition>> {
            val assignmentByMemberId = assignor.assignPartitions(partitionsPerTopic, consumers)
            val assignmentByInstanceId = mutableMapOf<String, MutableList<TopicPartition>>()
            for ((memberId, value) in consumers) {
                val instanceId = value.groupInstanceId
                instanceId?.let { id -> assignmentByInstanceId[id] = assignmentByMemberId[memberId]!! }
            }
            return assignmentByInstanceId
        }
    }
}
