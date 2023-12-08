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
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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

    private lateinit var staticMemberInfos: List<MemberInfo>

    @BeforeEach
    fun setUp() {
        staticMemberInfos = listOf(
            MemberInfo(consumer1, instance1),
            MemberInfo(consumer2, instance2),
            MemberInfo(consumer3, instance3),
        )
    }

    @Test
    fun testOneConsumerNoTopic() {
        val assignment = assignor.assign(
            partitionsPerTopic = emptyMap(),
            subscriptions = mapOf(consumer1 to Subscription(emptyList())),
        )
        assertEquals(setOf(consumer1), assignment.keys)
        assertTrue(assignment[consumer1]!!.isEmpty())
    }

    @Test
    fun testOneConsumerNonexistentTopic() {
        val assignment = assignor.assign(
            partitionsPerTopic = emptyMap(),
            subscriptions = mapOf(consumer1 to Subscription(topics = listOf(topic1))),
        )
        assertEquals(setOf(consumer1), assignment.keys)
        assertTrue(assignment[consumer1]!!.isEmpty())
    }

    @Test
    fun testOneConsumerOneTopic() {
        val partitionsPerTopic = mapOf(topic1 to 3)
        val assignment = assignor.assign(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to Subscription(topics = listOf(topic1))),
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

    @Test
    fun testOnlyAssignsPartitionsFromSubscribedTopics() {
        val otherTopic = "other"
        val partitionsPerTopic = mapOf(
            topic1 to 3,
            otherTopic to 3,
        )
        val assignment = assignor.assign(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to Subscription(listOf(topic1))),
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

    @Test
    fun testOneConsumerMultipleTopics() {
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 1,
            numberOfPartitions2 = 2,
        )
        val assignment = assignor.assign(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumer1 to Subscription(listOf(topic1, topic2))),
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

    @Test
    fun testTwoConsumersOneTopicOnePartition() {
        val partitionsPerTopic = mapOf(topic1 to 1)
        val consumers = mapOf(
            consumer1 to Subscription(listOf(topic1)),
            consumer2 to Subscription(listOf(topic1)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertAssignment(
            expected = emptyList(),
            actual = assignment[consumer2]!!,
        )
    }

    @Test
    fun testTwoConsumersOneTopicTwoPartitions() {
        val partitionsPerTopic = mapOf(topic1 to 2)
        val consumers = mapOf(
            consumer1 to Subscription(listOf(topic1)),
            consumer2 to Subscription(listOf(topic1)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertAssignment(
            expected = listOf(TopicPartition(topic = topic1, partition = 1)),
            actual = assignment[consumer2]!!,
        )
    }

    @Test
    fun testMultipleConsumersMixedTopics() {
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 2,
        )
        val consumers = mapOf(
            consumer1 to Subscription(listOf(topic1)),
            consumer2 to Subscription(listOf(topic1, topic2)),
            consumer3 to Subscription(listOf(topic1)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
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

    @Test
    fun testTwoConsumersTwoTopicsSixPartitions() {
        val topic1 = "topic1"
        val topic2 = "topic2"
        val consumer1 = "consumer1"
        val consumer2 = "consumer2"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumers = mapOf(
            consumer1 to Subscription(listOf(topic1, topic2)),
            consumer2 to Subscription(listOf(topic1, topic2)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
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

    @Test
    fun testTwoStaticConsumersTwoTopicsSixPartitions() {
        // although consumer high has a higher rank than consumer low, the comparison happens on
        // instance id level.
        val consumerIdLow = "consumer-b"
        val consumerIdHigh = "consumer-a"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumerLowSubscription = Subscription(
            topics = listOf(topic1, topic2),
            userData = null,
            ownedPartitions = emptyList(),
        )
        consumerLowSubscription.groupInstanceId = instance1

        val consumerHighSubscription = Subscription(
            topics = listOf(topic1, topic2),
            userData = null,
            ownedPartitions = emptyList(),
        )
        consumerHighSubscription.groupInstanceId = instance2
        val consumers = mapOf(
            consumerIdLow to consumerLowSubscription,
            consumerIdHigh to consumerHighSubscription,
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
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

    @Test
    fun testOneStaticConsumerAndOneDynamicConsumerTwoTopicsSixPartitions() {
        // although consumer high has a higher rank than low, consumer low will win the comparison
        // because it has instance id while consumer 2 doesn't.
        val consumerIdLow = "consumer-b"
        val consumerIdHigh = "consumer-a"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3)
        val consumerLowSubscription = Subscription(
            topics = listOf(topic1, topic2),
            userData = null,
            ownedPartitions = emptyList(),
        )
        consumerLowSubscription.groupInstanceId = instance1
        val consumers = mapOf(
            consumerIdLow to consumerLowSubscription,
            consumerIdHigh to Subscription(topics = listOf(topic1, topic2)),
        )

        val assignment = assignor.assign(partitionsPerTopic, consumers)
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
                TopicPartition(topic = topic1, partition =  2),
                TopicPartition(topic = topic2, partition =  2),
            ),
            actual = assignment[consumerIdHigh]!!,
        )
    }

    @Test
    fun testStaticMemberRangeAssignmentPersistent() {
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 5,
            numberOfPartitions2 = 4,
        )
        val consumers = mutableMapOf<String, Subscription>()
        for (m in staticMemberInfos) {
            val subscription = Subscription(
                topics = listOf(topic1, topic2),
                userData = null,
                ownedPartitions = emptyList(),
            )
            subscription.groupInstanceId = m.groupInstanceId
            consumers[m.memberId] = subscription
        }
        // Consumer 4 is a dynamic member.
        val consumer4 = "consumer4"
        consumers[consumer4] = Subscription(topics = listOf(topic1, topic2))
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

        var assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(expectedAssignment, assignment)

        // Replace dynamic member 4 with a new dynamic member 5.
        consumers.remove(consumer4)
        val consumer5 = "consumer5"
        consumers[consumer5] = Subscription(topics = listOf(topic1, topic2))
        expectedAssignment.remove(consumer4)
        expectedAssignment[consumer5] = mutableListOf(
            TopicPartition(topic = topic1, partition = 4),
            TopicPartition(topic = topic2, partition = 3),
        )
        assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(expectedAssignment, assignment)
    }

    @Test
    fun testStaticMemberRangeAssignmentPersistentAfterMemberIdChanges() {
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 5,
            numberOfPartitions2 = 5,
        )
        val consumers = mutableMapOf<String, Subscription>()
        for (m in staticMemberInfos) {
            val subscription = Subscription(
                topics = listOf(topic1, topic2),
                userData = null,
                ownedPartitions = emptyList()
            )
            subscription.groupInstanceId = m.groupInstanceId
            consumers[m.memberId] = subscription
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
                TopicPartition(topic = topic2, partition =  2),
                TopicPartition(topic = topic2, partition =  3),
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

    private fun assertAssignment(expected: List<TopicPartition>, actual: List<TopicPartition>) {
        // order doesn't matter for assignment, so convert to a set
        assertEquals(expected.toSet(), actual.toSet())
    }

    private fun setupPartitionsPerTopicWithTwoTopics(
        numberOfPartitions1: Int,
        numberOfPartitions2: Int,
    ): Map<String, Int> = mapOf(
        topic1 to numberOfPartitions1,
        topic2 to numberOfPartitions2,
    )

    companion object {

        fun checkStaticAssignment(
            assignor: AbstractPartitionAssignor,
            partitionsPerTopic: Map<String, Int>,
            consumers: Map<String, Subscription>,
        ): Map<String, List<TopicPartition>> {
            val assignmentByMemberId: Map<String, List<TopicPartition>> = assignor.assign(partitionsPerTopic, consumers)
            val assignmentByInstanceId: MutableMap<String, List<TopicPartition>> = HashMap()
            for ((memberId, value) in consumers) {
                val instanceId = value.groupInstanceId
                instanceId?.let { id -> assignmentByInstanceId[id] = assignmentByMemberId[memberId]!! }
            }
            return assignmentByInstanceId
        }
    }
}
