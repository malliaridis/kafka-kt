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

import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Collectors
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RoundRobinAssignorTest {
    
    private val assignor = RoundRobinAssignor()
    
    private val topic = "topic"
    
    private val consumerId = "consumer"
    
    private val topic1 = "topic1"
    
    private val topic2 = "topic2"
    
    @Test
    fun testOneConsumerNoTopic() {
        val assignment = assignor.assign(
            partitionsPerTopic = emptyMap(),
            subscriptions = mapOf(consumerId to Subscription(topics = emptyList())),
        )
        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(assignment[consumerId]!!.isEmpty())
    }

    @Test
    fun testOneConsumerNonexistentTopic() {
        val assignment = assignor.assign(
            partitionsPerTopic = emptyMap(),
            subscriptions = mapOf(consumerId to Subscription(topics = listOf(topic))),
        )
        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(assignment[consumerId]!!.isEmpty())
    }

    @Test
    fun testOneConsumerOneTopic() {
        val partitionsPerTopic = mapOf(topic to 3)
        val assignment = assignor.assign(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumerId to Subscription(topics = listOf(topic))),
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition =  0),
                TopicPartition(topic = topic, partition =  1),
                TopicPartition(topic = topic, partition =  2),
            ),
            actual = assignment[consumerId]!!,
        )
    }

    @Test
    fun testOnlyAssignsPartitionsFromSubscribedTopics() {
        val otherTopic = "other"
        val partitionsPerTopic = mapOf(topic to 3, otherTopic to 3)
        val assignment = assignor.assign(
            partitionsPerTopic = partitionsPerTopic,
            subscriptions = mapOf(consumerId to Subscription(topics = listOf(topic))),
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic, partition = 0),
                TopicPartition(topic = topic, partition = 1),
                TopicPartition(topic = topic, partition = 2),
            ),
            actual = assignment[consumerId]!!,
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
            subscriptions = mapOf(consumerId to Subscription(topics = listOf(topic1, topic2))),
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumerId]!!,
        )
    }

    @Test
    fun testTwoConsumersOneTopicOnePartition() {
        val consumer1 = "consumer1"
        val consumer2 = "consumer2"
        val partitionsPerTopic = mapOf(topic to 1)
        val consumers = mapOf(
            consumer1 to Subscription(topics = listOf(topic)),
            consumer2 to Subscription(topics = listOf(topic)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = emptyList(),
            actual = assignment[consumer2]!!,
        )
    }

    @Test
    fun testTwoConsumersOneTopicTwoPartitions() {
        val consumer1 = "consumer1"
        val consumer2 = "consumer2"
        val partitionsPerTopic = mapOf(topic to 2)
        val consumers = mapOf(
            consumer1 to Subscription(topics = listOf(topic)),
            consumer2 to Subscription(topics = listOf(topic)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 0)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic, partition = 1)),
            actual = assignment[consumer2]!!,
        )
    }

    @Test
    fun testMultipleConsumersMixedTopics() {
        val topic1 = "topic1"
        val topic2 = "topic2"
        val consumer1 = "consumer1"
        val consumer2 = "consumer2"
        val consumer3 = "consumer3"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 2,
        )
        val consumers = mapOf(
            consumer1 to Subscription(topics = listOf(topic1)),
            consumer2 to Subscription(topics = listOf(topic1, topic2)),
            consumer3 to Subscription(topics = listOf(topic1)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(
            expected = listOf(TopicPartition(topic = topic1, partition = 0)),
            actual = assignment[consumer1]!!
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 1),
            ),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(TopicPartition(topic = topic1, partition = 2)),
            actual = assignment[consumer3]!!,
        )
    }

    @Test
    fun testTwoDynamicConsumersTwoTopicsSixPartitions() {
        val topic1 = "topic1"
        val topic2 = "topic2"
        val consumer1 = "consumer1"
        val consumer2 = "consumer2"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumers = mapOf(
            consumer1 to Subscription(topics = listOf(topic1, topic2)),
            consumer2 to Subscription(topics = listOf(topic1, topic2)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition =  0),
                TopicPartition(topic = topic1, partition =  2),
                TopicPartition(topic = topic2, partition =  1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(
                TopicPartition(topic = topic1, partition =  1),
                TopicPartition(topic = topic2, partition =  0),
                TopicPartition(topic = topic2, partition =  2),
            ),
            actual = assignment[consumer2]!!,
        )
    }

    @Test
    fun testTwoStaticConsumersTwoTopicsSixPartitions() {
        // although consumer 2 has a higher rank than 1, the comparison happens on
        // instance id level.
        val topic1 = "topic1"
        val topic2 = "topic2"
        val consumer1 = "consumer-b"
        val instance1 = "instance1"
        val consumer2 = "consumer-a"
        val instance2 = "instance2"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumers = mutableMapOf<String, Subscription>()
        val consumer1Subscription = Subscription(
            topics = listOf(topic1, topic2),
            userData = null,
        )
        consumer1Subscription.groupInstanceId = instance1
        consumers[consumer1] = consumer1Subscription
        val consumer2Subscription = Subscription(
            topics = listOf(topic1, topic2),
            userData = null,
        )
        consumer2Subscription.groupInstanceId = instance2
        consumers[consumer2] = consumer2Subscription
        val assignment = assignor.assign(partitionsPerTopic, consumers)
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
    }

    @Test
    fun testOneStaticConsumerAndOneDynamicConsumerTwoTopicsSixPartitions() {
        // although consumer 2 has a higher rank than 1, consumer 1 will win the comparison
        // because it has instance id while consumer 2 doesn't.
        val consumer1 = "consumer-b"
        val instance1 = "instance1"
        val consumer2 = "consumer-a"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumer1Subscription = Subscription(
            topics = listOf(topic1, topic2),
            userData = null,
        )
        consumer1Subscription.groupInstanceId = instance1
        val consumers = mapOf(
            consumer1 to consumer1Subscription,
            consumer2 to Subscription(topics = listOf(topic1, topic2)),
        )
        val assignment = assignor.assign(partitionsPerTopic, consumers)
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
    }

    @Test
    fun testStaticMemberRoundRobinAssignmentPersistent() {
        // Have 3 static members instance1, instance2, instance3 to be persistent
        // across generations. Their assignment shall be the same.
        val consumer1 = "consumer1"
        val instance1 = "instance1"
        val consumer2 = "consumer2"
        val instance2 = "instance2"
        val consumer3 = "consumer3"
        val instance3 = "instance3"
        val staticMemberInfos = listOf(
            MemberInfo(consumer1, instance1),
            MemberInfo(consumer2, instance2),
            MemberInfo(consumer3, instance3),
        )

        // Consumer 4 is a dynamic member.
        val consumer4 = "consumer4"
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 3,
            numberOfPartitions2 = 3,
        )
        val consumers = mutableMapOf<String, Subscription>()
        for (m in staticMemberInfos) {
            val subscription = Subscription(topics = listOf(topic1, topic2), userData = null)
            subscription.groupInstanceId = m.groupInstanceId
            consumers[m.memberId] = subscription
        }
        consumers[consumer4] = Subscription(topics = listOf(topic1, topic2))
        val expectedAssignment = mutableMapOf(
            consumer1 to mutableListOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition( topic = topic2, partition = 1),
            ),
            consumer2 to mutableListOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition( topic = topic2, partition = 2),
            ),
            consumer3 to mutableListOf(
                TopicPartition(topic = topic1, partition = 2),
            ),
            consumer4 to mutableListOf(
                TopicPartition(topic = topic2, partition = 0),
            ),
        )

        var assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(expectedAssignment, assignment)

        // Replace dynamic member 4 with a new dynamic member 5.
        consumers.remove(consumer4)
        val consumer5 = "consumer5"
        consumers[consumer5] = Subscription(topics = listOf(topic1, topic2))
        expectedAssignment.remove(consumer4)
        expectedAssignment[consumer5] = mutableListOf(TopicPartition(topic = topic2, partition = 0))
        assignment = assignor.assign(partitionsPerTopic, consumers)
        assertEquals(expectedAssignment, assignment)
    }

    @Test
    fun testStaticMemberRoundRobinAssignmentPersistentAfterMemberIdChanges() {
        val consumer1 = "consumer1"
        val instance1 = "instance1"
        val consumer2 = "consumer2"
        val instance2 = "instance2"
        val consumer3 = "consumer3"
        val instance3 = "instance3"
        val memberIdToInstanceId = mutableMapOf(
            consumer1 to instance1,
            consumer2 to instance2,
            consumer3 to instance3,
        )
        val partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(
            numberOfPartitions1 = 5,
            numberOfPartitions2 = 5,
        )
        val expectedInstanceAssignment = mapOf(
            instance1 to listOf(
                TopicPartition(topic = topic1, partition = 0),
                TopicPartition(topic = topic1, partition = 3),
                TopicPartition(topic = topic2, partition = 1),
                TopicPartition(topic = topic2,  partition = 4),
            ),
            instance2 to listOf(
                TopicPartition(topic = topic1, partition = 1),
                TopicPartition(topic = topic1, partition = 4),
                TopicPartition(topic = topic2, partition = 2),
            ),
            instance3 to listOf(
                TopicPartition(topic = topic1, partition = 2),
                TopicPartition(topic = topic2, partition = 0),
                TopicPartition(topic = topic2, partition = 3),
            ),
        )
        val staticMemberInfos = memberIdToInstanceId.map { (key, value) -> MemberInfo(key, value) }
        val consumers = mutableMapOf<String, Subscription>()
        for (m in staticMemberInfos) {
            val subscription = Subscription(
                topics = listOf(topic1, topic2),
                userData = null,
            )
            subscription.groupInstanceId = m.groupInstanceId
            consumers[m.memberId] = subscription
        }
        val staticAssignment = checkStaticAssignment(assignor, partitionsPerTopic, consumers)
        assertEquals(expectedInstanceAssignment, staticAssignment)
        memberIdToInstanceId.clear()

        // Now switch the member.id fields for each member info, the assignment should
        // stay the same as last time.
        val consumer4 = "consumer4"
        val consumer5 = "consumer5"
        consumers[consumer4] = consumers[consumer3]!!
        consumers.remove(consumer3)
        consumers[consumer5] = consumers[consumer2]!!
        consumers.remove(consumer2)
        val newStaticAssignment = checkStaticAssignment(
            assignor = assignor,
            partitionsPerTopic = partitionsPerTopic,
            consumers = consumers,
        )
        assertEquals(staticAssignment, newStaticAssignment)
    }

    private fun setupPartitionsPerTopicWithTwoTopics(
        numberOfPartitions1: Int,
        numberOfPartitions2: Int,
    ): Map<String, Int> = mapOf(
        topic1 to numberOfPartitions1,
        topic2 to numberOfPartitions2,
    )

    companion object {

        private fun checkStaticAssignment(
            assignor: AbstractPartitionAssignor,
            partitionsPerTopic: Map<String, Int>,
            consumers: Map<String, Subscription>,
        ): Map<String, List<TopicPartition>> {
            val assignmentByMemberId = assignor.assign(partitionsPerTopic, consumers)
            return consumers.entries
                .filter { (_, subscription) -> subscription.groupInstanceId != null }
                .associate { (key, value) -> value.groupInstanceId!! to assignmentByMemberId[key]!! }
        }
    }
}
