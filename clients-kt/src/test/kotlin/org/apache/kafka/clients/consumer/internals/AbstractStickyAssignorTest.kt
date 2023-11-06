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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.CollectionUtils.groupPartitionsByTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.nio.ByteBuffer
import kotlin.math.abs
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

    protected var tp0 = tp(topic, 0)

    protected var tp1 = tp(topic, 1)

    protected var tp2 = tp(topic, 2)

    protected var groupId = "group"

    protected var generationId = 1

    protected abstract fun createAssignor(): AbstractStickyAssignor

    // simulate ConsumerProtocolSubscription V0 protocol
    protected abstract fun buildSubscriptionV0(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): Subscription

    // simulate ConsumerProtocolSubscription V1 protocol
    protected abstract fun buildSubscriptionV1(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generationId: Int,
    ): Subscription

    // simulate ConsumerProtocolSubscription V2 or above protocol
    protected abstract fun buildSubscriptionV2Above(
        topics: List<String>,
        partitions: List<TopicPartition>,
        generation: Int,
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
        val topics = topics(topic)
        val ownedPartitions = listOf(tp(topic1, 0), tp(topic2, 1))
        val subscriptions = mutableListOf<Subscription>()
        // add subscription in all ConsumerProtocolSubscription versions
        subscriptions.add(buildSubscriptionV0(topics, ownedPartitions, generationId))
        subscriptions.add(buildSubscriptionV1(topics, ownedPartitions, generationId))
        subscriptions.add(buildSubscriptionV2Above(topics, ownedPartitions, generationId))
        for (subscription in subscriptions) {
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

    @Test
    fun testOneConsumerNoTopic() {
        val partitionsPerTopic: Map<String, Int> = HashMap()
        subscriptions = mutableMapOf(consumerId to Subscription(emptyList()))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(assignment[consumerId]!!.isEmpty())
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOneConsumerNonexistentTopic() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 0
        subscriptions = mutableMapOf(consumerId to Subscription(topics(topic)))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(assignment[consumerId]!!.isEmpty())
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOneConsumerOneTopic() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        subscriptions = mutableMapOf(consumerId to Subscription(topics(topic)))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(tp(topic, 0), tp(topic, 1), tp(topic, 2)),
            actual = assignment[consumerId]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOnlyAssignsPartitionsFromSubscribedTopics() {
        val otherTopic = "other"
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 2
        subscriptions = mutableMapOf(
            consumerId to buildSubscriptionV2Above(
                topics = topics(topic),
                partitions = listOf(
                    tp(topic, 0),
                    tp(topic, 1),
                    tp(otherTopic, 0),
                    tp(otherTopic, 1),
                ),
                generation = generationId,
            ),
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(tp(topic, 0), tp(topic, 1)),
            actual = assignment[consumerId]!!
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOneConsumerMultipleTopics() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 1
        partitionsPerTopic[topic2] = 2
        subscriptions = mutableMapOf(
            consumerId to Subscription(topics(topic1, topic2)),
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)),
            actual = assignment[consumerId]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testTwoConsumersOneTopicOnePartition() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 1
        subscriptions[consumer1] = Subscription(topics(topic))
        subscriptions[consumer2] = Subscription(topics(topic))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testTwoConsumersOneTopicTwoPartitions() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 2
        subscriptions[consumer1] = Subscription(topics(topic))
        subscriptions[consumer2] = Subscription(topics(topic))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(listOf(tp(topic, 0)), assignment[consumer1]!!)
        assertEquals(listOf(tp(topic, 1)), assignment[consumer2]!!)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testMultipleConsumersMixedTopicSubscriptions() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 3
        partitionsPerTopic[topic2] = 2
        subscriptions[consumer1] = Subscription(topics(topic1))
        subscriptions[consumer2] = Subscription(topics(topic1, topic2))
        subscriptions[consumer3] = Subscription(topics(topic1))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(listOf(tp(topic1, 0), tp(topic1, 2)), assignment[consumer1]!!)
        assertEquals(listOf(tp(topic2, 0), tp(topic2, 1)), assignment[consumer2]!!)
        assertEquals(listOf(tp(topic1, 1)), assignment[consumer3]!!)
        assertNull(assignor.partitionsTransferringOwnership)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testTwoConsumersTwoTopicsSixPartitions() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 3
        partitionsPerTopic[topic2] = 3
        subscriptions[consumer1] = Subscription(topics(topic1, topic2))
        subscriptions[consumer2] = Subscription(topics(topic1, topic2))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)),
            actual = assignment[consumer2]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test is testing consumer owned minQuota partitions, and expected to have maxQuota partitions situation
     */
    @Test
    fun testConsumerOwningMinQuotaExpectedMaxQuota() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 2
        partitionsPerTopic[topic2] = 3
        val subscribedTopics = topics(topic1, topic2)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 0), tp(topic2, 1)),
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 1), tp(topic2, 2)),
            generation = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(tp(topic1, 0), tp(topic2, 1), tp(topic2, 0)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic1, 1), tp(topic2, 2)),
            actual = assignment[consumer2]!!,
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test is testing consumers owned maxQuota partitions are more than numExpectedMaxCapacityMembers situation
     */
    @Test
    fun testMaxQuotaConsumerMoreThanNumExpectedMaxCapacityMembers() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 2
        partitionsPerTopic[topic2] = 2
        val subscribedTopics = topics(topic1, topic2)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 0), tp(topic2, 0)),
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 1), tp(topic2, 1)),
            generation = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generation = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = mapOf(tp(topic2, 0) to consumer3),
            actual = assignor.partitionsTransferringOwnership!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(tp(topic1, 0)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic1, 1), tp(topic2, 1)),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(tp(topic2, 0)),
            actual = assignment[consumer3]!!,
        )
        assertTrue(isFullyBalanced(assignment))
    }

    /**
     * This unit test is testing all consumers owned less than minQuota partitions situation
     */
    @Test
    fun testAllConsumersAreUnderMinQuota() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 2
        partitionsPerTopic[topic2] = 3
        val subscribedTopics = topics(topic1, topic2)
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 0)),
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = listOf(tp(topic1, 1)),
            generation = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = subscribedTopics,
            partitions = emptyList(),
            generation = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(tp(topic1, 0), tp(topic2, 1)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic1, 1), tp(topic2, 2)),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(tp(topic2, 0)),
            actual = assignment[consumer3]!!,
        )
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testAddRemoveConsumerOneTopic() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        subscriptions[consumer1] = Subscription(topics(topic))
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(tp(topic, 0), tp(topic, 1), tp(topic, 2)),
            actual = assignment[consumer1]!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = assignment[consumer1]!!,
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = emptyList(),
            generation = generationId,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = mapOf(tp(topic, 2) to consumer2),
            actual = assignor.partitionsTransferringOwnership!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(tp(topic, 0), tp(topic, 1)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic, 2)),
            actual = assignment[consumer2]!!,
        )
        assertTrue(isFullyBalanced(assignment))

        subscriptions.remove(consumer1)
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = assignment[consumer2]!!,
            generation = generationId,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = setOf(tp(topic, 2), tp(topic, 1), tp(topic, 0)),
            actual = assignment[consumer2]!!.toSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testAddRemoveTwoConsumersTwoTopics() {
        val allTopics = topics(topic1, topic2)
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 3
        partitionsPerTopic[topic2] = 4
        subscriptions[consumer1] = Subscription(allTopics)
        subscriptions[consumer2] = Subscription(allTopics)
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                tp(topic1, 0),
                tp(topic1, 2),
                tp(topic2, 1),
                tp(topic2, 3),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)),
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
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = assignment[consumer2]!!,
            generation = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = emptyList(),
            generation = generationId,
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = emptyList(),
            generation = generationId,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        val expectedPartitionsTransferringOwnership = mutableMapOf<TopicPartition, String>()
        expectedPartitionsTransferringOwnership[tp(topic2, 1)] = consumer3
        expectedPartitionsTransferringOwnership[tp(topic2, 3)] = consumer3
        expectedPartitionsTransferringOwnership[tp(topic2, 2)] = consumer4

        assertEquals(expectedPartitionsTransferringOwnership, assignor.partitionsTransferringOwnership)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            listOf(tp(topic1, 0), tp(topic1, 2)),
            assignment[consumer1]!!
        )
        assertEquals(
            listOf(tp(topic1, 1), tp(topic2, 0)),
            assignment[consumer2]!!
        )
        assertEquals(
            listOf(tp(topic2, 1), tp(topic2, 3)),
            assignment[consumer3]!!
        )
        assertEquals(
            listOf(tp(topic2, 2)),
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
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = allTopics,
            partitions = assignment[consumer4]!!,
            generation = generationId,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = listOf(
                tp(topic2, 1),
                tp(topic2, 3),
                tp(topic1, 0),
                tp(topic2, 0),
            ),
            actual = assignment[consumer3]!!,
        )
        assertEquals(
            expected = listOf(
                tp(topic2, 2),
                tp(topic1, 1),
                tp(topic1, 2),
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
    @Test
    fun testPoorRoundRobinAssignmentScenario() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        for (i in 1..5) partitionsPerTopic[String.format("topic%d", i)] = (i % 2) + 1
        subscriptions["consumer1"] = Subscription(topics("topic1", "topic2", "topic3", "topic4", "topic5"))
        subscriptions["consumer2"] = Subscription(topics("topic1", "topic3", "topic5"))
        subscriptions["consumer3"] = Subscription(topics("topic1", "topic3", "topic5"))
        subscriptions["consumer4"] = Subscription(topics("topic1", "topic2", "topic3", "topic4", "topic5"))
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @Test
    fun testAddRemoveTopicTwoConsumers() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        subscriptions[consumer1] = Subscription(topics(topic))
        subscriptions[consumer2] = Subscription(topics(topic))
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        // verify balance
        assertTrue(isFullyBalanced(assignment))
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        // verify stickiness
        val consumer1Assignment1 = assignment[consumer1]!!
        val consumer2Assignment1 = assignment[consumer2]!!

        assertTrue(
            consumer1Assignment1.size == 1 && consumer2Assignment1.size == 2 || consumer1Assignment1.size == 2 && consumer2Assignment1.size == 1
        )

        partitionsPerTopic[topic2] = 3
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = assignment[consumer1]!!,
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = assignment[consumer2]!!,
            generation = generationId,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

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
            topics = topics(topic2),
            partitions = assignment[consumer1]!!,
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic2),
            partitions = assignment[consumer2]!!,
            generation = generationId,
        )
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

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

    @Test
    fun testReassignmentAfterOneConsumerLeaves() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        for (i in 1..19) partitionsPerTopic[getTopicName(i, 20)] = i
        for (i in 1..19) {
            val topics = mutableListOf<String>()
            for (j in 1..i) topics.add(getTopicName(j, 20))
            subscriptions[getConsumerName(i, 20)] = Subscription(topics)
        }
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)
        
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        
        for (i in 1..19) {
            val consumer = getConsumerName(i, 20)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = subscriptions[consumer]!!.topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
            )
        }
        subscriptions.remove("consumer10")
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(assignor.isSticky)
    }

    @Test
    fun testReassignmentAfterOneConsumerAdded() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic["topic"] = 20
        for (i in 1..9) subscriptions[getConsumerName(i, 10)] = Subscription(
            topics("topic")
        )
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)
        
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        // add a new consumer
        subscriptions[getConsumerName(10, 10)] = Subscription(topics("topic"))
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @Test
    fun testSameSubscriptions() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        for (i in 1..14) partitionsPerTopic[getTopicName(i, 15)] = i
        for (i in 1..8) {
            val topics = mutableListOf<String>()
            for (j in 1..partitionsPerTopic.size) topics.add(getTopicName(j, 15))
            subscriptions[getConsumerName(i, 9)] = Subscription(topics)
        }
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)
        
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        
        for (i in 1..8) {
            val consumer = getConsumerName(i, 9)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = subscriptions[consumer]!!.topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
            )
        }
        subscriptions.remove(getConsumerName(5, 9))
        assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @Timeout(30)
    @Test
    fun testLargeAssignmentAndGroupWithUniformSubscription() {
        // 1 million partitions!
        val topicCount = 500
        val partitionCount = 2000
        val consumerCount = 2000
        val topics = mutableListOf<String>()
        val partitionsPerTopic = mutableMapOf<String, Int>()
        
        repeat(topicCount) { i ->
            val topicName = getTopicName(i, topicCount)
            topics.add(topicName)
            partitionsPerTopic[topicName] = partitionCount
        }
        repeat(consumerCount) { i ->
            subscriptions[getConsumerName(i, consumerCount)] = Subscription(topics)
        }
        
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        for (i in 1 until consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
            )
        }
        assignor.assign(partitionsPerTopic, subscriptions)
    }

    @Timeout(90)
    @Test
    fun testLargeAssignmentAndGroupWithNonEqualSubscription() {
        // 1 million partitions!
        val topicCount = 500
        val partitionCount = 2000
        val consumerCount = 2000
        val topics = mutableListOf<String>()
        val partitionsPerTopic = mutableMapOf<String, Int>()
        repeat(topicCount) { i ->
            val topicName = getTopicName(i, topicCount)
            topics.add(topicName)
            partitionsPerTopic[topicName] = partitionCount
        }
        repeat(consumerCount) { i ->
            if (i == consumerCount - 1)
                subscriptions[getConsumerName(i, consumerCount)] = Subscription(topics.subList(0, 1))
            else subscriptions[getConsumerName(i, consumerCount)] = Subscription(topics)
        }
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        
        for (i in 1 until consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            if (i == consumerCount - 1) subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics.subList(0, 1),
                partitions = assignment[consumer]!!,
                generation = generationId,
            )
            else subscriptions[consumer] = buildSubscriptionV2Above(
                topics = topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
            )
        }
        assignor.assign(partitionsPerTopic, subscriptions)
    }

    @Test
    fun testLargeAssignmentWithMultipleConsumersLeavingAndRandomSubscription() {
        val topicCount = 40
        val consumerCount = 200
        val partitionsPerTopic = mutableMapOf<String, Int>()
        for (i in 0 until topicCount) partitionsPerTopic[getTopicName(i, topicCount)] = Random.nextInt(10) + 1
        for (i in 0 until consumerCount) {
            val topics = mutableListOf<String>()
            repeat(Random.nextInt(20)) {
                topics.add(getTopicName(Random.nextInt(topicCount), topicCount))
            }
            subscriptions[getConsumerName(i, consumerCount)] = Subscription(topics)
        }
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        for (i in 1 until consumerCount) {
            val consumer = getConsumerName(i, consumerCount)
            subscriptions[consumer] = buildSubscriptionV2Above(
                topics = subscriptions[consumer]!!.topics,
                partitions = assignment[consumer]!!,
                generation = generationId,
            )
        }
        repeat(50) {
            val c = getConsumerName(Random.nextInt(consumerCount), consumerCount)
            subscriptions.remove(c)
        }
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(assignor.isSticky)
    }

    @Test
    fun testNewSubscription() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        for (i in 1..4) partitionsPerTopic[getTopicName(i, 5)] = 1
        for (i in 0..2) {
            val topics = mutableListOf<String>()
            for (j in i..3 * i - 2) topics.add(getTopicName(j, 5))
            subscriptions[getConsumerName(i, 3)] = Subscription(topics)
        }
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

        subscriptions[getConsumerName(0, 3)]!!.topics.add(getTopicName(1, 5))
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(assignor.isSticky)
    }

    @Test
    fun testMoveExistingAssignments() {
        val topic4 = "topic4"
        val topic5 = "topic5"
        val topic6 = "topic6"
        val partitionsPerTopic = mutableMapOf<String, Int>()
        for (i in 1..6) partitionsPerTopic[String.format("topic%d", i)] = 1

        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic1, topic2),
            partitions = listOf(tp(topic1, 0)),
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic1, topic2, topic3, topic4),
            partitions = listOf(tp(topic2, 0), tp(topic3, 0)),
            generation = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic2, topic3, topic4, topic5, topic6),
            partitions = listOf(
                tp(topic4, 0),
                tp(topic5, 0),
                tp(topic6, 0),
            ),
            generation = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertNull(assignor.partitionsTransferringOwnership)
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
    }

    @Test
    fun testStickiness() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 3
        subscriptions[consumer1] = Subscription(topics(topic1))
        subscriptions[consumer2] = Subscription(topics(topic1))
        subscriptions[consumer3] = Subscription(topics(topic1))
        subscriptions[consumer4] = Subscription(topics(topic1))
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)

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
        subscriptions[consumer2] = buildSubscriptionV2Above(topics(topic1), assignment[consumer2]!!, generationId)
        subscriptions[consumer3] = buildSubscriptionV2Above(topics(topic1), assignment[consumer3]!!, generationId)
        subscriptions[consumer4] = buildSubscriptionV2Above(topics(topic1), assignment[consumer4]!!, generationId)
        assignment = assignor.assign(partitionsPerTopic, subscriptions)

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

    @Test
    fun testAssignmentUpdatedForDeletedTopic() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic1] = 1
        partitionsPerTopic[topic3] = 100
        subscriptions = mutableMapOf(consumerId to Subscription(topics(topic1, topic2, topic3))
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        assertEquals(
            expected = assignment.values.stream().mapToInt { it.size }.sum(),
            actual = 1 + 100,
        )
        assertEquals(setOf(consumerId), assignment.keys)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testNoExceptionThrownWhenOnlySubscribedTopicDeleted() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        subscriptions[consumerId] = Subscription(topics(topic))
        var assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())

        subscriptions[consumerId] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = assignment[consumerId]!!,
            generation = generationId,
        )
        assignment = assignor.assign(emptyMap(), subscriptions)

        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        assertEquals(assignment.size, 1)
        assertTrue(assignment[consumerId]!!.isEmpty())
    }

    @Test
    fun testReassignmentWithRandomSubscriptionsAndChanges() {
        val minNumConsumers = 20
        val maxNumConsumers = 40
        val minNumTopics = 10
        val maxNumTopics = 20
        for (round in 1..100) {
            val numTopics = minNumTopics + Random.nextInt(maxNumTopics - minNumTopics)
            val topics = ArrayList<String>()
            val partitionsPerTopic = mutableMapOf<String, Int>()
            for (i in 0 until numTopics) {
                topics.add(getTopicName(i, maxNumTopics))
                partitionsPerTopic[getTopicName(i, maxNumTopics)] = i + 1
            }
            val numConsumers = minNumConsumers + Random.nextInt(maxNumConsumers - minNumConsumers)
            for (i in 0 until numConsumers) {
                val sub = getRandomSublist(topics).sorted()
                subscriptions[getConsumerName(i, maxNumConsumers)] = Subscription(sub)
            }
            assignor = createAssignor()
            var assignment = assignor.assign(partitionsPerTopic, subscriptions)

            verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)

            subscriptions.clear()

            for (i in 0 until numConsumers) {
                val sub = getRandomSublist(topics).sorted()
                val consumer = getConsumerName(i, maxNumConsumers)
                subscriptions[consumer] = buildSubscriptionV2Above(
                    topics = sub,
                    partitions = assignment[consumer]!!,
                    generation = generationId,
                )
            }
            assignment = assignor.assign(partitionsPerTopic, subscriptions)

            verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
            assertTrue(assignor.isSticky)
        }
    }

    @Test
    fun testAllConsumersReachExpectedQuotaAndAreConsideredFilled() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 4
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 1)),
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 2)),
            generation = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = emptyList(),
            generation = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(listOf(tp(topic, 0), tp(topic, 1)), actual = assignment[consumer1]!!)
        assertEquals(listOf(tp(topic, 2)), assignment[consumer2]!!)
        assertEquals(listOf(tp(topic, 3)), assignment[consumer3]!!)
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOwnedPartitionsAreInvalidatedForConsumerWithStaleGeneration() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        partitionsPerTopic[topic2] = 3
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = listOf(
                tp(topic, 0),
                tp(topic, 2),
                tp(topic2, 1),
            ),
            generation = currentGeneration,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = listOf(
                tp(topic, 0),
                tp(topic, 2),
                tp(topic2, 1),
            ),
            generation = currentGeneration - 1,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = setOf(
                tp(topic, 0),
                tp(topic, 2),
                tp(topic2, 1),
            ),
            actual = assignment[consumer1]!!.toSet(),
        )
        assertEquals(
            expected = setOf(
                tp(topic, 1),
                tp(topic2, 0),
                tp(topic2, 2),
            ),
            actual = assignment[consumer2]!!.toSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOwnedPartitionsAreInvalidatedForConsumerWithNoGeneration() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        partitionsPerTopic[topic2] = 3
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = listOf(
                tp(topic, 0),
                tp(topic, 2),
                tp(topic2, 1),
            ),
            generation = currentGeneration,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = listOf(
                tp(topic, 0),
                tp(topic, 2),
                tp(topic2, 1),
            ),
            generation = AbstractStickyAssignor.DEFAULT_GENERATION,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                tp(topic, 0),
                tp(topic, 2),
                tp(topic2, 1),
            ),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = setOf(
                tp(topic, 1),
                tp(topic2, 0),
                tp(topic2, 2),
            ),
            actual = assignment[consumer2]!!.toSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testPartitionsTransferringOwnershipIncludeThePartitionClaimedByMultipleConsumersInSameGeneration() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3

        // partition topic-0 is owned by multiple consumer
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 1)),
            generation = generationId,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = listOf(tp(topic, 0), tp(topic, 2)),
            generation = generationId,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic),
            partitions = emptyList<TopicPartition>(),
            generation = generationId,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        // we should include the partitions claimed by multiple consumers in partitionsTransferringOwnership
        assertEquals(
            expected = mapOf(tp(topic, 0) to consumer3),
            actual = assignor.partitionsTransferringOwnership!!,
        )
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertEquals(
            expected = listOf(tp(topic, 1)),
            actual = assignment[consumer1]!!,
        )
        assertEquals(
            expected = listOf(tp(topic, 2)),
            actual = assignment[consumer2]!!,
        )
        assertEquals(
            expected = listOf(tp(topic, 0)),
            actual = assignment[consumer3]!!,
        )
        assertTrue(isFullyBalanced(assignment))
    }

    fun testEnsurePartitionsAssignedToHighestGeneration() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        partitionsPerTopic[topic2] = 3
        partitionsPerTopic[topic3] = 3
        val currentGeneration = 10

        // ensure partitions are always assigned to the member with the highest generation
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3),
            partitions = listOf(
                tp(topic, 0),
                tp(topic2, 0),
                tp(topic3, 0),
            ),
            generation = currentGeneration,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3),
            partitions = listOf(
                tp(topic, 1),
                tp(topic2, 1),
                tp(topic3, 1),
            ),
            generation = currentGeneration - 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3),
            partitions = listOf(
                tp(topic2, 1),
                tp(topic3, 0),
                tp(topic3, 2),
            ),
            generation = currentGeneration - 2,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        assertEquals(
            expected = setOf(tp(topic, 0), tp(topic2, 0), tp(topic3, 0)),
            actual = assignment[consumer1]!!.toSet(),
        )
        assertEquals(
            expected = setOf(tp(topic, 1), tp(topic2, 1), tp(topic3, 1)),
            actual = assignment[consumer2]!!.toSet()
        )
        assertEquals(
            expected = setOf(tp(topic, 2), tp(topic2, 2), tp(topic3, 2)),
            actual = assignment[consumer3]!!.toSet(),
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    fun testNoReassignmentOnCurrentMembers() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        partitionsPerTopic[topic1] = 3
        partitionsPerTopic[topic2] = 3
        partitionsPerTopic[topic3] = 3
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3, topic1),
            partitions = listOf(),
            generation = AbstractStickyAssignor.DEFAULT_GENERATION,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3, topic1),
            partitions = listOf(
                tp(topic, 0),
                tp(topic2, 0),
                tp(topic1, 0),
            ),
            generation = currentGeneration - 1,
        )
        subscriptions[consumer3] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3, topic1),
            partitions = listOf(
                tp(topic3, 2),
                tp(topic2, 2),
                tp(topic1, 1),
            ),
            generation = currentGeneration - 2,
        )
        subscriptions[consumer4] = buildSubscriptionV2Above(
            topics = topics(topic, topic2, topic3, topic1),
            partitions = listOf(
                tp(topic3, 1),
                tp(topic, 1),
                tp(topic, 2),
            ),
            generation = currentGeneration - 3,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)

        // ensure assigned partitions don't get reassigned
        assertEquals(
            expected = listOf(tp(topic1, 2), tp(topic2, 1), tp(topic3, 0)).toSet(),
            actual = assignment[consumer1]!!.toSet()
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
    }

    @Test
    fun testOwnedPartitionsAreInvalidatedForConsumerWithMultipleGeneration() {
        val partitionsPerTopic = mutableMapOf<String, Int>()
        partitionsPerTopic[topic] = 3
        partitionsPerTopic[topic2] = 3
        val currentGeneration = 10
        subscriptions[consumer1] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = listOf(
                tp(topic, 0),
                tp(topic2, 1),
                tp(topic, 1),
            ),
            generation = currentGeneration,
        )
        subscriptions[consumer2] = buildSubscriptionV2Above(
            topics = topics(topic, topic2),
            partitions = listOf(
                tp(topic, 0),
                tp(topic2, 1),
                tp(topic2, 2),
            ),
            generation = currentGeneration - 2,
        )
        val assignment = assignor.assign(partitionsPerTopic, subscriptions)
        assertEquals(
            expected = listOf(
                tp(topic, 0),
                tp(topic2, 1),
                tp(topic, 1),
            ).toSet(),
            actual = assignment[consumer1]!!.toSet(),
        )
        assertEquals(
            expected = listOf(
                tp(topic, 2),
                tp(topic2, 2),
                tp(topic2, 0),
            ).toSet(),
            actual = assignment[consumer2]!!.toSet()
        )
        assertTrue(assignor.partitionsTransferringOwnership!!.isEmpty())
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic)
        assertTrue(isFullyBalanced(assignment))
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
    protected open fun verifyValidityAndBalance(
        subscriptions: Map<String, Subscription>,
        assignments: Map<String, List<TopicPartition>>,
        partitionsPerTopic: Map<String, Int>?,
    ) {
        val size = subscriptions.size
        assert(size == assignments.size)
        val consumers = assignments.keys.sorted()
        for(i in 0 until size) {
            val consumer = consumers[i]
            val partitions = assignments[consumer]!!
            for (partition in partitions) assertTrue(
                actual = subscriptions[consumer]!!.topics.contains(partition.topic),
                message = "Error: Partition ${partition}is assigned to c$i, but it is not subscribed to " +
                        "Topic t${partition.topic}\nSubscriptions: $subscriptions\nAssignments: $assignments",
            )
            if (i == size - 1) continue
            for (j in i + 1 until size) {
                val otherConsumer = consumers[j]
                val otherPartitions = assignments[otherConsumer]!!
                val intersection = partitions.toMutableSet()
                intersection.retainAll(otherPartitions.toSet())
                assertTrue(
                    actual = intersection.isEmpty(),
                    message = "Error: Consumers c$i and c$j have common partitions assigned to them: " +
                            "$intersection\nSubscriptions: $subscriptions\nAssignments: $assignments"
                )
                val len = partitions.size
                val otherLen = otherPartitions.size
                if (abs((len - otherLen).toDouble()) <= 1) continue
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

    protected fun memberData(subscription: Subscription?): AbstractStickyAssignor.MemberData {
        return assignor.memberData((subscription)!!)
    }

    companion object {

        internal fun topics(vararg topics: String): List<String> = topics.toList()

        @Deprecated("Use listOf() instead")
        internal fun partitions(vararg partitions: TopicPartition): List<TopicPartition>  = partitions.toList()

        internal fun tp(topic: String, partition: Int): TopicPartition = TopicPartition(topic, partition)

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
