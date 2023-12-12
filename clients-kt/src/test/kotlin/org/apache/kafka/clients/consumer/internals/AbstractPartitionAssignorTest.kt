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

import java.util.stream.IntStream
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AbstractPartitionAssignorTest {

    @Test
    fun testMemberInfoSortingWithoutGroupInstanceId() {
        val m1 = MemberInfo("a", null)
        val m2 = MemberInfo("b", null)
        val m3 = MemberInfo("c", null)
        val memberInfoList = listOf(m1, m2, m3)
        assertEquals(memberInfoList, memberInfoList.sorted())
    }

    @Test
    fun testMemberInfoSortingWithAllGroupInstanceId() {
        val m1 = MemberInfo("a", "y")
        val m2 = MemberInfo("b", "z")
        val m3 = MemberInfo("c", "x")
        val memberInfoList = listOf(m1, m2, m3)
        assertEquals(listOf(m3, m1, m2), memberInfoList.sorted())
    }

    @Test
    fun testMemberInfoSortingSomeGroupInstanceId() {
        val m1 = MemberInfo("a", null)
        val m2 = MemberInfo("b", "y")
        val m3 = MemberInfo("c", "x")
        val memberInfoList = listOf(m1, m2, m3)
        assertEquals(listOf(m3, m2, m1), memberInfoList.sorted())
    }

    @Test
    fun testMergeSortManyMemberInfo() {
        val bound = 2
        val memberInfoList = mutableListOf<MemberInfo?>()
        val staticMemberList = mutableListOf<MemberInfo>()
        val dynamicMemberList = mutableListOf<MemberInfo>()
        repeat(100) { i ->
            // Need to make sure all the ids are defined as 3-digits otherwise
            // the comparison result will break.
            val id = (i + 100).toString()
            val groupInstanceId = if (Random.nextInt(bound) < bound / 2) id else null
            val m = MemberInfo(id, groupInstanceId)
            memberInfoList.add(m)
            if (m.groupInstanceId != null) staticMemberList.add(m)
            else dynamicMemberList.add(m)
        }
        staticMemberList.addAll(dynamicMemberList)
        memberInfoList.shuffle()
        assertEquals(staticMemberList, memberInfoList.sortedWith(nullsLast(naturalOrder())))
    }

    @Test
    fun testUseRackAwareAssignment() {
        val assignor: AbstractPartitionAssignor = RangeAssignor()
        val racks = arrayOf("a", "b", "c")
        val allRacks = setOf(*racks)
        val twoRacks = setOf("a", "b")
        val partitionsOnAllRacks: MutableMap<TopicPartition, Set<String>> = HashMap()
        val partitionsOnSubsetOfRacks: MutableMap<TopicPartition, Set<String>> = HashMap()
        for (i in 0..9) {
            val tp = TopicPartition("topic", i)
            partitionsOnAllRacks[tp] = allRacks
            partitionsOnSubsetOfRacks[tp] = setOf(racks[i % racks.size])
        }
        assertFalse(assignor.useRackAwareAssignment(emptySet(), emptySet(), partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(emptySet(), allRacks, partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(allRacks, emptySet(), emptyMap()))
        assertFalse(assignor.useRackAwareAssignment(setOf("d"), allRacks, partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(setOf("a", "d"), allRacks, partitionsOnAllRacks))
        assertTrue(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnSubsetOfRacks))
        assertTrue(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnSubsetOfRacks))
        assertTrue(assignor.useRackAwareAssignment(setOf("a", "d"), allRacks, partitionsOnSubsetOfRacks))

        assignor.preferRackAwareLogic = true
        assertFalse(assignor.useRackAwareAssignment(emptySet(), emptySet(), partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(emptySet(), allRacks, partitionsOnAllRacks))
        assertFalse(assignor.useRackAwareAssignment(allRacks, emptySet(), emptyMap()))
        assertFalse(assignor.useRackAwareAssignment(setOf("d"), allRacks, partitionsOnAllRacks))
        assertTrue(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnAllRacks))
        assertTrue(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnAllRacks))
        assertTrue(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnSubsetOfRacks))
        assertTrue(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnSubsetOfRacks))
    }

    enum class RackConfig {
        NO_BROKER_RACK,
        NO_CONSUMER_RACK,
        BROKER_AND_CONSUMER_RACK,
    }

    companion object {

        const val TEST_NAME_WITH_RACK_CONFIG = "{displayName}.rackConfig = {0}"

        const val TEST_NAME_WITH_CONSUMER_RACK = "{displayName}.hasConsumerRack = {0}"

        val ALL_RACKS = arrayOf("a", "b", "c", "d", "e", "f")

        fun racks(numRacks: Int): List<String> = List(numRacks) { i -> ALL_RACKS[i % ALL_RACKS.size] }

        fun nullRacks(numRacks: Int): List<String?> = List(numRacks) { null }

        fun verifyRackAssignment(
            assignor: AbstractPartitionAssignor,
            numPartitionsPerTopic: Map<String, Int>,
            replicationFactor: Int,
            brokerRacks: List<String?>,
            consumerRacks: List<String?>,
            consumerTopics: List<List<String>>,
            expectedAssignments: List<String>,
            numPartitionsWithRackMismatch: Int,
        ) = verifyRackAssignment(
            assignor = assignor,
            numPartitionsPerTopic = numPartitionsPerTopic,
            replicationFactor = replicationFactor,
            brokerRacks = brokerRacks,
            consumerRacks = consumerRacks,
            consumerTopics = consumerTopics,
            consumerOwnedPartitions = emptyList(),
            expectedAssignments = expectedAssignments,
            numPartitionsWithRackMismatch = numPartitionsWithRackMismatch
        )

        fun verifyRackAssignment(
            assignor: AbstractPartitionAssignor,
            numPartitionsPerTopic: Map<String, Int>,
            replicationFactor: Int,
            brokerRacks: List<String?>,
            consumerRacks: List<String?>,
            consumerTopics: List<List<String>>,
            consumerOwnedPartitions: List<String>? = emptyList(),
            expectedAssignments: List<String>,
            numPartitionsWithRackMismatch: Int,
        ) {
            val consumers = List(consumerRacks.size) { "consumer$it" }
            val subscriptions = subscriptions(
                consumerTopics = consumerTopics,
                consumerRacks = consumerRacks,
                consumerOwnedPartitions = consumerOwnedPartitions,
            )
            val partitionsPerTopic = partitionsPerTopic(
                numPartitionsPerTopic = numPartitionsPerTopic,
                replicationFactor = replicationFactor,
                brokerRacks = brokerRacks,
            )
            val subscriptionsByConsumer = subscriptions.withIndex()
                .associate { (index, sub) -> consumers[index] to sub }

            val expectedAssignment: MutableMap<String, String?> = HashMap(consumers.size)
            for (i in consumers.indices) expectedAssignment[consumers[i]] = expectedAssignments[i]

            val assignment = assignor.assignPartitions(
                partitionsPerTopic = partitionsPerTopic,
                subscriptions = subscriptionsByConsumer,
            )
            val actualAssignment = assignment.mapValues { (_, tp) -> toSortedString(tp) }

            assertEquals(expectedAssignment.toMap(), actualAssignment)

            if (numPartitionsWithRackMismatch >= 0) {
                val numMismatched: MutableList<TopicPartition> = ArrayList()
                for (i in consumers.indices) {
                    val rack = consumerRacks[i]
                    if (rack != null) {
                        val partitions = assignment[consumers[i]]!!
                        for (tp in partitions) {
                            val partitionInfo = partitionsPerTopic[tp.topic]!!
                                .first { info -> info.topic == tp.topic && info.partition == tp.partition }

                            if (partitionInfo.replicas.none { node -> rack == node.rack })
                                numMismatched.add(tp)
                        }
                    }
                }
                assertEquals(
                    expected = numPartitionsWithRackMismatch,
                    actual = numMismatched.size,
                    message = "Partitions with rack mismatch $numMismatched",
                )
            }
        }

        private fun toSortedString(partitions: List<*>): String =
            partitions.map { it.toString() }
                .sorted()
                .joinToString()

        private fun subscriptions(
            consumerTopics: List<List<String>>,
            consumerRacks: List<String?>,
            consumerOwnedPartitions: List<String>?,
        ): List<ConsumerPartitionAssignor.Subscription> {
            val ownedPartitions = ownedPartitions(consumerOwnedPartitions, consumerTopics.size)

            val subscriptions = List(consumerTopics.size) { i ->
                ConsumerPartitionAssignor.Subscription(
                    topics = consumerTopics[i],
                    userData = null,
                    ownedPartitions = ownedPartitions[i],
                    generationId = AbstractStickyAssignor.DEFAULT_GENERATION,
                    rackId = consumerRacks[i],
                )
            }

            return subscriptions
        }

        private fun ownedPartitions(
            consumerOwnedPartitions: List<String>?,
            numConsumers: Int,
        ): List<List<TopicPartition>> {
            val owedPartitions: MutableList<List<TopicPartition>> = ArrayList(numConsumers)
            for (i in 0..<numConsumers) {
                val owned = emptyList<TopicPartition>()
                if (consumerOwnedPartitions == null || consumerOwnedPartitions.size <= i) owedPartitions.add(owned)
                else {
                    val partitions = consumerOwnedPartitions[i].split(", ".toRegex())
                    val topicPartitions: MutableList<TopicPartition> = ArrayList(partitions.size)
                    for (partition in partitions) {
                        val topic = partition.substring(0, partition.lastIndexOf('-'))
                        val p = partition.substring(partition.lastIndexOf('-') + 1).toInt()
                        topicPartitions.add(TopicPartition(topic, p))
                    }
                    owedPartitions.add(topicPartitions)
                }
            }
            return owedPartitions
        }

        private fun partitionsPerTopic(
            numPartitionsPerTopic: Map<String, Int>,
            replicationFactor: Int,
            brokerRacks: List<String?>,
        ): MutableMap<String, MutableList<PartitionInfo>> {
            val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
            var nextIndex = 0
            for ((topic, numPartitions) in numPartitionsPerTopic) {
                partitionsPerTopic[topic] = partitionInfos(
                    topic = topic,
                    numberOfPartitions = numPartitions,
                    replicationFactor = replicationFactor,
                    brokerRacks = brokerRacks,
                    nextNodeIndex = nextIndex,
                )
                nextIndex += numPartitions
            }
            return partitionsPerTopic
        }

        private fun partitionInfos(
            topic: String,
            numberOfPartitions: Int,
            replicationFactor: Int,
            brokerRacks: List<String?>,
            nextNodeIndex: Int,
        ): MutableList<PartitionInfo> {
            val numBrokers = brokerRacks.size
            val nodes: MutableList<Node> = MutableList(numBrokers) { i ->
                Node(i, "", i, brokerRacks[i])
            }

            val partitionInfos: MutableList<PartitionInfo> = MutableList(numberOfPartitions) { i ->
                val replicas = List(replicationFactor) { j ->
                    nodes[(i + j + nextNodeIndex) % nodes.size]
                }
                PartitionInfo(topic, i, replicas[0], replicas, replicas)
            }

            return partitionInfos
        }

        fun partitionInfos(
            topic: String,
            numberOfPartitions: Int,
            replicationFactor: Int,
            numBrokerRacks: Int,
            nextNodeIndex: Int,
        ): MutableList<PartitionInfo> {
            val numBrokers =
                if (numBrokerRacks <= 0) replicationFactor
                else numBrokerRacks * replicationFactor

            val brokerRacks: List<String?> = List(numBrokers) { i ->
                if (numBrokerRacks <= 0) null
                else ALL_RACKS[i % numBrokerRacks]
            }

            return partitionInfos(
                topic = topic,
                numberOfPartitions = numberOfPartitions,
                replicationFactor = replicationFactor,
                brokerRacks = brokerRacks,
                nextNodeIndex = nextNodeIndex,
            )
        }

        fun preferRackAwareLogic(assignor: AbstractPartitionAssignor, value: Boolean) {
            assignor.preferRackAwareLogic = value
        }
    }
}
