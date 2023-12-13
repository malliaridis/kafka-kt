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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 */
abstract class AbstractPartitionAssignor : ConsumerPartitionAssignor {

    // Used only in unit tests to verify rack-aware assignment when all racks have all partitions.
    var preferRackAwareLogic = false

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     *
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in
     * metadata will be excluded from this map.
     * @param subscriptions Map from the member id to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    abstract fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>>

    /**
     * Default implementation of assignPartitions() that does not include racks. This is only
     * included to avoid breaking any custom implementation that extends AbstractPartitionAssignor.
     * Note that this class is internal, but to be safe, we are maintaining compatibility.
     */
    open fun assignPartitions(
        partitionsPerTopic: MutableMap<String, MutableList<PartitionInfo>>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        val partitionCountPerTopic = partitionsPerTopic.mapValues { it.value.size }
        return assign(partitionCountPerTopic, subscriptions)
    }

    override fun assign(metadata: Cluster, groupSubscription: GroupSubscription): GroupAssignment {
        val subscriptions = groupSubscription.subscriptions
        val allSubscribedTopics: MutableSet<String> = hashSetOf()
        for (subscription in subscriptions)
            allSubscribedTopics.addAll(subscription.value.topics)

        val partitionsPerTopic = mutableMapOf<String, MutableList<PartitionInfo>>()
        for (topic in allSubscribedTopics) {
            var partitions = metadata.partitionsForTopic(topic)
            if (partitions.isNotEmpty()) {
                partitions = partitions.toMutableList()
                partitions.sortWith(Comparator.comparingInt { it.partition })
                partitionsPerTopic[topic] = partitions
            } else log.debug("Skipping assignment for topic {} since no metadata is available", topic)
        }

        val rawAssignments = assignPartitions(partitionsPerTopic, subscriptions)

        // this class maintains no user data, so just wrap the results
        val assignments = rawAssignments.mapValues { (_, value) ->
            ConsumerPartitionAssignor.Assignment(value)
        }
        return GroupAssignment(assignments)
    }

    internal fun useRackAwareAssignment(
        consumerRacks: Set<String>,
        partitionRacks: Set<String>,
        racksPerPartition: Map<TopicPartition, Set<String>>,
    ): Boolean {
        return if (consumerRacks.isEmpty() || consumerRacks.intersect(partitionRacks).isEmpty()) false
        else if (preferRackAwareLogic) true
        else !racksPerPartition.values.stream().allMatch { o -> partitionRacks == o }
    }

    class MemberInfo(
        val memberId: String,
        val groupInstanceId: String?,
        val rackId: String? = null,
    ) : Comparable<MemberInfo> {

        override operator fun compareTo(other: MemberInfo): Int {
            return if (groupInstanceId != null && other.groupInstanceId != null)
                groupInstanceId.compareTo(other.groupInstanceId)
            else if (groupInstanceId != null) -1
            else if (other.groupInstanceId != null) 1
            else memberId.compareTo(other.memberId)
        }

        override fun equals(other: Any?): Boolean {
            return other is MemberInfo && memberId == other.memberId
        }

        /**
         * We could just use member.id to be the hashcode, since it's unique
         * across the group.
         */
        override fun hashCode(): Int = memberId.hashCode()

        override fun toString(): String =
            "MemberInfo [member.id: $memberId, group.instance.id: ${(groupInstanceId ?: "{}")}]"
    }

    companion object {

        private val log: Logger = LoggerFactory.getLogger(AbstractPartitionAssignor::class.java)

        private val NO_NODES = listOf(Node.noNode())

        internal fun <K, V> put(map: MutableMap<K, MutableList<V>>, key: K, value: V) {
            val list = map.computeIfAbsent(key) { mutableListOf() }
            list.add(value)
        }

        internal fun partitions(topic: String, numPartitions: Int): List<TopicPartition> {
            return List(numPartitions) { i -> TopicPartition(topic, i) }
        }

        internal fun partitionInfosWithoutRacks(
            partitionsPerTopic: Map<String, Int>,
        ): MutableMap<String, MutableList<PartitionInfo>> =
            partitionsPerTopic.mapValues { (topic, numPartitions) ->
                MutableList(numPartitions) { index ->
                    PartitionInfo(
                        topic = topic,
                        partition = index,
                        leader = Node.noNode(),
                        replicas = NO_NODES,
                        inSyncReplicas = NO_NODES,
                    )
                }
            }.toMutableMap()
    }
}
