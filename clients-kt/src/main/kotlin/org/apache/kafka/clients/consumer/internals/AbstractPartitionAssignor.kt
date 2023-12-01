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
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 */
abstract class AbstractPartitionAssignor : ConsumerPartitionAssignor {

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

    override fun assign(metadata: Cluster, groupSubscription: GroupSubscription): GroupAssignment {
        val subscriptions = groupSubscription.subscriptions
        val allSubscribedTopics: MutableSet<String> = hashSetOf()
        for ((_, value) in subscriptions) allSubscribedTopics.addAll(value.topics)

        val partitionsPerTopic: MutableMap<String, Int> = HashMap()
        for (topic in allSubscribedTopics) {
            val numPartitions = metadata.partitionCountForTopic(topic)
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic[topic] = numPartitions
            else log.debug(
                "Skipping assignment for topic {} since no metadata is available",
                topic,
            )
        }
        val rawAssignments = assign(partitionsPerTopic, subscriptions)

        // this class maintains no user data, so just wrap the results
        val assignments = rawAssignments.mapValues { (_, value) ->
            ConsumerPartitionAssignor.Assignment(value)
        }
        return GroupAssignment(assignments)
    }

    class MemberInfo(
        val memberId: String,
        val groupInstanceId: String?,
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

        internal fun <K, V> put(map: MutableMap<K, MutableList<V>>, key: K, value: V) {
            val list = map.computeIfAbsent(key) { mutableListOf() }
            list.add(value)
        }

        internal fun partitions(topic: String, numPartitions: Int): List<TopicPartition> {
            val partitions: MutableList<TopicPartition> = ArrayList(numPartitions)
            for (i in 0 until numPartitions) partitions.add(TopicPartition(topic, i))
            return partitions
        }
    }
}
