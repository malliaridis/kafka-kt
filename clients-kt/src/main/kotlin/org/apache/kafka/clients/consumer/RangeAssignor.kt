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

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
import org.apache.kafka.common.TopicPartition
import kotlin.math.min

/**
 *
 * The range assignor works on a per-topic basis. For each topic, we lay out the available
 * partitions in numeric order and the consumers in lexicographic order. We then divide the number
 * of partitions by the total number of consumers to determine the number of partitions to assign to
 * each consumer. If it does not evenly divide, then the first few consumers will have one extra
 * partition.
 *
 * For example, suppose there are two consumers `C0` and `C1`, two topics `t0` and `t1`, and each
 * topic has 3 partitions, resulting in partitions `t0p0`, `t0p1`, `t0p2`, `t1p0`, `t1p1`, and
 * `t1p2`.
 *
 * The assignment will be:
 * - `C0: [t0p0, t0p1, t1p0, t1p1]`
 * - `C1: [t0p2, t1p2]`
 *
 * Since the introduction of static membership, we could leverage `group.instance.id` to make the
 * assignment behavior more sticky. For the above example, after one rolling bounce, group
 * coordinator will attempt to assign new `member.id` towards consumers, for example `C0` -&gt; `C3`
 * `C1` -&gt; `C2`.
 *
 * The assignment could be completely shuffled to:
 * - `C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])`
 * - `C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])`
 *
 * The assignment change was caused by the change of `member.id` relative order, and can be avoided
 * by setting the group.instance.id.
 *
 * Consumers will have individual instance ids `I1`, `I2`. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * The assignment will always be:
 * - `I0: [t0p0, t0p1, t1p0, t1p1]`
 * - `I1: [t0p2, t1p2]`
 */
class RangeAssignor : AbstractPartitionAssignor() {

    override fun name(): String = RANGE_ASSIGNOR_NAME

    private fun consumersPerTopic(
        consumerMetadata: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): Map<String, MutableList<MemberInfo>> {
        val topicToConsumers = mutableMapOf<String, MutableList<MemberInfo>>()
        for ((consumerId, value) in consumerMetadata) {
            val memberInfo = MemberInfo(consumerId, value.groupInstanceId)
            for (topic in value.topics) put(topicToConsumers, topic, memberInfo)
        }
        return topicToConsumers.toMutableMap()
    }

    override fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): Map<String, MutableList<TopicPartition>> {
        val consumersPerTopic = consumersPerTopic(subscriptions)
        val assignment = subscriptions.keys.associateWith { mutableListOf<TopicPartition>() }

        for ((topic, consumersForTopic) in consumersPerTopic) {
            val numPartitionsForTopic = partitionsPerTopic[topic] ?: continue
            consumersForTopic.sort()

            val numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size
            val consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size
            val partitions = partitions(topic, numPartitionsForTopic)
            var i = 0
            val n = consumersForTopic.size
            while (i < n) {
                val start = numPartitionsPerConsumer * i + min(i, consumersWithExtraPartition)
                val length =
                    numPartitionsPerConsumer + if (i + 1 > consumersWithExtraPartition) 0 else 1
                assignment[consumersForTopic[i].memberId]!!
                    .addAll(partitions.subList(start, start + length))
                i++
            }
        }
        return assignment
    }

    companion object {
        const val RANGE_ASSIGNOR_NAME = "range"
    }
}
