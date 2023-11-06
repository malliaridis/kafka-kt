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
import org.apache.kafka.common.utils.CircularIterator
import java.util.*
import kotlin.collections.ArrayList

/**
 * The round robin assignor lays out all the available partitions and all the available consumers.
 * It then proceeds to do a round robin assignment from partition to consumer. If the subscriptions
 * of all consumer instances are identical, then the partitions will be uniformly distributed.
 * (i.e., the partition ownership counts will be within a delta of exactly one across all consumers.)
 *
 * For example, suppose there are two consumers `C0` and `C1`, two topics `t0` and `t1`, and each
 * topic has 3 partitions, resulting in partitions `t0p0`, `t0p1`, `t0p2`, `t1p0`, `t1p1`, and
 * `t1p2`.
 *
 * The assignment will be:
 * - `C0: [t0p0, t0p2, t1p1]`
 * - `C1: [t0p1, t1p0, t1p2]`
 *
 * When subscriptions differ across consumer instances, the assignment process still considers each
 * consumer instance in round robin fashion but skips over an instance if it is not subscribed to
 * the topic. Unlike the case when subscriptions are identical, this can result in imbalanced
 * assignments. For example, we have three consumers `C0`, `C1`, `C2`, and three topics `t0`, `t1`,
 * `t2`, with 1, 2, and 3 partitions, respectively.
 *
 * Therefore, the partitions are `t0p0`, `t1p0`, `t1p1`, `t2p0`, `t2p1`, `t2p2`. `C0` is subscribed
 * to `t0`; `C1` is subscribed to `t0`, `t1`; and `C2` is subscribed to `t0`, `t1`, `t2`.
 *
 * That assignment will be:
 * - `C0: [t0p0]`
 * - `C1: [t1p0]`
 * - `C2: [t1p1, t2p0, t2p1, t2p2]`
 *
 * Since the introduction of static membership, we could leverage `group.instance.id` to make the
 * assignment behavior more sticky. For example, we have three consumers with assigned `member.id`
 * `C0`, `C1`, `C2`, two topics `t0` and `t1`, and each topic has 3 partitions, resulting in
 * partitions `t0p0`, `t0p1`, `t0p2`, `t1p0`, `t1p1`, and `t1p2`. We choose to honor the sorted
 * order based on ephemeral `member.id`.
 *
 * The assignment will be:
 * - `C0: [t0p0, t1p0]`
 * - `C1: [t0p1, t1p1]`
 * - `C2: [t0p2, t1p2]`
 *
 * After one rolling bounce, group coordinator will attempt to assign new `member.id` towards
 * consumers, for example `C0` -&gt; `C5` `C1` -&gt; `C3`, `C2` -&gt; `C4`.
 *
 * The assignment could be completely shuffled to:
 * - `C3 (was C1): [t0p0, t1p0] (before was [t0p1, t1p1])`
 * - `C4 (was C2): [t0p1, t1p1] (before was [t0p2, t1p2])`
 * - `C5 (was C0): [t0p2, t1p2] (before was [t0p0, t1p0])`
 *
 * This issue could be mitigated by the introduction of static membership. Consumers will have
 * individual instance ids `I1`, `I2`, `I3`. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * The assignment will always be:
 * - `I0: [t0p0, t1p0]`
 * - `I1: [t0p1, t1p1]`
 * - `I2: [t0p2, t1p2]`
 */
class RoundRobinAssignor : AbstractPartitionAssignor() {

    override fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): Map<String, List<TopicPartition>> {
        val assignment = subscriptions.mapValues { mutableListOf<TopicPartition>() }
        val memberInfoList = subscriptions.map { (key, value) ->
            MemberInfo(key, value.groupInstanceId)
        }

        val assigner = CircularIterator(memberInfoList.sorted())
        for (partition in allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            val topic = partition.topic

            while (!subscriptions[assigner.peek().memberId]!!.topics.contains(topic))
                assigner.next()

            assignment[assigner.next().memberId]!!.add(partition)
        }
        return assignment
    }

    private fun allPartitionsSorted(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): List<TopicPartition> {
        val topics: SortedSet<String> = TreeSet()
        for (subscription in subscriptions.values) topics.addAll(subscription.topics)
        val allPartitions: MutableList<TopicPartition> = ArrayList()

        for (topic in topics) {
            val numPartitionsForTopic = partitionsPerTopic[topic]
            if (numPartitionsForTopic != null)
                allPartitions.addAll(partitions(topic, numPartitionsForTopic))
        }
        return allPartitions
    }

    override fun name(): String = ROUNDROBIN_ASSIGNOR_NAME

    companion object {
        const val ROUNDROBIN_ASSIGNOR_NAME = "roundrobin"
    }
}
