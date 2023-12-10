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
import java.util.Collections
import java.util.Objects
import java.util.Optional
import java.util.function.BinaryOperator
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import kotlin.math.max

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
 *
 * Rack-aware assignment is used if both consumer and partition replica racks are available and
 * some partitions have replicas only on a subset of racks. We attempt to match consumer racks with
 * partition replica racks on a best-effort basis, prioritizing balanced assignment over rack alignment.
 * Topics with equal partition count and same set of subscribers guarantee co-partitioning by prioritizing
 * co-partitioning over rack alignment. In this case, aligning partition replicas of these topics on the
 * same racks will improve locality for consumers. For example, if partitions 0 of all topics have a replica
 * on rack 'a', partition 1 on rack 'b' etc., partition 0 of all topics can be assigned to a consumer
 * on rack 'a', partition 1 to a consumer on rack 'b' and so on.
 *
 * Note that rack-aware assignment currently takes all replicas into account, including any offline replicas
 * and replicas that are not in the ISR. This is based on the assumption that these replicas are likely
 * to join the ISR relatively soon. Since consumers don't rebalance on ISR change, this avoids unnecessary
 * cross-rack traffic for long durations after replicas rejoin the ISR. In the future, we may consider
 * rebalancing when replicas are added or removed to improve consumer rack alignment.
 */
class RangeAssignor : AbstractPartitionAssignor() {

    override fun name(): String = RANGE_ASSIGNOR_NAME

    private fun consumersPerTopic(
        consumerMetadata: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): Map<String, List<MemberInfo>> {
        val topicToConsumers = mutableMapOf<String, MutableList<MemberInfo>>()
        consumerMetadata.forEach { (consumerId, subscription) ->
            val memberInfo = MemberInfo(
                memberId = consumerId,
                groupInstanceId = subscription.groupInstanceId,
                subscription.rackId,
            )
            subscription.topics.forEach { topic -> put(topicToConsumers, topic, memberInfo) }
        }
        return topicToConsumers.toMutableMap()
    }

    /**
     * Performs range assignment of the specified partitions for the consumers with the provided subscriptions.
     * If rack-awareness is enabled for one or more consumers, we perform rack-aware assignment first to assign
     * the subset of partitions that can be aligned on racks, while retaining the same co-partitioning and
     * per-topic balancing guarantees as non-rack-aware range assignment. The remaining partitions are assigned
     * using standard non-rack-aware range assignment logic, which may result in mis-aligned racks.
     */
    override fun assignPartitions(
        partitionsPerTopic: Map<String, List<PartitionInfo>>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        val consumersPerTopic = consumersPerTopic(subscriptions)
        val consumerRacks = consumerRacks(subscriptions)
        val topicAssignmentStates = partitionsPerTopic
            .filterValues { it.isNotEmpty() }
            .map { (key, value) ->
                TopicAssignmentState(
                    topic = key,
                    partitionInfos = value,
                    membersOrNull = consumersPerTopic[key],
                    consumerRacks = consumerRacks
                )
            }

        val assignment = subscriptions.keys
            .associateWith { mutableListOf<TopicPartition>() }
            .toMutableMap()

        val useRackAware = topicAssignmentStates.any { it.needsRackAwareAssignment }
        if (useRackAware) assignWithRackMatching(topicAssignmentStates, assignment)

        topicAssignmentStates.forEach {
            assignRanges(
                assignmentState = it,
                mayAssign = { _, _ -> true },
                assignment = assignment,
            )
        }

        if (useRackAware) assignment.values.forEach { list -> list.sortWith(PARTITION_COMPARATOR) }
        return assignment
    }

    // This method is not used, but retained for compatibility with any custom assignors that extend this class.
    override fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> =
        assignPartitions(partitionInfosWithoutRacks(partitionsPerTopic), subscriptions)

    private fun assignRanges(
        assignmentState: TopicAssignmentState,
        mayAssign: (String, TopicPartition) -> Boolean,
        assignment: Map<String, MutableList<TopicPartition>>,
    ) {
        for (consumer in assignmentState.consumers.keys) {
            if (assignmentState.unassignedPartitions.isEmpty()) break
            val assignablePartitions = assignmentState.unassignedPartitions
                .filter { mayAssign(consumer, it) }
                .limit(assignmentState.maxAssignable(consumer).toLong())

            if (assignablePartitions.isEmpty()) continue
            assign(
                consumer = consumer,
                partitions = assignablePartitions,
                assignmentState = assignmentState,
                assignment = assignment,
            )
        }
    }

    private fun assignWithRackMatching(
        assignmentStates: Collection<TopicAssignmentState>,
        assignment: Map<String, MutableList<TopicPartition>>,
    ) {
        assignmentStates.groupBy { it.consumers }
            .forEach { (consumers, states) ->
                states.groupBy { it.partitionRacks.size }
                    .forEach { (numPartitions, coPartitionedStates) ->
                        if (coPartitionedStates.size > 1) assignCoPartitionedWithRackMatching(
                            consumers = consumers,
                            numPartitions = numPartitions,
                            assignmentStates = coPartitionedStates,
                            assignment = assignment,
                        ) else {
                            val state = coPartitionedStates[0]
                            if (state.needsRackAwareAssignment) assignRanges(
                                assignmentState = state,
                                mayAssign = { consumer, tp -> state.racksMatch(consumer, tp) },
                                assignment = assignment,
                            )
                        }
                    }
            }
    }

    private fun assignCoPartitionedWithRackMatching(
        consumers: MutableMap<String, String?>,
        numPartitions: Int,
        assignmentStates: Collection<TopicAssignmentState>,
        assignment: Map<String, MutableList<TopicPartition>>,
    ) {
        val remainingConsumers: MutableSet<String> = java.util.LinkedHashSet(consumers.keys)
        for (i in 0..<numPartitions) {
            val matchingConsumer = remainingConsumers.firstOrNull { c ->
                assignmentStates.all {
                    it.racksMatch(c, TopicPartition(it.topic, i)) && it.maxAssignable(c) > 0
                }
            }
            if (matchingConsumer != null) {
                assignmentStates.forEach { state ->
                    assign(
                        consumer = matchingConsumer,
                        partitions = listOf(TopicPartition(state.topic, i)),
                        assignmentState = state,
                        assignment = assignment,
                    )
                }
                if (assignmentStates.none { state -> state.maxAssignable(matchingConsumer) > 0 }) {
                    remainingConsumers.remove(matchingConsumer)
                    if (remainingConsumers.isEmpty()) break
                }
            }
        }
    }

    private fun assign(
        consumer: String,
        partitions: List<TopicPartition>,
        assignmentState: TopicAssignmentState,
        assignment: Map<String, MutableList<TopicPartition>>,
    ) {
        assignment[consumer]!!.addAll(partitions)
        assignmentState.onAssigned(consumer, partitions)
    }

    private fun consumerRacks(subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>): Map<String, String> {
        return subscriptions
            .mapNotNull { (key, subscription) -> subscription.rackId?.let { key to it } }
            .toMap()

    }

    private class TopicAssignmentState(
        val topic: String,
        partitionInfos: List<PartitionInfo>,
        membersOrNull: List<MemberInfo>?,
        consumerRacks: Map<String, String>,
    ) {

        val consumers: MutableMap<String, String?>

        val needsRackAwareAssignment: Boolean

        val partitionRacks: Map<TopicPartition, Set<String>>

        val unassignedPartitions: MutableSet<TopicPartition>

        private val numAssignedByConsumer: MutableMap<String, Int>

        private val numPartitionsPerConsumer: Int

        private var remainingConsumersWithExtraPartition: Int

        init {
            val members = membersOrNull?.sorted() ?: emptyList()
            consumers = members.map { member -> member.memberId }
                .associateWith { memberId -> consumerRacks[memberId] }
                .toMutableMap()

            unassignedPartitions = partitionInfos.map { partitionInfo ->
                TopicPartition(partitionInfo.topic, partitionInfo.partition)
            }.toMutableSet()

            numAssignedByConsumer = consumers.keys.associateWith { 0 }.toMutableMap()

            numPartitionsPerConsumer = if (consumers.isEmpty()) 0 else partitionInfos.size / consumers.size
            remainingConsumersWithExtraPartition = if (consumers.isEmpty()) 0 else partitionInfos.size % consumers.size

            val allConsumerRacks = members.mapNotNull { member -> consumerRacks[member.memberId] }
            val allPartitionRacks = mutableSetOf<String>()

            partitionRacks = if (allConsumerRacks.isNotEmpty()) {
                partitionInfos.associate { partitionInfo ->
                    val tp = TopicPartition(partitionInfo.topic, partitionInfo.partition)
                    val racks = partitionInfo.replicas.mapNotNull { it.rack }.toSet()

                    allPartitionRacks.addAll(racks)
                    tp to racks
                }
            } else emptyMap()

            needsRackAwareAssignment = useRackAwareAssignment(allConsumerRacks, allPartitionRacks, partitionRacks)
        }

        fun racksMatch(consumer: String, tp: TopicPartition?): Boolean {
            val consumerRack = consumers[consumer]
            val replicaRacks = partitionRacks[tp]
            return consumerRack == null || replicaRacks != null && replicaRacks.contains(consumerRack)
        }

        fun maxAssignable(consumer: String): Int {
            val maxForConsumer =
                numPartitionsPerConsumer + (if (remainingConsumersWithExtraPartition > 0) 1 else 0) - numAssignedByConsumer[consumer]!!
            return maxForConsumer.coerceAtLeast(0)
        }

        fun onAssigned(consumer: String, newlyAssignedPartitions: List<TopicPartition>) {
            val numAssigned = numAssignedByConsumer.compute(consumer) { _, n -> n!! + newlyAssignedPartitions.size }!!
            if (numAssigned > numPartitionsPerConsumer) remainingConsumersWithExtraPartition--
            unassignedPartitions.removeAll(newlyAssignedPartitions.toSet())
        }

        override fun toString(): String {
            return "TopicAssignmentState(" +
                    "topic=$topic" +
                    ", consumers=$consumers" +
                    ", partitionRacks=$partitionRacks" +
                    ", unassignedPartitions=$unassignedPartitions" +
                    ")"
        }
    }

    companion object {

        const val RANGE_ASSIGNOR_NAME = "range"

        private val PARTITION_COMPARATOR = TopicPartitionComparator()
    }
}
