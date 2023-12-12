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

import java.io.Serializable
import java.util.Collections
import java.util.LinkedList
import java.util.TreeSet
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.Utils.PartitionComparator
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import kotlin.math.abs
import kotlin.math.absoluteValue
import kotlin.math.ceil
import kotlin.math.floor

/**
 * Sticky assignment implementation used by [org.apache.kafka.clients.consumer.StickyAssignor] and
 * [org.apache.kafka.clients.consumer.CooperativeStickyAssignor]. Sticky assignors are rack-aware.
 * If racks are specified for consumers, we attempt to match consumer racks with partition replica
 * racks on a best-effort basis, prioritizing balanced assignment over rack alignment. Previously
 * owned partitions may be reassigned to improve rack locality. We use rack-aware assignment if both
 * consumer and partition racks are available and some partitions have replicas only on a subset of racks.
 */
abstract class AbstractStickyAssignor : AbstractPartitionAssignor() {

    var maxGeneration = DEFAULT_GENERATION

    private var partitionMovements: PartitionMovements? = null

    // Keep track of the partitions being migrated from one consumer to another during assignment
    // so the cooperative assignor can adjust the assignment
    var partitionsTransferringOwnership: MutableMap<TopicPartition, String>? = mutableMapOf()

    abstract fun memberData(
        subscription: ConsumerPartitionAssignor.Subscription,
    ): MemberData

    override fun assignPartitions(
        partitionsPerTopic: MutableMap<String, MutableList<PartitionInfo>>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        val consumerToOwnedPartitions = mutableMapOf<String, MutableList<TopicPartition>>()
        val partitionsWithMultiplePreviousOwners = mutableSetOf<TopicPartition>()

        val allPartitions = partitionsPerTopic.values.flatten()
        val rackInfo = RackInfo(allPartitions, subscriptions)

        val assignmentBuilder = if (
            allSubscriptionsEqual(
                allTopics = partitionsPerTopic.keys,
                subscriptions = subscriptions,
                consumerToOwnedPartitions = consumerToOwnedPartitions,
                partitionsWithMultiplePreviousOwners = partitionsWithMultiplePreviousOwners,
            )
        ) {
            log.debug(
                "Detected that all consumers were subscribed to same set of topics, invoking the " +
                        "optimized assignment algorithm"
            )
            partitionsTransferringOwnership = mutableMapOf()
            ConstrainedAssignmentBuilder(
                partitionsPerTopic = partitionsPerTopic,
                rackInfo = rackInfo,
                consumerToOwnedPartitions = consumerToOwnedPartitions,
                partitionsWithMultiplePreviousOwners = partitionsWithMultiplePreviousOwners,
            )
        } else {
            log.debug(
                "Detected that not all consumers were subscribed to same set of topics, falling " +
                        "back to the general case assignment algorithm"
            )
            // we must set this to null for the general case so the cooperative assignor knows to
            // compute it from scratch
            partitionsTransferringOwnership = null
            GeneralAssignmentBuilder(
                partitionsPerTopic = partitionsPerTopic,
                rackInfo = rackInfo,
                currentAssignment = consumerToOwnedPartitions,
                subscriptions = subscriptions,
            )
        }
        return assignmentBuilder.build()
    }

    override fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> = assignPartitions(
        partitionsPerTopic = partitionInfosWithoutRacks(partitionsPerTopic),
        subscriptions = subscriptions,
    )

    /**
     * Returns true iff all consumers have an identical subscription. Also fills out the passed in
     * `consumerToOwnedPartitions` with each consumer's previously owned and still-subscribed
     * partitions, and the `partitionsWithMultiplePreviousOwners` with any partitions claimed by
     * multiple previous owners
     */
    private fun allSubscriptionsEqual(
        allTopics: Set<String>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
        consumerToOwnedPartitions: MutableMap<String, MutableList<TopicPartition>>,
        partitionsWithMultiplePreviousOwners: MutableSet<TopicPartition>,
    ): Boolean {
        var isAllSubscriptionsEqual = true
        val subscribedTopics = mutableSetOf<String>()

        // keep track of all previously owned partitions so we can invalidate them if invalid input
        // is detected, e.g. two consumers somehow claiming the same partition in the same/current
        // generation
        val allPreviousPartitionsToOwner = mutableMapOf<TopicPartition, String>()
        for ((consumer, subscription) in subscriptions) {

            // initialize the subscribed topics set if this is the first subscription
            if (subscribedTopics.isEmpty()) subscribedTopics.addAll(subscription.topics)
            else if (
                isAllSubscriptionsEqual
                && !(subscription.topics.size == subscribedTopics.size
                        && subscribedTopics.containsAll(subscription.topics))
            ) isAllSubscriptionsEqual = false

            val memberData = memberData(subscription)
            val memberGeneration = memberData.generation ?: DEFAULT_GENERATION
            maxGeneration = maxGeneration.coerceAtLeast(memberGeneration)

            val ownedPartitions = mutableListOf<TopicPartition>()
            consumerToOwnedPartitions[consumer] = ownedPartitions

            // the member has a valid generation, so we can consider its owned partitions if it has the highest
            // generation amongst
            for (tp in memberData.partitions) {
                if (allTopics.contains(tp.topic)) {
                    val otherConsumer = allPreviousPartitionsToOwner[tp]
                    if (otherConsumer == null) {
                        // this partition is not owned by other consumer in the same generation
                        ownedPartitions.add(tp)
                        allPreviousPartitionsToOwner[tp] = consumer
                    } else {
                        val otherMemberGeneration = subscriptions[otherConsumer]!!.generationId ?: DEFAULT_GENERATION
                        if (memberGeneration == otherMemberGeneration) {
                            // if two members of the same generation own the same partition, revoke the partition
                            log.error(
                                "Found multiple consumers {} and {} claiming the same TopicPartition {} in the "
                                        + "same generation {}, this will be invalidated and removed from their previous assignment.",
                                consumer, otherConsumer, tp, memberGeneration
                            )
                            partitionsWithMultiplePreviousOwners.add(tp)
                            consumerToOwnedPartitions[otherConsumer]!!.remove(tp)
                            allPreviousPartitionsToOwner[tp] = consumer
                        } else if (memberGeneration > otherMemberGeneration) {
                            // move partition from the member with an older generation to the member with the newer generation
                            ownedPartitions.add(tp)
                            consumerToOwnedPartitions[otherConsumer]!!.remove(tp)
                            allPreviousPartitionsToOwner[tp] = consumer
                            log.warn(
                                "Consumer {} in generation {} and consumer {} in generation {} claiming the same " +
                                        "TopicPartition {} in different generations. The topic partition will be " +
                                        "assigned to the member with the higher generation {}.",
                                consumer,
                                memberGeneration,
                                otherConsumer,
                                otherMemberGeneration,
                                tp,
                                memberGeneration,
                            )
                        } else {
                            // let the other member continue to own the topic partition
                            log.warn(
                                "Consumer {} in generation {} and consumer {} in generation {} claiming the same " +
                                        "TopicPartition {} in different generations. The topic partition will be " +
                                        "assigned to the member with the higher generation {}.",
                                consumer,
                                memberGeneration,
                                otherConsumer,
                                otherMemberGeneration,
                                tp,
                                otherMemberGeneration,
                            )
                        }
                    }
                }
            }
        }
        return isAllSubscriptionsEqual
    }

    val isSticky: Boolean
        get() = partitionMovements!!.isSticky()

    /**
     * This constrainedAssign optimizes the assignment algorithm when all consumers were subscribed
     * to same set of topics. The method includes the following steps:
     *
     * 1. Reassign previously owned partitions:
     *    a. if owned less than minQuota partitions, just assign all owned partitions, and put the
     *       member into unfilled member list
     *    b. if owned maxQuota or more, and we're still under the number of expected max capacity
     *       members, assign maxQuota partitions
     *    c. if owned at least "minQuota" of partitions, assign minQuota partitions, and put the
     *       member into unfilled member list if we're still under the number of expected max
     *       capacity members
     * 2. Fill remaining members up to the expected numbers of maxQuota partitions, otherwise, to
     *    minQuota partitions
     *
     * @param partitionsPerTopic The number of partitions for each subscribed topic
     * @param consumerToOwnedPartitions Each consumer's previously owned and still-subscribed
     * partitions
     * @param partitionsWithMultiplePreviousOwners The partitions being claimed in the previous
     * assignment of multiple consumers
     * @return Map from each member to the list of partitions assigned to them.
     */
    private fun constrainedAssign(
        partitionsPerTopic: Map<String, Int>,
        consumerToOwnedPartitions: Map<String, MutableList<TopicPartition>>,
        partitionsWithMultiplePreviousOwners: Set<TopicPartition>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        if (log.isDebugEnabled) log.debug(
            "Performing constrained assign with partitionsPerTopic: {}, " +
                    "consumerToOwnedPartitions: {}.",
            partitionsPerTopic,
            consumerToOwnedPartitions,
        )

        val allRevokedPartitions = mutableSetOf<TopicPartition>()

        // the consumers which may still be assigned one or more partitions to reach expected
        // capacity
        val unfilledMembersWithUnderMinQuotaPartitions: MutableList<String> = LinkedList()
        val unfilledMembersWithExactlyMinQuotaPartitions = LinkedList<String>()
        val numberOfConsumers = consumerToOwnedPartitions.size
        val totalPartitionsCount = partitionsPerTopic.values.sum()

        val minQuota = floor((totalPartitionsCount.toDouble()) / numberOfConsumers).toInt()
        val maxQuota = ceil((totalPartitionsCount.toDouble()) / numberOfConsumers).toInt()
        // the expected number of members receiving more than minQuota partitions (zero when
        // minQuota == maxQuota)
        val expectedNumMembersWithOverMinQuotaPartitions = totalPartitionsCount % numberOfConsumers
        // the current number of members receiving more than minQuota partitions (zero when
        // minQuota == maxQuota)
        var currentNumMembersWithOverMinQuotaPartitions = 0

        // initialize the assignment map with an empty array of size maxQuota for all members
        val assignment: MutableMap<String, MutableList<TopicPartition>> = consumerToOwnedPartitions.keys
            .associateWith { ArrayList<TopicPartition>(maxQuota) }
            .toMutableMap()

        val assignedPartitions: MutableList<TopicPartition> = ArrayList()
        // Reassign previously owned partitions, up to the expected number of partitions per consumer
        for ((consumer, ownedPartitions) in consumerToOwnedPartitions) {
            val consumerAssignment = assignment[consumer]!!
            for (doublyClaimedPartition in partitionsWithMultiplePreviousOwners) {
                if (ownedPartitions.contains(doublyClaimedPartition)) {
                    log.error(
                        "Found partition {} still claimed as owned by consumer {}, despite being " +
                                "claimed by multiple consumers already in the same generation. " +
                                "Removing it from the ownedPartitions",
                        doublyClaimedPartition,
                        consumer,
                    )
                    ownedPartitions.remove(doublyClaimedPartition)
                }
            }

            if (ownedPartitions.size < minQuota) {
                // the expected assignment size is more than this consumer has now, so keep all the
                // owned partitions and put this member into the unfilled member list
                if (ownedPartitions.size > 0) {
                    consumerAssignment.addAll(ownedPartitions)
                    assignedPartitions.addAll(ownedPartitions)
                }
                unfilledMembersWithUnderMinQuotaPartitions.add(consumer)
            } else if (
                ownedPartitions.size >= maxQuota
                && currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions
            ) {
                // consumer owned the "maxQuota" of partitions or more, and we're still under the number of expected members
                // with more than the minQuota partitions, so keep "maxQuota" of the owned partitions, and revoke the rest of the partitions
                currentNumMembersWithOverMinQuotaPartitions++
                if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                    unfilledMembersWithExactlyMinQuotaPartitions.clear()
                }
                val maxQuotaPartitions = ownedPartitions.subList(0, maxQuota)
                consumerAssignment.addAll(maxQuotaPartitions)
                assignedPartitions.addAll(maxQuotaPartitions)
                allRevokedPartitions.addAll(ownedPartitions.subList(maxQuota, ownedPartitions.size))
            } else {
                // consumer owned at least "minQuota" of partitions, so keep "minQuota" of the owned
                // partitions, and revoke the rest of the partitions
                val minQuotaPartitions: List<TopicPartition> = ownedPartitions.subList(0, minQuota)
                consumerAssignment.addAll(minQuotaPartitions)
                assignedPartitions.addAll(minQuotaPartitions)
                allRevokedPartitions.addAll(ownedPartitions.subList(minQuota, ownedPartitions.size))

                // this consumer is potential maxQuota candidate since we're still under the number
                // of expected members with more than the minQuota partitions. Note, if the number
                // of expected members with more than the minQuota partitions is 0, it means
                // minQuota == maxQuota, and there are no potentially unfilled
                if (currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions)
                    unfilledMembersWithExactlyMinQuotaPartitions.add(consumer)
            }
        }
        val unassignedPartitions = getUnassignedPartitions(
            totalPartitionsCount = totalPartitionsCount,
            partitionsPerTopic = partitionsPerTopic,
            sortedAssignedPartitions = assignedPartitions
        )
        if (log.isDebugEnabled) log.debug(
            "After reassigning previously owned partitions, unfilled members: {}, unassigned " +
                    "partitions: {}, current assignment: {}",
            unfilledMembersWithUnderMinQuotaPartitions,
            unassignedPartitions,
            assignment
        )

        unfilledMembersWithUnderMinQuotaPartitions.sort()
        unfilledMembersWithExactlyMinQuotaPartitions.sort()

        var unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator()
        // Round-Robin filling remaining members up to the expected numbers of maxQuota, otherwise,
        // to minQuota
        for (unassignedPartition: TopicPartition in unassignedPartitions) {
            val consumer: String = if (unfilledConsumerIter.hasNext()) {
                unfilledConsumerIter.next()
            } else {
                if (
                    unfilledMembersWithUnderMinQuotaPartitions.isEmpty()
                    && unfilledMembersWithExactlyMinQuotaPartitions.isEmpty()
                ) {
                    // Should not enter here since we have calculated the exact number to assign to
                    // each consumer. This indicates issues in the assignment algorithm
                    val currentPartitionIndex = unassignedPartitions.indexOf(unassignedPartition)
                    log.error(
                        "No more unfilled consumers to be assigned. The remaining unassigned " +
                                "partitions are: {}",
                        unassignedPartitions.subList(
                            currentPartitionIndex,
                            unassignedPartitions.size,
                        )
                    )
                    error("No more unfilled consumers to be assigned.")
                } else if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty())
                    unfilledMembersWithExactlyMinQuotaPartitions.poll()
                else {
                    unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator()
                    unfilledConsumerIter.next()
                }
            }
            val consumerAssignment = assignment[consumer]!!
            consumerAssignment.add(unassignedPartition)

            // We already assigned all possible ownedPartitions, so we know this must be newly
            // assigned to this consumer or else the partition was actually claimed by multiple
            // previous owners and had to be invalidated from all members claimed ownedPartitions
            if (
                allRevokedPartitions.contains(unassignedPartition)
                || partitionsWithMultiplePreviousOwners.contains(unassignedPartition)
            ) partitionsTransferringOwnership!![unassignedPartition] = consumer

            val currentAssignedCount = consumerAssignment.size
            if (currentAssignedCount == minQuota) {
                unfilledConsumerIter.remove()
                unfilledMembersWithExactlyMinQuotaPartitions.add(consumer)
            } else if (currentAssignedCount == maxQuota) {
                currentNumMembersWithOverMinQuotaPartitions++
                if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                    // We only start to iterate over the "potentially unfilled" members at minQuota
                    // after we've filled all members up to at least minQuota, so once the last
                    // minQuota member reaches maxQuota, we should be done. But in case of some
                    // algorithmic error, just log a warning and continue to assign any remaining
                    // partitions within the assignment constraints
                    if (unassignedPartitions.indexOf(unassignedPartition) != unassignedPartitions.size - 1)
                        log.error(
                            "Filled the last member up to maxQuota but still had partitions " +
                                    "remaining to assign, will continue but this indicates a bug " +
                                    "in the assignment."
                        )
                }
            }
        }

        if (unfilledMembersWithUnderMinQuotaPartitions.isNotEmpty()) {
            // we expected all the remaining unfilled members have minQuota partitions and we're
            // already at the expected number of members with more than the minQuota partitions.
            // Otherwise, there must be error here.
            if (currentNumMembersWithOverMinQuotaPartitions != expectedNumMembersWithOverMinQuotaPartitions) {
                log.error(
                    "Current number of members with more than the minQuota partitions: {}, is less than the expected number " +
                            "of members with more than the minQuota partitions: {}, and no more partitions to be assigned to the remaining unfilled consumers: {}",
                    currentNumMembersWithOverMinQuotaPartitions,
                    expectedNumMembersWithOverMinQuotaPartitions,
                    unfilledMembersWithUnderMinQuotaPartitions,
                )
                error(
                    "We haven't reached the expected number of members with more than the " +
                            "minQuota partitions, but no more partitions to be assigned"
                )
            } else {
                for (unfilledMember in unfilledMembersWithUnderMinQuotaPartitions) {
                    val assignedPartitionsCount = assignment[unfilledMember]!!.size
                    if (assignedPartitionsCount != minQuota) {
                        log.error(
                            "Consumer: [{}] should have {} partitions, but got {} partitions, " +
                                    "and no more partitions to be assigned. The remaining " +
                                    "unfilled consumers are: {}",
                            unfilledMember,
                            minQuota,
                            assignedPartitionsCount,
                            unfilledMembersWithUnderMinQuotaPartitions,
                        )
                        error(
                            "Consumer: [$unfilledMember] doesn't reach minQuota partitions, and " +
                                    "no more partitions to be assigned",
                        )
                    } else log.trace(
                        "skip over this unfilled member: [{}] because we've reached the expected " +
                                "number of members with more than the minQuota partitions, and " +
                                "this member already have minQuota partitions",
                        unfilledMember,
                    )
                }
            }
        }
        log.info("Final assignment of partitions to consumers: \n{}", assignment)
        return assignment
    }

    private fun getAllTopicPartitions(
        partitionsPerTopic: Map<String, Int>,
        sortedAllTopics: List<String>,
        totalPartitionsCount: Int,
    ): MutableList<TopicPartition> {
        val allPartitions: MutableList<TopicPartition> = ArrayList(totalPartitionsCount)
        for (topic: String in sortedAllTopics) {
            val partitionCount = partitionsPerTopic[topic]!!
            for (i in 0 until partitionCount) allPartitions.add(TopicPartition(topic, i))
        }
        return allPartitions
    }

    /**
     * This generalAssign algorithm guarantees the assignment that is as balanced as possible.
     * This method includes the following steps:
     *
     * 1. Preserving all the existing partition assignments
     * 2. Removing all the partition assignments that have become invalid due to the change that
     *    triggers the reassignment
     * 3. Assigning the unassigned partitions in a way that balances out the overall assignments of
     *    partitions to consumers
     * 4. Further balancing out the resulting assignment by finding the partitions that can be
     *    reassigned to another consumer towards an overall more balanced assignment.
     *
     * @param partitionsPerTopic The number of partitions for each subscribed topic.
     * @param subscriptions Map from the member id to their respective topic subscription
     * @param currentAssignment Each consumer's previously owned and still-subscribed partitions
     * @return Map from each member to the list of partitions assigned to them.
     */
    private fun generalAssign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
        currentAssignment: MutableMap<String, MutableList<TopicPartition>>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        if (log.isDebugEnabled) log.debug(
            "performing general assign. partitionsPerTopic: {}, subscriptions: {}, " +
                    "currentAssignment: {}",
            partitionsPerTopic,
            subscriptions,
            currentAssignment,
        )

        val prevAssignment: MutableMap<TopicPartition, ConsumerGenerationPair> = HashMap()
        partitionMovements = PartitionMovements()
        prepopulateCurrentAssignments(subscriptions, prevAssignment)

        // a mapping of all consumers to all potential topics that can be assigned to them
        val consumer2AllPotentialTopics: MutableMap<String, List<String>> =
            HashMap(subscriptions.keys.size)

        // a mapping of all topics to all consumers that can be assigned to them
        val topic2AllPotentialConsumers = partitionsPerTopic.keys
            .associateWith { mutableListOf<String>() }

        for ((consumerId, subscription) in subscriptions) {
            val subscribedTopics: MutableList<String> = ArrayList(subscription.topics.size)
            consumer2AllPotentialTopics[consumerId] = subscribedTopics

            subscription.topics.filter { topic -> partitionsPerTopic[topic] != null }
                .forEach { topic ->
                    subscribedTopics.add(topic)
                    topic2AllPotentialConsumers[topic]!!.add(consumerId)
                }

            // add this consumer to currentAssignment (with an empty topic partition assignment) if
            // it does not already exist
            currentAssignment.computeIfAbsent(consumerId) { mutableListOf() }
        }

        // a mapping of partition to current consumer
        val currentPartitionConsumer = mutableMapOf<TopicPartition, String>()
        for ((key, value) in currentAssignment)
            for (topicPartition in value)
                currentPartitionConsumer[topicPartition] = key
        val totalPartitionsCount = partitionsPerTopic.values.sum()

        val sortedAllTopics: List<String> = ArrayList(topic2AllPotentialConsumers.keys)
        Collections.sort(sortedAllTopics, TopicComparator(topic2AllPotentialConsumers))

        val sortedAllPartitions = getAllTopicPartitions(
            partitionsPerTopic = partitionsPerTopic,
            sortedAllTopics = sortedAllTopics,
            totalPartitionsCount = totalPartitionsCount,
        )

        // the partitions already assigned in current assignment
        val assignedPartitions: MutableList<TopicPartition> = ArrayList()
        var revocationRequired = false
        val iterator = currentAssignment.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            val consumerSubscription = subscriptions[entry.key]
            if (consumerSubscription == null) {
                // if a consumer that existed before (and had some partition assignments) is now
                // removed, remove it from currentAssignment
                for (topicPartition in entry.value) currentPartitionConsumer.remove(topicPartition)
                iterator.remove()
            } else {
                // otherwise (the consumer still exists)
                val partitionIter = entry.value.iterator()
                while (partitionIter.hasNext()) {
                    val partition = partitionIter.next()
                    if (!topic2AllPotentialConsumers.containsKey(partition.topic)) {
                        // if this topic partition of this consumer no longer exists, remove it from
                        // currentAssignment of the consumer
                        partitionIter.remove()
                        currentPartitionConsumer.remove(partition)
                    } else if (!consumerSubscription.topics.contains(partition.topic)) {
                        // because the consumer is no longer subscribed to its topic, remove it from
                        // currentAssignment of the consumer
                        partitionIter.remove()
                        revocationRequired = true
                    } else {
                        // otherwise, remove the topic partition from those that need to be assigned
                        // only if its current consumer is still subscribed to its topic (because it
                        // is already assigned and we would want to preserve that assignment as much
                        // as possible)
                        assignedPartitions.add(partition)
                    }
                }
            }
        }

        // all partitions that needed to be assigned
        val unassignedPartitions = getUnassignedPartitions(
            sortedAllPartitions = sortedAllPartitions,
            sortedAssignedPartitions = assignedPartitions,
            topic2AllPotentialConsumers = topic2AllPotentialConsumers,
        )
        if (log.isDebugEnabled) log.debug("unassigned Partitions: {}", unassignedPartitions)

        // at this point we have preserved all valid topic partition to consumer assignments and
        // removed all invalid topic partitions and invalid consumers. Now we need to assign
        // unassignedPartitions to consumers so that the topic partition assignments are as balanced
        // as possible.

        // an ascending sorted set of consumers based on how many topic partitions are already
        // assigned to them
        val sortedCurrentSubscriptions: TreeSet<String> =
            TreeSet(SubscriptionComparator(currentAssignment))
        sortedCurrentSubscriptions.addAll(currentAssignment.keys)

        balance(
            currentAssignment = currentAssignment,
            prevAssignment = prevAssignment,
            sortedPartitions = sortedAllPartitions,
            unassignedPartitions = unassignedPartitions,
            sortedCurrentSubscriptions = sortedCurrentSubscriptions,
            consumer2AllPotentialTopics = consumer2AllPotentialTopics,
            topic2AllPotentialConsumers = topic2AllPotentialConsumers,
            currentPartitionConsumer = currentPartitionConsumer,
            revocationRequired = revocationRequired,
            partitionsPerTopic = partitionsPerTopic,
            totalPartitionCount = totalPartitionsCount,
        )
        log.info("Final assignment of partitions to consumers: \n{}", currentAssignment)
        return currentAssignment
    }

    /**
     * get the unassigned partition list by computing the difference set of the sortedPartitions(all
     * partitions) and sortedAssignedPartitions. If no assigned partitions, we'll just return all
     * sorted topic partitions. This is used in generalAssign method
     *
     * We loop the sortedPartition, and compare the ith element in sortedAssignedPartitions(i start
     * from 0):
     * - if not equal to the ith element, add to unassignedPartitions
     * - if equal to the ith element, get next element from sortedAssignedPartitions
     *
     * @param sortedAllPartitions Sorted all partitions
     * @param sortedAssignedPartitions Sorted partitions, all are included in the sortedPartitions
     * @param topic2AllPotentialConsumers Topics mapped to all consumers that subscribed to it
     * @return Partitions that aren't assigned to any current consumer
     */
    private fun getUnassignedPartitions(
        sortedAllPartitions: MutableList<TopicPartition>,
        sortedAssignedPartitions: List<TopicPartition>,
        topic2AllPotentialConsumers: Map<String, MutableList<String>>,
    ): MutableList<TopicPartition> {
        if (sortedAssignedPartitions.isEmpty()) return sortedAllPartitions

        val unassignedPartitions = mutableListOf<TopicPartition>()
        Collections.sort(sortedAssignedPartitions, PartitionComparator(topic2AllPotentialConsumers))
        var shouldAddDirectly = false
        val sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator()
        var nextAssignedPartition = sortedAssignedPartitionsIter.next()
        for (topicPartition in sortedAllPartitions) {
            if (shouldAddDirectly || nextAssignedPartition != topicPartition)
                unassignedPartitions.add(topicPartition)
            else {
                // this partition is in assignedPartitions, don't add to unassignedPartitions, just
                // get next assigned partition
                if (sortedAssignedPartitionsIter.hasNext()) {
                    nextAssignedPartition = sortedAssignedPartitionsIter.next()
                } else {
                    // add the remaining directly since there is no more sortedAssignedPartitions
                    shouldAddDirectly = true
                }
            }
        }
        return unassignedPartitions
    }

    /**
     * get the unassigned partition list by computing the difference set of all sorted partitions
     * and sortedAssignedPartitions. If no assigned partitions, we'll just return all sorted topic
     * partitions. This is used in constrainedAssign method
     *
     * To compute the difference set, we use two pointers technique here:
     *
     * We loop through the all sorted topics, and then iterate all partitions the topic has,
     * compared with the ith element in sortedAssignedPartitions(i starts from 0):
     * - if not equal to the ith element, add to unassignedPartitions
     * - if equal to the ith element, get next element from sortedAssignedPartitions
     *
     * @param totalPartitionsCount all partitions counts in this assignment
     * @param partitionsPerTopic the number of partitions for each subscribed topic.
     * @param sortedAssignedPartitions sorted partitions, all are included in the sortedPartitions
     * @return the partitions not yet assigned to any consumers
     */
    private fun getUnassignedPartitions(
        totalPartitionsCount: Int,
        partitionsPerTopic: Map<String, Int>,
        sortedAssignedPartitions: MutableList<TopicPartition>,
    ): List<TopicPartition> {
        val sortedAllTopics = partitionsPerTopic.keys.toMutableList()
        // sort all topics first, then we can have sorted all topic partitions by adding partitions
        // starting from 0
        sortedAllTopics.sort()
        if (sortedAssignedPartitions.isEmpty()) {
            // no assigned partitions means all partitions are unassigned partitions
            return getAllTopicPartitions(partitionsPerTopic, sortedAllTopics, totalPartitionsCount)
        }

        val unassignedPartitions: MutableList<TopicPartition> =
            ArrayList(totalPartitionsCount - sortedAssignedPartitions.size)

        sortedAssignedPartitions.sortWith(
            Comparator.comparing { obj: TopicPartition -> obj.topic }
                .thenComparing { obj: TopicPartition -> obj.partition }
        )

        var shouldAddDirectly = false
        val sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator()
        var nextAssignedPartition = sortedAssignedPartitionsIter.next()
        for (topic: String in sortedAllTopics) {
            val partitionCount = partitionsPerTopic[topic]!!
            for (i in 0 until partitionCount) {
                if (
                    shouldAddDirectly
                    || !((nextAssignedPartition.topic == topic)
                            && nextAssignedPartition.partition == i)
                ) unassignedPartitions.add(TopicPartition(topic, i))
                else {
                    // this partition is in assignedPartitions, don't add to unassignedPartitions,
                    // just get next assigned partition
                    if (sortedAssignedPartitionsIter.hasNext())
                        nextAssignedPartition = sortedAssignedPartitionsIter.next()

                    // add the remaining directly since there is no more sortedAssignedPartitions
                    else shouldAddDirectly = true
                }
            }
        }
        return unassignedPartitions
    }

    /**
     * update the prevAssignment with the partitions, consumer and generation in parameters
     *
     * @param partitions The partitions to be updated the prevAssignment
     * @param consumer The consumer ID
     * @param prevAssignment The assignment contains the assignment with the 2nd largest generation
     * @param generation The generation of this assignment (partitions)
     */
    private fun updatePrevAssignment(
        prevAssignment: MutableMap<TopicPartition, ConsumerGenerationPair>,
        partitions: List<TopicPartition>,
        consumer: String,
        generation: Int,
    ) {
        partitions.forEach { partition ->
            if (prevAssignment.containsKey(partition)) {
                // only keep the latest previous assignment
                if (generation > prevAssignment[partition]!!.generation)
                    prevAssignment[partition] = ConsumerGenerationPair(consumer, generation)
            } else prevAssignment[partition] = ConsumerGenerationPair(consumer, generation)
        }
    }

    /**
     * Filling in the prevAssignment from the subscriptions.
     *
     * @param subscriptions Map from the member id to their respective topic subscription
     * @param prevAssignment The assignment contains the assignment with the 2nd largest generation
     */
    private fun prepopulateCurrentAssignments(
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
        prevAssignment: MutableMap<TopicPartition, ConsumerGenerationPair>,
    ) {
        // we need to process subscriptions' user data with each consumer's reported generation in
        // mind higher generations overwrite lower generations in case of a conflict note that a
        // conflict could exist only if user data is for different generations
        subscriptions.forEach { subscriptionEntry ->
            val consumer = subscriptionEntry.key
            val subscription = subscriptionEntry.value
            if (subscription.userData != null) {
                // since this is our 2nd time to deserialize memberData, rewind userData is necessary
                subscription.userData.rewind()
            }
            val memberData = memberData(subscription)

            // we already have the maxGeneration info, so just compare the current generation of
            // memberData, and put into prevAssignment
            if (memberData.generation != null && memberData.generation < maxGeneration) {
                // if the current member's generation is lower than maxGeneration, put into
                // prevAssignment if needed
                updatePrevAssignment(
                    prevAssignment = prevAssignment,
                    partitions = memberData.partitions,
                    consumer = consumer,
                    generation = memberData.generation,
                )
            } else if (memberData.generation == null && maxGeneration > DEFAULT_GENERATION) {
                // if maxGeneration is larger than DEFAULT_GENERATION
                // put all (no generation) partitions as DEFAULT_GENERATION into prevAssignment if needed
                updatePrevAssignment(
                    prevAssignment = prevAssignment,
                    partitions = memberData.partitions,
                    consumer = consumer,
                    generation = DEFAULT_GENERATION,
                )
            }
        }
    }

    /**
     * determine if the current assignment is a balanced one
     *
     * @param currentAssignment The assignment whose balance needs to be checked
     * @param sortedCurrentSubscriptions An ascending sorted set of consumers based on how many
     * topic partitions are already assigned to them
     * @param allSubscriptions A mapping of all consumers to all potential topics that can be
     * assigned to them
     * @param partitionsPerTopic The number of partitions for each subscribed topic
     * @param totalPartitionCount Total partition count to be assigned
     * @return `true` if the given assignment is balanced; `false` otherwise
     */
    private fun isBalanced(
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        sortedCurrentSubscriptions: TreeSet<String>,
        allSubscriptions: Map<String, List<String>>,
        partitionsPerTopic: Map<String, Int>,
        totalPartitionCount: Int,
    ): Boolean {
        val min = currentAssignment[sortedCurrentSubscriptions.first()]!!.size
        val max = currentAssignment[sortedCurrentSubscriptions.last()]!!.size

        // if minimum and maximum numbers of partitions assigned to consumers differ by at most one
        // return true
        if (min >= max - 1) return true

        // create a mapping from partitions to the consumer assigned to them
        val allPartitions: MutableMap<TopicPartition, String?> = HashMap()
        currentAssignment.forEach { (key, topicPartitions) ->
            for (topicPartition in topicPartitions) {
                if (allPartitions.containsKey(topicPartition)) log.error(
                    "{} is assigned to more than one consumer.",
                    topicPartition
                )
                allPartitions[topicPartition] = key
            }
        }

        // for each consumer that does not have all the topic partitions it can get make sure none
        // of the topic partitions it could but did not get cannot be moved to it (because that
        // would break the balance)
        for (consumer: String in sortedCurrentSubscriptions) {
            val consumerPartitions = currentAssignment[consumer]!!
            val consumerPartitionCount = consumerPartitions.size

            // skip if this consumer already has all the topic partitions it can get
            val allSubscribedTopics = allSubscriptions[consumer]!!
            val maxAssignmentSize = getMaxAssignmentSize(
                totalPartitionCount = totalPartitionCount,
                allSubscribedTopics = allSubscribedTopics,
                partitionsPerTopic = partitionsPerTopic
            )
            if (consumerPartitionCount == maxAssignmentSize) continue

            // otherwise make sure it cannot get any more
            for (topic in allSubscribedTopics) {
                val partitionCount = partitionsPerTopic[topic]!!
                for (i in 0 until partitionCount) {
                    val topicPartition = TopicPartition(topic, i)
                    if (!currentAssignment[consumer]!!.contains(topicPartition)) {
                        val otherConsumer = allPartitions[topicPartition]
                        val otherConsumerPartitionCount = currentAssignment[otherConsumer]!!.size
                        if (consumerPartitionCount < otherConsumerPartitionCount) {
                            log.debug(
                                "{} can be moved from consumer {} to consumer {} for a more " +
                                        "balanced assignment.",
                                topicPartition,
                                otherConsumer,
                                consumer,
                            )
                            return false
                        }
                    }
                }
            }
        }
        return true
    }

    /**
     * get the maximum assigned partition size of the `allSubscribedTopics`
     *
     * @param totalPartitionCount Total partition count to be assigned
     * @param allSubscribedTopics The subscribed topics of a consumer
     * @param partitionsPerTopic The number of partitions for each subscribed topic
     * @return maximum assigned partition size
     */
    private fun getMaxAssignmentSize(
        totalPartitionCount: Int,
        allSubscribedTopics: List<String>,
        partitionsPerTopic: Map<String, Int>,
    ): Int {
        return if (allSubscribedTopics.size == partitionsPerTopic.size) totalPartitionCount
        else allSubscribedTopics.sumOf { topic -> partitionsPerTopic[topic]!! }
    }

    /**
     * @return the balance score of the given assignment, as the sum of assigned partitions size
     * difference of all consumer pairs. A perfectly balanced assignment (with all consumers getting
     * the same number of partitions) has a balance score of 0. Lower balance score indicates a more
     * balanced assignment.
     */
    private fun getBalanceScore(assignment: Map<String, MutableList<TopicPartition>>): Int {
        var score = 0
        val consumer2AssignmentSize = assignment.mapValues { (_, value) -> value.size }
            .toMutableMap()

        val iterator = consumer2AssignmentSize.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            val consumerAssignmentSize = entry.value
            iterator.remove()
            for ((_, value) in consumer2AssignmentSize)
                score += abs(consumerAssignmentSize - value)
        }
        return score
    }

    /**
     * The assignment should improve the overall balance of the partition assignments to consumers.
     */
    private fun assignPartition(
        partition: TopicPartition,
        sortedCurrentSubscriptions: TreeSet<String>,
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        consumer2AllPotentialTopics: Map<String, List<String>>,
        currentPartitionConsumer: MutableMap<TopicPartition, String>,
    ) {
        for (consumer in sortedCurrentSubscriptions) {
            if (consumer2AllPotentialTopics[consumer]!!.contains(partition.topic)) {
                sortedCurrentSubscriptions.remove(consumer)
                currentAssignment[consumer]!!.add(partition)
                currentPartitionConsumer[partition] = consumer
                sortedCurrentSubscriptions.add(consumer)
                break
            }
        }
    }

    private fun canParticipateInReassignment(
        topic: String,
        topic2AllPotentialConsumers: Map<String, MutableList<String>>,
    ): Boolean {
        // if a topic has two or more potential consumers it is subject to reassignment.
        return topic2AllPotentialConsumers[topic]!!.size >= 2
    }

    private fun canParticipateInReassignment(
        consumer: String,
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        consumer2AllPotentialTopics: Map<String, List<String>>,
        topic2AllPotentialConsumers: Map<String, MutableList<String>>,
        partitionsPerTopic: Map<String, Int>,
        totalPartitionCount: Int,
    ): Boolean {
        val currentPartitions: List<TopicPartition> = currentAssignment[consumer]!!
        val currentAssignmentSize = currentPartitions.size
        val allSubscribedTopics = consumer2AllPotentialTopics[consumer]!!
        val maxAssignmentSize = getMaxAssignmentSize(
            totalPartitionCount = totalPartitionCount,
            allSubscribedTopics = allSubscribedTopics,
            partitionsPerTopic = partitionsPerTopic
        )
        if (currentAssignmentSize > maxAssignmentSize) log.error(
            "The consumer {} is assigned more partitions than the maximum possible.",
            consumer,
        )
        if (currentAssignmentSize < maxAssignmentSize)
        // if a consumer is not assigned all its potential partitions it is subject to reassignment
            return true

        for (partition: TopicPartition in currentPartitions)
        // if any of the partitions assigned to a consumer is subject to reassignment the
        // consumer itself is subject to reassignment
            if (
                canParticipateInReassignment(
                    topic = partition.topic,
                    topic2AllPotentialConsumers = topic2AllPotentialConsumers,
                )
            ) return true
        return false
    }

    /**
     * Balance the current assignment using the data structures created in the assign(...) method
     * above.
     */
    private fun balance(
        currentAssignment: MutableMap<String, MutableList<TopicPartition>>,
        prevAssignment: Map<TopicPartition, ConsumerGenerationPair>,
        sortedPartitions: MutableList<TopicPartition>,
        unassignedPartitions: MutableList<TopicPartition>,
        sortedCurrentSubscriptions: TreeSet<String>,
        consumer2AllPotentialTopics: Map<String, List<String>>,
        topic2AllPotentialConsumers: Map<String, MutableList<String>>,
        currentPartitionConsumer: MutableMap<TopicPartition, String>,
        revocationRequired: Boolean,
        partitionsPerTopic: Map<String, Int>,
        totalPartitionCount: Int,
    ) {
        val initializing = currentAssignment[sortedCurrentSubscriptions.last()]!!.isEmpty()

        // assign all unassigned partitions
        for (partition: TopicPartition in unassignedPartitions) {
            // skip if there is no potential consumer for the topic
            if (topic2AllPotentialConsumers[partition.topic]!!.isEmpty()) continue
            assignPartition(
                partition = partition,
                sortedCurrentSubscriptions = sortedCurrentSubscriptions,
                currentAssignment = currentAssignment,
                consumer2AllPotentialTopics = consumer2AllPotentialTopics,
                currentPartitionConsumer = currentPartitionConsumer,
            )
        }

        // narrow down the reassignment scope to only those partitions that can actually be reassigned
        val fixedPartitions = mutableSetOf<TopicPartition>()
        for (topic: String in topic2AllPotentialConsumers.keys)
            if (!canParticipateInReassignment(topic, topic2AllPotentialConsumers))
                for (i in 0 until (partitionsPerTopic[topic])!!)
                    fixedPartitions.add(TopicPartition(topic, i))

        sortedPartitions.removeAll(fixedPartitions)
        unassignedPartitions.removeAll(fixedPartitions)

        // narrow down the reassignment scope to only those consumers that are subject to
        // reassignment
        val fixedAssignments: MutableMap<String, MutableList<TopicPartition>> = HashMap()
        for (consumer in consumer2AllPotentialTopics.keys)
            if (
                !canParticipateInReassignment(
                    consumer = consumer,
                    currentAssignment = currentAssignment,
                    consumer2AllPotentialTopics = consumer2AllPotentialTopics,
                    topic2AllPotentialConsumers = topic2AllPotentialConsumers,
                    partitionsPerTopic = partitionsPerTopic,
                    totalPartitionCount = totalPartitionCount,
                )
            ) {
                sortedCurrentSubscriptions.remove(consumer)
                fixedAssignments[consumer] = currentAssignment.remove(consumer)!!
            }

        // create a deep copy of the current assignment, so we can revert to it if we do not get a
        // more balanced assignment later
        val preBalanceAssignment = deepCopy(currentAssignment)
        val preBalancePartitionConsumers = currentPartitionConsumer.toMap()

        // if we don't already need to revoke something due to subscription changes, first try to
        // balance by only moving newly added partitions
        if (!revocationRequired) {
            performReassignments(
                reassignablePartitions = unassignedPartitions,
                currentAssignment = currentAssignment,
                prevAssignment = prevAssignment,
                sortedCurrentSubscriptions = sortedCurrentSubscriptions,
                consumer2AllPotentialTopics = consumer2AllPotentialTopics,
                topic2AllPotentialConsumers = topic2AllPotentialConsumers,
                currentPartitionConsumer = currentPartitionConsumer,
                partitionsPerTopic = partitionsPerTopic,
                totalPartitionCount = totalPartitionCount,
            )
        }
        val reassignmentPerformed = performReassignments(
            reassignablePartitions = sortedPartitions,
            currentAssignment = currentAssignment,
            prevAssignment = prevAssignment,
            sortedCurrentSubscriptions = sortedCurrentSubscriptions,
            consumer2AllPotentialTopics = consumer2AllPotentialTopics,
            topic2AllPotentialConsumers = topic2AllPotentialConsumers,
            currentPartitionConsumer = currentPartitionConsumer,
            partitionsPerTopic = partitionsPerTopic,
            totalPartitionCount = totalPartitionCount
        )

        // if we are not preserving existing assignments and we have made changes to the current
        // assignment make sure we are getting a more balanced assignment; otherwise, revert to
        // previous assignment
        if (
            !initializing
            && reassignmentPerformed
            && getBalanceScore(currentAssignment) >= getBalanceScore(preBalanceAssignment)
        ) {
            deepCopy(preBalanceAssignment, currentAssignment)
            currentPartitionConsumer.clear()
            currentPartitionConsumer.putAll(preBalancePartitionConsumers)
        }

        // add the fixed assignments (those that could not change) back
        for ((consumer, value) in fixedAssignments) {
            currentAssignment[consumer] = value
            sortedCurrentSubscriptions.add(consumer)
        }
        fixedAssignments.clear()
    }

    private fun performReassignments(
        reassignablePartitions: List<TopicPartition>,
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        prevAssignment: Map<TopicPartition, ConsumerGenerationPair>,
        sortedCurrentSubscriptions: TreeSet<String>,
        consumer2AllPotentialTopics: Map<String, List<String>>,
        topic2AllPotentialConsumers: Map<String, MutableList<String>>,
        currentPartitionConsumer: MutableMap<TopicPartition, String>,
        partitionsPerTopic: Map<String, Int>,
        totalPartitionCount: Int,
    ): Boolean {
        var reassignmentPerformed = false
        var modified: Boolean

        // repeat reassignment until no partition can be moved to improve the balance
        do {
            modified = false
            // reassign all reassignable partitions (starting from the partition with least
            // potential consumers and if needed) until the full list is processed or a balance is
            // achieved
            val partitionIterator = reassignablePartitions.iterator()
            while (
                partitionIterator.hasNext()
                && !isBalanced(
                    currentAssignment = currentAssignment,
                    sortedCurrentSubscriptions = sortedCurrentSubscriptions,
                    allSubscriptions = consumer2AllPotentialTopics,
                    partitionsPerTopic = partitionsPerTopic,
                    totalPartitionCount = totalPartitionCount,
                )
            ) {
                val partition = partitionIterator.next()

                // the partition must have at least two consumers
                if (topic2AllPotentialConsumers[partition.topic]!!.size <= 1) log.error(
                    "Expected more than one potential consumer for partition '{}'",
                    partition,
                )

                // the partition must have a current consumer
                val consumer = currentPartitionConsumer[partition]
                if (consumer == null) log.error(
                    "Expected partition '{}' to be assigned to a consumer",
                    partition,
                )
                if (prevAssignment.containsKey(partition)
                    && currentAssignment[consumer]!!.size > currentAssignment[prevAssignment[partition]!!.consumer]!!.size + 1
                ) {
                    reassignPartition(
                        partition = partition,
                        currentAssignment = currentAssignment,
                        sortedCurrentSubscriptions = sortedCurrentSubscriptions,
                        currentPartitionConsumer = currentPartitionConsumer,
                        newConsumer = prevAssignment[partition]!!.consumer,
                    )
                    reassignmentPerformed = true
                    modified = true
                    continue
                }

                // check if a better-suited consumer exist for the partition; if so, reassign it
                for (otherConsumer in topic2AllPotentialConsumers[partition.topic]!!) {
                    if (currentAssignment[consumer]!!.size > currentAssignment[otherConsumer]!!.size + 1) {
                        reassignPartition(
                            partition = partition,
                            currentAssignment = currentAssignment,
                            sortedCurrentSubscriptions = sortedCurrentSubscriptions,
                            currentPartitionConsumer = currentPartitionConsumer,
                            consumer2AllPotentialTopics = consumer2AllPotentialTopics,
                        )
                        reassignmentPerformed = true
                        modified = true
                        break
                    }
                }
            }
        } while (modified)
        return reassignmentPerformed
    }

    private fun reassignPartition(
        partition: TopicPartition,
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        sortedCurrentSubscriptions: TreeSet<String>,
        currentPartitionConsumer: MutableMap<TopicPartition, String>,
        consumer2AllPotentialTopics: Map<String, List<String>>,
    ) {
        // find the new consumer
        val newConsumer: String = sortedCurrentSubscriptions.first {
            consumer2AllPotentialTopics[it]!!.contains(partition.topic)
        }

        reassignPartition(
            partition = partition,
            currentAssignment = currentAssignment,
            sortedCurrentSubscriptions = sortedCurrentSubscriptions,
            currentPartitionConsumer = currentPartitionConsumer,
            newConsumer = newConsumer,
        )
    }

    private fun reassignPartition(
        partition: TopicPartition,
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        sortedCurrentSubscriptions: TreeSet<String>,
        currentPartitionConsumer: MutableMap<TopicPartition, String>,
        newConsumer: String,
    ) {
        val consumer = currentPartitionConsumer[partition]!!
        // find the correct partition movement considering the stickiness requirement
        val partitionToBeMoved = partitionMovements!!.getTheActualPartitionToBeMoved(
            partition = partition,
            oldConsumer = consumer,
            newConsumer = newConsumer,
        )
        processPartitionMovement(
            partition = partitionToBeMoved,
            newConsumer = newConsumer,
            currentAssignment = currentAssignment,
            sortedCurrentSubscriptions = sortedCurrentSubscriptions,
            currentPartitionConsumer = currentPartitionConsumer,
        )
    }

    private fun processPartitionMovement(
        partition: TopicPartition,
        newConsumer: String,
        currentAssignment: Map<String, MutableList<TopicPartition>>,
        sortedCurrentSubscriptions: TreeSet<String>,
        currentPartitionConsumer: MutableMap<TopicPartition, String>,
    ) {
        val oldConsumer = currentPartitionConsumer[partition]!!
        sortedCurrentSubscriptions.remove(oldConsumer)
        sortedCurrentSubscriptions.remove(newConsumer)
        partitionMovements!!.movePartition(partition, oldConsumer, newConsumer)
        currentAssignment[oldConsumer]!!.remove(partition)
        currentAssignment[newConsumer]!!.add(partition)
        currentPartitionConsumer[partition] = newConsumer
        sortedCurrentSubscriptions.add(newConsumer)
        sortedCurrentSubscriptions.add(oldConsumer)
    }

    private fun deepCopy(
        source: Map<String, MutableList<TopicPartition>>,
        dest: MutableMap<String, MutableList<TopicPartition>>,
    ) {
        dest.clear()
        for ((key, value) in source) dest[key] = ArrayList(value)
    }

    private fun deepCopy(
        assignment: Map<String, MutableList<TopicPartition>>,
    ): Map<String, MutableList<TopicPartition>> {
        val copy = mutableMapOf<String, MutableList<TopicPartition>>()
        deepCopy(assignment, copy)
        return copy
    }


    private class TopicComparator(
        private val map: Map<String, List<String>>,
    ) : Comparator<String>, Serializable {
        override fun compare(o1: String, o2: String): Int {
            var ret = map[o1]!!.size - map[o2]!!.size
            if (ret == 0) ret = o1.compareTo(o2)

            return ret
        }

        companion object {
            private val serialVersionUID = 1L
        }
    }

    private class SubscriptionComparator(
        private val map: Map<String, List<TopicPartition>>,
    ) : Comparator<String?>, Serializable {
        override fun compare(o1: String?, o2: String?): Int {
            var ret = map[o1]!!.size - map[o2]!!.size
            if (ret == 0) ret = o1!!.compareTo(o2!!)
            return ret
        }

        companion object {
            private val serialVersionUID = 1L
        }
    }

    /**
     * This class maintains some data structures to simplify lookup of partition movements among consumers.
     * At each point of time during a partition rebalance it keeps track of partition movements corresponding
     * to each topic, and also possible movement (in form a `ConsumerPair` object) for each partition.
     */
    private class PartitionMovements {

        private val partitionMovementsByTopic: MutableMap<String, MutableMap<ConsumerPair, MutableSet<TopicPartition>>> =
            mutableMapOf()

        private val partitionMovements: MutableMap<TopicPartition, ConsumerPair> = mutableMapOf()

        private fun removeMovementRecordOfPartition(partition: TopicPartition): ConsumerPair? {
            val pair = partitionMovements.remove(partition)

            val topic = partition.topic
            val partitionMovementsForThisTopic =
                partitionMovementsByTopic[topic]!!
            partitionMovementsForThisTopic[pair]!!.remove(partition)
            if (partitionMovementsForThisTopic[pair]!!.isEmpty()) partitionMovementsForThisTopic.remove(pair)
            if (partitionMovementsByTopic[topic]!!.isEmpty()) partitionMovementsByTopic.remove(topic)

            return pair
        }

        private fun addPartitionMovementRecord(partition: TopicPartition, pair: ConsumerPair) {
            partitionMovements[partition] = pair

            val topic = partition.topic
            if (!partitionMovementsByTopic.containsKey(topic)) partitionMovementsByTopic[topic] = mutableMapOf()

            val partitionMovementsForThisTopic =
                partitionMovementsByTopic[topic]!!
            if (!partitionMovementsForThisTopic.containsKey(pair)) partitionMovementsForThisTopic[pair] = mutableSetOf()

            partitionMovementsForThisTopic[pair]!!.add(partition)
        }

        fun movePartition(partition: TopicPartition, oldConsumer: String, newConsumer: String) {
            val pair = ConsumerPair(oldConsumer, newConsumer)
            if (partitionMovements.containsKey(partition)) {
                // this partition has previously moved
                val existingPair = removeMovementRecordOfPartition(partition)
                assert(existingPair!!.dstMemberId == oldConsumer)
                if (existingPair.srcMemberId != newConsumer) {
                    // the partition is not moving back to its previous consumer
                    // return new ConsumerPair2(existingPair.src, newConsumer);
                    addPartitionMovementRecord(partition, ConsumerPair(existingPair.srcMemberId, newConsumer))
                }
            } else addPartitionMovementRecord(partition, pair)
        }

        fun getTheActualPartitionToBeMoved(
            partition: TopicPartition,
            oldConsumer: String,
            newConsumer: String,
        ): TopicPartition {
            var oldConsumer = oldConsumer
            val topic = partition.topic

            if (!partitionMovementsByTopic.containsKey(topic)) return partition

            if (partitionMovements.containsKey(partition)) {
                // this partition has previously moved
                assert(oldConsumer == partitionMovements[partition]!!.dstMemberId)
                oldConsumer = partitionMovements[partition]!!.srcMemberId
            }

            val partitionMovementsForThisTopic = partitionMovementsByTopic[topic]!!
            val reversePair = ConsumerPair(newConsumer, oldConsumer)

            return if (!partitionMovementsForThisTopic.containsKey(reversePair)) partition
            else partitionMovementsForThisTopic[reversePair]!!.first()
        }

        private fun isLinked(
            src: String,
            dst: String,
            pairs: Set<ConsumerPair>,
            currentPath: MutableList<String>,
        ): Boolean {
            if (src == dst) return false
            if (pairs.isEmpty()) return false
            if (ConsumerPair(src, dst).`in`(pairs)) {
                currentPath.add(src)
                currentPath.add(dst)
                return true
            }
            for (pair in pairs) if (pair.srcMemberId == src) {
                val reducedSet = pairs.toMutableSet()
                reducedSet.remove(pair)
                currentPath.add(pair.srcMemberId)
                return isLinked(pair.dstMemberId, dst, reducedSet, currentPath)
            }
            return false
        }

        private fun `in`(cycle: List<String>, cycles: Set<List<String>>): Boolean {
            val superCycle = cycle.toMutableList()
            superCycle.removeAt(superCycle.size - 1)
            superCycle.addAll(cycle)

            return cycles.any { foundCycle ->
                foundCycle.size == cycle.size && Collections.indexOfSubList(superCycle, foundCycle) != -1
            }
        }

        private fun hasCycles(pairs: Set<ConsumerPair>): Boolean {
            val cycles = mutableSetOf<List<String>>()
            for (pair in pairs) {
                val reducedPairs = pairs.toMutableSet()
                reducedPairs.remove(pair)
                val path = mutableListOf(pair.srcMemberId)
                if (isLinked(pair.dstMemberId, pair.srcMemberId, reducedPairs, path) && !`in`(path, cycles)) {
                    cycles.add(path.toList())
                    log.error("A cycle of length {} was found: {}", path.size - 1, path)
                }
            }

            // for now we want to make sure there is no partition movements of the same topic between
            // a pair of consumers. the odds of finding a cycle among more than two consumers seem to be very low
            // (according to various randomized tests with the given sticky algorithm) that it should not worth
            // the added complexity of handling those cases.

            return cycles.any { cycle -> cycle.size == 3 } // indicates a cycle of length 2
        }

        fun isSticky(): Boolean {
            for ((key, value) in partitionMovementsByTopic) {
                val topicMovementPairs = value.keys
                if (hasCycles(topicMovementPairs)) {
                    log.error(
                        "Stickiness is violated for topic {}"
                                + "\nPartition movements for this topic occurred among the following consumer pairs:"
                                + "\n{}", key, value.toString()
                    )
                    return false
                }
            }
            return true
        }
    }

    /**
     * `ConsumerPair` represents a pair of Kafka consumer ids involved in a partition reassignment.
     * Each `ConsumerPair` object, which contains a source (`src`) and a destination (`dst`)
     * element, normally corresponds to a particular partition or topic, and indicates that the
     * particular partition or some partition of the particular topic was moved from the source
     * consumer to the destination consumer during the rebalance. This class is used, through the
     * `PartitionMovements` class, by the sticky assignor and helps in determining whether a
     * partition reassignment results in cycles among the generated graph of consumer pairs.
     */
    private data class ConsumerPair(
        val srcMemberId: String,
        val dstMemberId: String,
    ) {
        override fun toString(): String = "$srcMemberId->$dstMemberId"

        fun `in`(pairs: Set<ConsumerPair>): Boolean = pairs.any { this == it }
    }

    private inner class RackInfo(
        partitionInfos: List<PartitionInfo>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ) {

        var consumerRacks: Map<String, String>? = null

        var partitionRacks: Map<TopicPartition, Set<String?>>? = null

        private val numConsumersByPartition: Map<TopicPartition, Int>

        init {
            val consumers = subscriptions.values.toList()
            val consumersByRack = mutableMapOf<String, MutableList<String>>()
            subscriptions.forEach { (memberId, subscription) ->
                subscription.rackId?.let { put(consumersByRack, it, memberId) }
            }
            val partitionsByRack: Map<String, MutableList<TopicPartition>>
            val partitionRacks: Map<TopicPartition, Set<String>>
            if (consumersByRack.isEmpty()) {
                partitionsByRack = emptyMap()
                partitionRacks = emptyMap()
            } else {
                partitionRacks = mutableMapOf()
                partitionsByRack = mutableMapOf()
                partitionInfos.forEach { partitionInfo ->
                    val tp = TopicPartition(partitionInfo.topic, partitionInfo.partition)
                    val racks = mutableSetOf<String>()
                    partitionRacks.put(tp, racks)
                    partitionInfo.replicas
                        .mapNotNull { it.rack }
                        .distinct()
                        .forEach { rackId ->
                            put(partitionsByRack, rackId, tp)
                            racks.add(rackId)
                        }
                }
            }
            if (useRackAwareAssignment(consumersByRack.keys, partitionsByRack.keys, partitionRacks)) {
                val consumerRacks = mutableMapOf<String, String>()
                consumersByRack.forEach { (rack, rackConsumers) ->
                    rackConsumers.forEach { consumer -> consumerRacks[consumer] = rack }
                }
                this.consumerRacks = consumerRacks
                this.partitionRacks = partitionRacks
            } else {
                this.consumerRacks = emptyMap()
                this.partitionRacks = emptyMap()
            }
            numConsumersByPartition = partitionRacks.mapValues { (_, value) ->
                value.sumOf { r -> consumersByRack[r]?.size ?: 0 }
            }
        }

        fun racksMismatch(consumer: String, tp: TopicPartition): Boolean {
            val consumerRack = consumerRacks!![consumer]
            val replicaRacks = partitionRacks!![tp]
            return consumerRack != null && (replicaRacks == null || !replicaRacks.contains(consumerRack))
        }

        fun sortPartitionsByRackConsumers(partitions: MutableList<TopicPartition>): MutableList<TopicPartition> {
            if (numConsumersByPartition.isEmpty()) return partitions

            // Return a sorted linked list of partitions to enable fast updates during rack-aware assignment
            return partitions.sortedWith(Comparator.comparing { tp -> numConsumersByPartition[tp] ?: 0 })
                .toMutableList()
        }

        fun nextRackConsumer(tp: TopicPartition, consumerList: List<String>, firstIndex: Int): Int {
            val racks = partitionRacks!![tp]
            if (racks.isNullOrEmpty()) return -1
            for (i in consumerList.indices) {
                val index = (firstIndex + i) % consumerList.size
                val consumer = consumerList[index]
                val consumerRack = consumerRacks!![consumer]
                if (consumerRack != null && racks.contains(consumerRack)) return index
            }
            return -1
        }

        override fun toString(): String = "RackInfo(consumerRacks=$consumerRacks, partitionRacks=$partitionRacks)"
    }

    private abstract class AbstractAssignmentBuilder(
        val partitionsPerTopic: Map<String, List<PartitionInfo>>,
        val rackInfo: RackInfo,
        val currentAssignment: MutableMap<String, MutableList<TopicPartition>>,
    ) {
        val totalPartitionsCount: Int = partitionsPerTopic.values.sumOf { it.size }

        /**
         * Builds the assignment.
         *
         * @return Map from each member to the list of partitions assigned to them.
         */
        abstract fun build(): MutableMap<String, MutableList<TopicPartition>>

        protected fun getAllTopicPartitions(sortedAllTopics: List<String>): MutableList<TopicPartition> {
            return sortedAllTopics.flatMap { topic ->
                partitionsPerTopic[topic]!!.map { partitionInfo ->
                    TopicPartition(partitionInfo.topic, partitionInfo.partition)
                }
            }.toMutableList()
        }
    }

    /**
     * This constrained assignment optimizes the assignment algorithm when all consumers were subscribed
     * to same set of topics. The method includes the following steps:
     *
     * 1. Reassign previously owned partitions:
     *
     *    a. if owned less than minQuota partitions, just assign all owned partitions, and put the member
     *       into unfilled member list.
     *
     *    b. if owned maxQuota or more, and we're still under the number of expected max capacity members,
     *       assign maxQuota partitions
     *
     *    c. if owned at least "minQuota" of partitions, assign minQuota partitions, and put the member
     *       into unfilled member list if we're still under the number of expected max capacity members
     *
     *    If using rack-aware algorithm, only owned partitions with matching racks are allocated in this step.
     *
     * 2. Fill remaining members with rack matching up to the expected numbers of maxQuota partitions,
     *    otherwise, to minQuota partitions. Partitions that cannot be aligned on racks within the quota
     *    are not assigned in this step. This step is only used if rack-aware.
     *
     * 3. Fill remaining members up to the expected numbers of maxQuota partitions, otherwise, to minQuota partitions.
     *    For rack-aware algorithm, these are partitions that could not be aligned on racks within
     *    the balancing constraints.
     *
     * @constructor Constructs a constrained assignment builder.
     * @param partitionsPerTopic The partitions for each subscribed topic
     * @param rackInfo Rack information for consumers and racks
     * @param consumerToOwnedPartitions Each consumer's previously owned and still-subscribed partitions
     * @param partitionsWithMultiplePreviousOwners The partitions being claimed in the previous assignment
     * of multiple consumers
     */
    private inner class ConstrainedAssignmentBuilder(
        partitionsPerTopic: Map<String, MutableList<PartitionInfo>>,
        rackInfo: RackInfo,
        consumerToOwnedPartitions: MutableMap<String, MutableList<TopicPartition>>,
        val partitionsWithMultiplePreviousOwners: Set<TopicPartition>,
    ) : AbstractAssignmentBuilder(
        partitionsPerTopic = partitionsPerTopic,
        rackInfo = rackInfo,
        currentAssignment = consumerToOwnedPartitions,
    ) {
        var allRevokedPartitions = mutableSetOf<TopicPartition>()

        // the consumers which may still be assigned one or more partitions to reach expected capacity
        var unfilledMembersWithUnderMinQuotaPartitions = mutableListOf<String>()
        var unfilledMembersWithExactlyMinQuotaPartitions = LinkedList<String>()

        val minQuota: Int
        val maxQuota: Int

        // the expected number of members receiving more than minQuota partitions (zero when minQuota == maxQuota)
        val expectedNumMembersWithOverMinQuotaPartitions: Int

        // the current number of members receiving more than minQuota partitions (zero when minQuota == maxQuota)
        var currentNumMembersWithOverMinQuotaPartitions = 0

        val assignment: MutableMap<String, MutableList<TopicPartition>>
        val assignedPartitions: MutableList<TopicPartition>

        init {
            val numberOfConsumers = consumerToOwnedPartitions.size
            minQuota = floor(totalPartitionsCount.toDouble() / numberOfConsumers).toInt()
            maxQuota = ceil(totalPartitionsCount.toDouble() / numberOfConsumers).toInt()
            expectedNumMembersWithOverMinQuotaPartitions = totalPartitionsCount % numberOfConsumers

            // initialize the assignment map with an empty array of size maxQuota for all members
            assignment = consumerToOwnedPartitions.keys
                .associateWith<String, MutableList<TopicPartition>> { ArrayList(maxQuota) }
                .toMutableMap()

            assignedPartitions = mutableListOf()
        }

        override fun build(): MutableMap<String, MutableList<TopicPartition>> {
            if (log.isDebugEnabled) log.debug(
                "Performing constrained assign with partitionsPerTopic: {}, currentAssignment: {}, rackInfo {}.",
                partitionsPerTopic, currentAssignment, rackInfo
            )

            assignOwnedPartitions()

            var unassignedPartitions = getUnassignedPartitions(assignedPartitions)

            if (log.isDebugEnabled) {
                log.debug(
                    "After reassigning previously owned partitions, unfilled members: {}, unassigned partitions: {}, " +
                            "current assignment: {}",
                    unfilledMembersWithUnderMinQuotaPartitions,
                    unassignedPartitions,
                    assignment
                )
            }

            unfilledMembersWithUnderMinQuotaPartitions.sort()
            unfilledMembersWithExactlyMinQuotaPartitions.sort()
            unassignedPartitions = rackInfo.sortPartitionsByRackConsumers(unassignedPartitions)

            assignRackAwareRoundRobin(unassignedPartitions)
            assignRoundRobin(unassignedPartitions)
            verifyUnfilledMembers()

            log.info("Final assignment of partitions to consumers: \n{}", assignment)

            return assignment
        }

        // Reassign previously owned partitions, up to the expected number of partitions per consumer
        fun assignOwnedPartitions() {
            for ((consumer, partitions) in currentAssignment.entries) {
                val ownedPartitions = partitions
                    .filter { tp -> !rackInfo.racksMismatch(consumer, tp) }
                    .toMutableList()

                val consumerAssignment = assignment[consumer]!!

                for (doublyClaimedPartition in partitionsWithMultiplePreviousOwners) {
                    if (ownedPartitions.contains(doublyClaimedPartition)) {
                        log.error(
                            "Found partition {} still claimed as owned by consumer {}, despite being claimed by multiple "
                                    + "consumers already in the same generation. Removing it from the ownedPartitions",
                            doublyClaimedPartition, consumer
                        )
                        ownedPartitions.remove(doublyClaimedPartition)
                    }
                }

                if (ownedPartitions.size < minQuota) {
                    // the expected assignment size is more than this consumer has now, so keep all the owned partitions
                    // and put this member into the unfilled member list
                    if (ownedPartitions.size > 0) {
                        consumerAssignment.addAll(ownedPartitions)
                        assignedPartitions.addAll(ownedPartitions)
                    }
                    unfilledMembersWithUnderMinQuotaPartitions.add(consumer)
                } else if (
                    ownedPartitions.size >= maxQuota
                    && currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions
                ) {
                    // consumer owned the "maxQuota" of partitions or more, and we're still under the number of expected members
                    // with more than the minQuota partitions, so keep "maxQuota" of the owned partitions, and revoke the rest of the partitions
                    currentNumMembersWithOverMinQuotaPartitions++
                    if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions)
                        unfilledMembersWithExactlyMinQuotaPartitions.clear()

                    val maxQuotaPartitions: List<TopicPartition> = ownedPartitions.subList(0, maxQuota)
                    consumerAssignment.addAll(maxQuotaPartitions)
                    assignedPartitions.addAll(maxQuotaPartitions)
                    allRevokedPartitions.addAll(ownedPartitions.subList(maxQuota, ownedPartitions.size))
                } else {
                    // consumer owned at least "minQuota" of partitions
                    // so keep "minQuota" of the owned partitions, and revoke the rest of the partitions
                    val minQuotaPartitions: List<TopicPartition> = ownedPartitions.subList(0, minQuota)
                    consumerAssignment.addAll(minQuotaPartitions)
                    assignedPartitions.addAll(minQuotaPartitions)
                    allRevokedPartitions.addAll(ownedPartitions.subList(minQuota, ownedPartitions.size))
                    // this consumer is potential maxQuota candidate since we're still under the number of expected members
                    // with more than the minQuota partitions. Note, if the number of expected members with more than
                    // the minQuota partitions is 0, it means minQuota == maxQuota, and there are no potentially unfilled
                    if (currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions) {
                        unfilledMembersWithExactlyMinQuotaPartitions.add(consumer)
                    }
                }
            }
        }

        // Round-Robin filling within racks for remaining members up to the expected numbers of maxQuota,
        // otherwise, to minQuota
        private fun assignRackAwareRoundRobin(unassignedPartitions: MutableList<TopicPartition>) {
            if (rackInfo.consumerRacks!!.isEmpty()) return

            var nextUnfilledConsumerIndex = 0
            val unassignedIter = unassignedPartitions.iterator()

            while (unassignedIter.hasNext()) {
                val unassignedPartition = unassignedIter.next()
                var consumer: String? = null
                var nextIndex = rackInfo.nextRackConsumer(
                    tp = unassignedPartition,
                    consumerList = unfilledMembersWithUnderMinQuotaPartitions,
                    firstIndex = nextUnfilledConsumerIndex,
                )
                if (nextIndex >= 0) {
                    consumer = unfilledMembersWithUnderMinQuotaPartitions[nextIndex]
                    val assignmentCount = assignment[consumer]!!.size + 1
                    if (assignmentCount >= minQuota) {
                        unfilledMembersWithUnderMinQuotaPartitions.remove(consumer)
                        if (assignmentCount < maxQuota) unfilledMembersWithExactlyMinQuotaPartitions.add(consumer)
                    } else nextIndex++

                    nextUnfilledConsumerIndex =
                        if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) 0
                        else nextIndex % unfilledMembersWithUnderMinQuotaPartitions.size
                } else if (unfilledMembersWithExactlyMinQuotaPartitions.isNotEmpty()) {
                    val firstIndex = rackInfo.nextRackConsumer(
                        tp = unassignedPartition,
                        consumerList = unfilledMembersWithExactlyMinQuotaPartitions,
                        firstIndex = 0,
                    )
                    if (firstIndex >= 0) {
                        consumer = unfilledMembersWithExactlyMinQuotaPartitions[firstIndex]
                        if (assignment[consumer]!!.size + 1 == maxQuota)
                            unfilledMembersWithExactlyMinQuotaPartitions.removeAt(firstIndex)
                    }
                }
                if (consumer != null) {
                    assignNewPartition(unassignedPartition, consumer)
                    unassignedIter.remove()
                }
            }
        }

        private fun assignRoundRobin(unassignedPartitions: List<TopicPartition>) {
            var unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator()

            // Round-Robin filling remaining members up to the expected numbers of maxQuota, otherwise, to minQuota
            for (unassignedPartition in unassignedPartitions) {
                val consumer = if (unfilledConsumerIter.hasNext()) unfilledConsumerIter.next()
                else {
                    if (
                        unfilledMembersWithUnderMinQuotaPartitions.isEmpty()
                        && unfilledMembersWithExactlyMinQuotaPartitions.isEmpty()
                    ) {
                        // Should not enter here since we have calculated the exact number to assign to each consumer.
                        // This indicates issues in the assignment algorithm
                        val currentPartitionIndex = unassignedPartitions.indexOf(unassignedPartition)
                        log.error(
                            "No more unfilled consumers to be assigned. The remaining unassigned partitions are: {}",
                            unassignedPartitions.subList(currentPartitionIndex, unassignedPartitions.size)
                        )
                        throw IllegalStateException("No more unfilled consumers to be assigned.")
                    } else if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) {
                        unfilledMembersWithExactlyMinQuotaPartitions.poll()
                    } else {
                        unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator()
                        unfilledConsumerIter.next()
                    }
                }

                val currentAssignedCount = assignNewPartition(unassignedPartition, consumer)

                if (currentAssignedCount == minQuota) {
                    unfilledConsumerIter.remove()
                    unfilledMembersWithExactlyMinQuotaPartitions.add(consumer)
                } else if (currentAssignedCount == maxQuota) {
                    currentNumMembersWithOverMinQuotaPartitions++
                    if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                        // We only start to iterate over the "potentially unfilled" members at minQuota after we've filled
                        // all members up to at least minQuota, so once the last minQuota member reaches maxQuota, we
                        // should be done. But in case of some algorithmic error, just log a warning and continue to
                        // assign any remaining partitions within the assignment constraints
                        if (unassignedPartitions.indexOf(unassignedPartition) != unassignedPartitions.size - 1) {
                            log.error(
                                "Filled the last member up to maxQuota but still had partitions remaining to assign, "
                                        + "will continue but this indicates a bug in the assignment."
                            )
                        }
                    }
                }
            }
        }

        private fun assignNewPartition(unassignedPartition: TopicPartition, consumer: String): Int {
            val consumerAssignment = assignment[consumer]
            consumerAssignment!!.add(unassignedPartition)

            // We already assigned all possible ownedPartitions, so we know this must be newly assigned to this consumer
            // or else the partition was actually claimed by multiple previous owners and had to be invalidated from all
            // members claimed ownedPartitions
            if (
                allRevokedPartitions.contains(unassignedPartition)
                || partitionsWithMultiplePreviousOwners.contains(unassignedPartition)
            ) partitionsTransferringOwnership!![unassignedPartition] = consumer

            return consumerAssignment.size
        }

        private fun verifyUnfilledMembers() {
            if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) return

            // we expected all the remaining unfilled members have minQuota partitions and we're already
            // at the expected number of members with more than the minQuota partitions. Otherwise,
            // there must be error here.
            if (currentNumMembersWithOverMinQuotaPartitions != expectedNumMembersWithOverMinQuotaPartitions) {
                log.error(
                    "Current number of members with more than the minQuota partitions: {}, is less than the " +
                            "expected number of members with more than the minQuota partitions: {}, " +
                            "and no more partitions to be assigned to the remaining unfilled consumers: {}",
                    currentNumMembersWithOverMinQuotaPartitions,
                    expectedNumMembersWithOverMinQuotaPartitions,
                    unfilledMembersWithUnderMinQuotaPartitions
                )
                throw IllegalStateException(
                    "We haven't reached the expected number of members with more than " +
                            "the minQuota partitions, but no more partitions to be assigned"
                )
            } else {
                for (unfilledMember in unfilledMembersWithUnderMinQuotaPartitions) {
                    val assignedPartitionsCount = assignment[unfilledMember]!!.size
                    if (assignedPartitionsCount != minQuota) {
                        log.error(
                            "Consumer: [{}] should have {} partitions, but got {} partitions, and no more partitions " +
                                    "to be assigned. The remaining unfilled consumers are: {}",
                            unfilledMember,
                            minQuota,
                            assignedPartitionsCount,
                            unfilledMembersWithUnderMinQuotaPartitions
                        )
                        throw IllegalStateException(
                            "Consumer: [$unfilledMember] doesn't reach minQuota partitions, " +
                                    "and no more partitions to be assigned"
                        )
                    } else log.trace(
                        "skip over this unfilled member: [{}] because we've reached the expected number of " +
                                "members with more than the minQuota partitions, and this member already has minQuota partitions",
                        unfilledMember
                    )
                }
            }
        }

        /**
         * get the unassigned partition list by computing the difference set of all sorted partitions
         * and sortedAssignedPartitions. If no assigned partitions, we'll just return all sorted topic partitions.
         *
         * To compute the difference set, we use two pointers technique here:
         *
         * We loop through the all sorted topics, and then iterate all partitions the topic has, compared with
         * the ith element in sortedAssignedPartitions(i starts from 0):
         * - if not equal to the ith element, add to unassignedPartitions
         * - if equal to the ith element, get next element from sortedAssignedPartitions
         *
         * @param sortedAssignedPartitions sorted partitions, all are included in the sortedPartitions
         * @return the partitions not yet assigned to any consumers
         */
        private fun getUnassignedPartitions(
            sortedAssignedPartitions: MutableList<TopicPartition>,
        ): MutableList<TopicPartition> {
            // sort all topics first, then we can have sorted all topic partitions by adding partitions starting from 0
            val sortedAllTopics = partitionsPerTopic.keys.sorted()

            if (sortedAssignedPartitions.isEmpty()) {
                // no assigned partitions means all partitions are unassigned partitions
                return getAllTopicPartitions(sortedAllTopics)
            }
            val unassignedPartitions: MutableList<TopicPartition> =
                ArrayList(totalPartitionsCount - sortedAssignedPartitions.size)

            sortedAssignedPartitions.sortWith(
                Comparator
                    .comparing { obj: TopicPartition -> obj.topic }
                    .thenComparing { obj: TopicPartition -> obj.partition }
            )
            var shouldAddDirectly = false
            val sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator()
            var nextAssignedPartition = sortedAssignedPartitionsIter.next()
            for (topic in sortedAllTopics) {
                val partitionCount = partitionsPerTopic[topic]!!.size
                for (i in 0..<partitionCount) {
                    if (
                        shouldAddDirectly
                        || !(nextAssignedPartition.topic == topic && nextAssignedPartition.partition == i)
                    ) unassignedPartitions.add(TopicPartition(topic, i))
                    else {
                        // this partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition
                        if (sortedAssignedPartitionsIter.hasNext()) {
                            nextAssignedPartition = sortedAssignedPartitionsIter.next()
                        } else {
                            // add the remaining directly since there is no more sortedAssignedPartitions
                            shouldAddDirectly = true
                        }
                    }
                }
            }
            return unassignedPartitions
        }
    }


    /**
     * This general assignment algorithm guarantees the assignment that is as balanced as possible.
     * This method includes the following steps:
     *
     * 1. Preserving all the existing partition assignments. If rack-aware algorithm is used, only assignments
     *    within racks are preserved.
     * 2. Removing all the partition assignments that have become invalid due to the change that triggers
     *    the reassignment. Partition assignments with mismatched racks are also removed.
     * 3. Assigning the unassigned partitions in a way that balances out the overall assignments of partitions
     *    to consumers. while preserving rack-alignment. This step is used only for rack-aware assignment.
     * 4. Assigning the remaining unassigned partitions in a way that balances out the overall assignments
     *    of partitions to consumers. For rack-aware algorithm, these are partitions that could not be aligned
     *    on racks within the balancing constraints.
     * 5. Further balancing out the resulting assignment by finding the partitions that can be reassigned
     *    to another consumer towards an overall more balanced assignment. For rack-aware algorithm, attempt
     *    to retain rack alignment if possible.
     *
     * @constructor Constructs a general assignment builder.
     * @param partitionsPerTopic The partitions for each subscribed topic.
     * @param subscriptions Map from the member id to their respective topic subscription
     * @param currentAssignment Each consumer's previously owned and still-subscribed partitions
     * @param rackInfo Rack information for consumers and partitions
     */
    private inner class GeneralAssignmentBuilder(
        partitionsPerTopic: Map<String, List<PartitionInfo>>,
        rackInfo: RackInfo,
        currentAssignment: MutableMap<String, MutableList<TopicPartition>>,
        private val subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ) : AbstractAssignmentBuilder(partitionsPerTopic, rackInfo, currentAssignment) {

        // a mapping of all topics to all consumers that can be assigned to them
        private val topic2AllPotentialConsumers: MutableMap<String, MutableList<String>>

        // a mapping of all consumers to all potential topics that can be assigned to them
        private val consumer2AllPotentialTopics: MutableMap<String, List<String>>

        // a mapping of partition to current consumer
        private val currentPartitionConsumer: MutableMap<TopicPartition?, String?>
        private val sortedAllPartitions: MutableList<TopicPartition>

        // an ascending sorted set of consumers based on how many topic partitions are already assigned to them
        private val sortedCurrentSubscriptions: TreeSet<String>
        private var revocationRequired = false

        init {
            topic2AllPotentialConsumers = HashMap(partitionsPerTopic.keys.size)
            consumer2AllPotentialTopics = HashMap(subscriptions.keys.size)

            // initialize topic2AllPotentialConsumers and consumer2AllPotentialTopics
            partitionsPerTopic.keys.forEach { topicName -> topic2AllPotentialConsumers[topicName] = mutableListOf() }
            subscriptions.forEach { (consumerId, subscription) ->
                val subscribedTopics: MutableList<String> = ArrayList(subscription.topics.size)
                consumer2AllPotentialTopics[consumerId] = subscribedTopics
                subscription.topics.filter { topic -> partitionsPerTopic[topic] != null }
                    .forEach { topic ->
                        subscribedTopics.add(topic)
                        topic2AllPotentialConsumers[topic]!!.add(consumerId)
                    }

                // add this consumer to currentAssignment (with an empty topic partition assignment) if it does
                // not already exist
                if (!currentAssignment.containsKey(consumerId)) currentAssignment[consumerId] = mutableListOf()
            }
            currentPartitionConsumer = mutableMapOf()
            for ((key, value) in currentAssignment)
                for (topicPartition in value) currentPartitionConsumer[topicPartition] = key

            val sortedAllTopics = topic2AllPotentialConsumers.keys.toMutableList()
            sortedAllTopics.sortWith(TopicComparator(topic2AllPotentialConsumers))

            sortedAllPartitions = getAllTopicPartitions(sortedAllTopics)
            sortedCurrentSubscriptions = TreeSet(SubscriptionComparator(currentAssignment))
        }

        override fun build(): MutableMap<String, MutableList<TopicPartition>> {
            if (log.isDebugEnabled) {
                log.debug(
                    "performing general assign. partitionsPerTopic: {}, subscriptions: {}, " +
                            "currentAssignment: {}, rackInfo: {}",
                    partitionsPerTopic, subscriptions, currentAssignment, rackInfo
                )
            }
            val prevAssignment: MutableMap<TopicPartition, ConsumerGenerationPair> = java.util.HashMap()
            partitionMovements = PartitionMovements()
            prepopulateCurrentAssignments(prevAssignment)

            // the partitions already assigned in current assignment
            val assignedPartitions = assignOwnedPartitions()

            // all partitions that still need to be assigned
            val unassignedPartitions = getUnassignedPartitions(assignedPartitions)
            if (log.isDebugEnabled) {
                log.debug("unassigned Partitions: {}", unassignedPartitions)
            }

            // at this point we have preserved all valid topic partition to consumer assignments and removed
            // all invalid topic partitions and invalid consumers. Now we need to assign unassignedPartitions
            // to consumers so that the topic partition assignments are as balanced as possible.
            sortedCurrentSubscriptions.addAll(currentAssignment.keys)
            balance(prevAssignment, unassignedPartitions)
            log.info("Final assignment of partitions to consumers: \n{}", currentAssignment)
            return currentAssignment
        }

        private fun assignOwnedPartitions(): MutableList<TopicPartition> {
            val assignedPartitions = mutableListOf<TopicPartition>()
            val iterator = currentAssignment.iterator()
            while (iterator.hasNext()) {
                val (consumer, value) = iterator.next()
                val consumerSubscription = subscriptions[consumer]
                if (consumerSubscription == null) {
                    // if a consumer that existed before (and had some partition assignments) is now removed,
                    // remove it from currentAssignment
                    for (topicPartition in value) currentPartitionConsumer.remove(topicPartition)
                    iterator.remove()
                } else {
                    // otherwise (the consumer still exists)
                    val partitionIter = value.iterator()
                    while (partitionIter.hasNext()) {
                        val partition = partitionIter.next()
                        if (!topic2AllPotentialConsumers.containsKey(partition.topic)) {
                            // if this topic partition of this consumer no longer exists, remove it from
                            // currentAssignment of the consumer
                            partitionIter.remove()
                            currentPartitionConsumer.remove(partition)
                        } else if (
                            !consumerSubscription.topics.contains(partition.topic)
                            || rackInfo.racksMismatch(consumer, partition)
                        ) {
                            // if the consumer is no longer subscribed to its topic or if racks don't match
                            // for rack-aware assignment, remove it from currentAssignment of the consumer
                            partitionIter.remove()
                            revocationRequired = true
                        } else {
                            // otherwise, remove the topic partition from those that need to be assigned only if
                            // its current consumer is still subscribed to its topic (because it is already assigned
                            // and we would want to preserve that assignment as much as possible)
                            assignedPartitions.add(partition)
                        }
                    }
                }
            }
            return assignedPartitions
        }

        /**
         * get the unassigned partition list by computing the difference set of the sortedPartitions(all partitions)
         * and sortedAssignedPartitions. If no assigned partitions, we'll just return all sorted topic partitions.
         *
         * We loop the sortedPartition, and compare the ith element in sortedAssignedPartitions(i start from 0):
         * - if not equal to the ith element, add to unassignedPartitions
         * - if equal to the ith element, get next element from sortedAssignedPartitions
         *
         * @param sortedAssignedPartitions: sorted partitions, all are included in the sortedPartitions
         * @return partitions that aren't assigned to any current consumer
         */
        private fun getUnassignedPartitions(sortedAssignedPartitions: MutableList<TopicPartition>): MutableList<TopicPartition> {
            if (sortedAssignedPartitions.isEmpty()) return sortedAllPartitions

            val unassignedPartitions = mutableListOf<TopicPartition>()
            sortedAssignedPartitions.sortWith(PartitionComparator(topic2AllPotentialConsumers))

            var shouldAddDirectly = false
            val sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator()
            var nextAssignedPartition = sortedAssignedPartitionsIter.next()

            for (topicPartition in sortedAllPartitions) {
                if (shouldAddDirectly || nextAssignedPartition != topicPartition)
                    unassignedPartitions.add(topicPartition)
                else {
                    // this partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition
                    if (sortedAssignedPartitionsIter.hasNext())
                        nextAssignedPartition = sortedAssignedPartitionsIter.next()
                    else
                    // add the remaining directly since there is no more sortedAssignedPartitions
                        shouldAddDirectly = true
                }
            }
            return unassignedPartitions
        }

        /**
         * update the prevAssignment with the partitions, consumer and generation in parameters
         *
         * @param partitions: The partitions to be updated the prevAssignment
         * @param consumer: The consumer Id
         * @param prevAssignment: The assignment contains the assignment with the 2nd largest generation
         * @param generation: The generation of this assignment (partitions)
         */
        private fun updatePrevAssignment(
            prevAssignment: MutableMap<TopicPartition, ConsumerGenerationPair>,
            partitions: List<TopicPartition>,
            consumer: String,
            generation: Int,
        ) {
            for (partition in partitions) {
                if (prevAssignment.containsKey(partition)) {
                    // only keep the latest previous assignment
                    if (generation > prevAssignment[partition]!!.generation)
                        prevAssignment[partition] = ConsumerGenerationPair(consumer, generation)
                } else prevAssignment[partition] = ConsumerGenerationPair(consumer, generation)
            }
        }

        /**
         * filling in the prevAssignment from the subscriptions.
         *
         * @param prevAssignment: The assignment contains the assignment with the 2nd largest generation
         */
        private fun prepopulateCurrentAssignments(prevAssignment: MutableMap<TopicPartition, ConsumerGenerationPair>) {
            // we need to process subscriptions' user data with each consumer's reported generation in mind
            // higher generations overwrite lower generations in case of a conflict
            // note that a conflict could exist only if user data is for different generations
            for ((consumer, subscription) in subscriptions) {
                if (subscription.userData != null) {
                    // since this is our 2nd time to deserialize memberData, rewind userData is necessary
                    subscription.userData.rewind()
                }
                val (partitions, generation) = memberData(subscription)

                // we already have the maxGeneration info, so just compare the current generation of memberData, and put into prevAssignment
                if (generation != null && generation < maxGeneration) {
                    // if the current member's generation is lower than maxGeneration, put into prevAssignment if needed
                    updatePrevAssignment(
                        prevAssignment = prevAssignment,
                        partitions = partitions,
                        consumer = consumer,
                        generation = generation,
                    )
                } else if (generation == null && maxGeneration > DEFAULT_GENERATION) {
                    // if maxGeneration is larger than DEFAULT_GENERATION
                    // put all (no generation) partitions as DEFAULT_GENERATION into prevAssignment if needed
                    updatePrevAssignment(
                        prevAssignment = prevAssignment,
                        partitions = partitions,
                        consumer = consumer,
                        generation = DEFAULT_GENERATION,
                    )
                }
            }
        }

        /**
         * determine if the current assignment is a balanced one
         *
         * @return true if the given assignment is balanced; false otherwise
         */
        private val isBalanced: Boolean
            get() {
                val min = currentAssignment[sortedCurrentSubscriptions.first()]!!.size
                val max = currentAssignment[sortedCurrentSubscriptions.last()]!!.size
                // if minimum and maximum numbers of partitions assigned to consumers differ by at most one return true
                if (min >= max - 1) return true

                // create a mapping from partitions to the consumer assigned to them
                val allPartitions: MutableMap<TopicPartition, String> = java.util.HashMap()
                val assignments = currentAssignment.entries
                for ((key, topicPartitions) in assignments) {
                    for (topicPartition in topicPartitions) {
                        if (allPartitions.containsKey(topicPartition)) log.error(
                            "{} is assigned to more than one consumer.",
                            topicPartition
                        )
                        allPartitions[topicPartition] = key
                    }
                }

                // for each consumer that does not have all the topic partitions it can get make sure none
                // of the topic partitions it could but did not get cannot be moved to it (because that
                // would break the balance)
                for (consumer in sortedCurrentSubscriptions) {
                    val consumerPartitions = currentAssignment[consumer]
                    val consumerPartitionCount = consumerPartitions!!.size

                    // skip if this consumer already has all the topic partitions it can get
                    val allSubscribedTopics = consumer2AllPotentialTopics[consumer]!!
                    val maxAssignmentSize = getMaxAssignmentSize(allSubscribedTopics)
                    if (consumerPartitionCount == maxAssignmentSize) continue

                    // otherwise make sure it cannot get any more
                    for (topic in allSubscribedTopics) {
                        val partitionCount = partitionsPerTopic[topic]!!.size
                        for (i in 0..<partitionCount) {
                            val topicPartition = TopicPartition(topic, i)
                            if (!currentAssignment[consumer]!!.contains(topicPartition)) {
                                val otherConsumer = allPartitions[topicPartition]
                                val otherConsumerPartitionCount = currentAssignment[otherConsumer]!!.size
                                if (consumerPartitionCount + 1 < otherConsumerPartitionCount) {
                                    log.debug(
                                        "{} can be moved from consumer {} to consumer {} for a more balanced assignment.",
                                        topicPartition, otherConsumer, consumer
                                    )
                                    return false
                                }
                            }
                        }
                    }
                }
                return true
            }

        /**
         * get the maximum assigned partition size of the `allSubscribedTopics`
         *
         * @param allSubscribedTopics the subscribed topics of a consumer
         * @return maximum assigned partition size
         */
        private fun getMaxAssignmentSize(allSubscribedTopics: List<String>): Int {
            return if (allSubscribedTopics.size == partitionsPerTopic.size) totalPartitionsCount
            else allSubscribedTopics.map { key -> partitionsPerTopic[key] }
                .sumOf { it!!.size }
        }

        /**
         * @return the balance score of the given assignment, as the sum of assigned partitions size difference
         * of all consumer pairs. A perfectly balanced assignment (with all consumers getting the same number
         * of partitions) has a balance score of 0. Lower balance score indicates a more balanced assignment.
         */
        private fun getBalanceScore(assignment: Map<String, List<TopicPartition>>): Int {
            var score = 0
            val consumer2AssignmentSize = mutableMapOf<String, Int>()
            for ((key, value) in assignment) consumer2AssignmentSize[key] = value.size
            val it = consumer2AssignmentSize.iterator()
            while (it.hasNext()) {
                val (_, consumerAssignmentSize) = it.next()
                it.remove()
                consumer2AssignmentSize.values.forEach { score += (consumerAssignmentSize - it).absoluteValue }
            }
            return score
        }

        /**
         * The assignment should improve the overall balance of the partition assignments to consumers.
         */
        private fun maybeAssignPartition(partition: TopicPartition, rackInfo: RackInfo?): Boolean {
            for (consumer in sortedCurrentSubscriptions) {
                if (
                    consumer2AllPotentialTopics[consumer]!!.contains(partition.topic)
                    && (rackInfo == null || !rackInfo.racksMismatch(consumer, partition))
                ) {
                    sortedCurrentSubscriptions.remove(consumer)
                    currentAssignment[consumer]!!.add(partition)
                    currentPartitionConsumer[partition] = consumer
                    sortedCurrentSubscriptions.add(consumer)
                    return true
                }
            }
            return false
        }

        /**
         * attempt to assign all unassigned partitions
         *
         * @param unassignedPartitions partitions that are still unassigned
         * @param rackInfo rack information used to match racks. If null, no rack-matching is performed
         * @param removeAssigned flag that indicates if assigned partitions should be removed from `unassignedPartitions`
         */
        private fun maybeAssign(
            unassignedPartitions: MutableList<TopicPartition>,
            rackInfo: RackInfo?,
            removeAssigned: Boolean,
        ) {
            // assign all unassigned partitions
            val iter = unassignedPartitions.iterator()
            while (iter.hasNext()) {
                val partition = iter.next()
                // skip if there is no potential consumer for the topic
                if (topic2AllPotentialConsumers[partition.topic]!!.isEmpty()) continue
                if (maybeAssignPartition(partition, rackInfo) && removeAssigned) iter.remove()
            }
        }

        private fun canTopicParticipateInReassignment(topic: String): Boolean {
            // if a topic has two or more potential consumers it is subject to reassignment.
            return topic2AllPotentialConsumers[topic]!!.size >= 2
        }

        private fun canConsumerParticipateInReassignment(consumer: String?): Boolean {
            val currentPartitions = currentAssignment[consumer]
            val currentAssignmentSize = currentPartitions!!.size
            val allSubscribedTopics = consumer2AllPotentialTopics[consumer]!!
            val maxAssignmentSize = getMaxAssignmentSize(allSubscribedTopics)
            if (currentAssignmentSize > maxAssignmentSize) log.error(
                "The consumer {} is assigned more partitions than the maximum possible.",
                consumer
            )
            // if a consumer is not assigned all its potential partitions it is subject to reassignment
            if (currentAssignmentSize < maxAssignmentSize) return true

            // if any of the partitions assigned to a consumer is subject to reassignment the consumer itself
            // is subject to reassignment
            return currentPartitions.any { canTopicParticipateInReassignment(it.topic) }
        }

        /**
         * Balance the current assignment using the data structures created in the assignPartitions(...) method above.
         */
        private fun balance(
            prevAssignment: Map<TopicPartition, ConsumerGenerationPair>,
            unassignedPartitions: MutableList<TopicPartition>,
        ) {
            val initializing = currentAssignment[sortedCurrentSubscriptions.last()]!!.isEmpty()

            // First assign with rack matching and then assign any remaining without rack matching
            var partitionsToAssign = unassignedPartitions
            if (rackInfo.consumerRacks!!.isNotEmpty()) {
                partitionsToAssign = unassignedPartitions.toMutableList()
                maybeAssign(partitionsToAssign, rackInfo, true)
            }
            maybeAssign(partitionsToAssign, null, false)

            // narrow down the reassignment scope to only those partitions that can actually be reassigned
            val fixedPartitions: MutableSet<TopicPartition> = HashSet()
            for (topic in topic2AllPotentialConsumers.keys) if (!canTopicParticipateInReassignment(topic)) {
                for (i in partitionsPerTopic[topic]!!.indices) {
                    fixedPartitions.add(TopicPartition(topic, i))
                }
            }
            sortedAllPartitions.removeAll(fixedPartitions)
            unassignedPartitions.removeAll(fixedPartitions)

            // narrow down the reassignment scope to only those consumers that are subject to reassignment
            val fixedAssignments: MutableMap<String, MutableList<TopicPartition>> = java.util.HashMap()
            for (consumer in consumer2AllPotentialTopics.keys) if (!canConsumerParticipateInReassignment(consumer)) {
                sortedCurrentSubscriptions.remove(consumer)
                fixedAssignments[consumer] = currentAssignment.remove(consumer)!!
            }

            // create a deep copy of the current assignment so we can revert to it if we do not get a more balanced assignment later
            val preBalanceAssignment = deepCopy(currentAssignment)
            val preBalancePartitionConsumers: Map<TopicPartition?, String?> =
                java.util.HashMap(currentPartitionConsumer)

            // if we don't already need to revoke something due to subscription changes, first try to balance by only moving newly added partitions
            if (!revocationRequired) {
                performReassignments(unassignedPartitions, prevAssignment)
            }
            val reassignmentPerformed = performReassignments(sortedAllPartitions, prevAssignment)

            // if we are not preserving existing assignments and we have made changes to the current assignment
            // make sure we are getting a more balanced assignment; otherwise, revert to previous assignment
            if (!initializing && reassignmentPerformed && getBalanceScore(currentAssignment) >= getBalanceScore(
                    preBalanceAssignment
                )
            ) {
                deepCopy(preBalanceAssignment, currentAssignment)
                currentPartitionConsumer.clear()
                currentPartitionConsumer.putAll(preBalancePartitionConsumers)
            }

            // add the fixed assignments (those that could not change) back
            for ((consumer, value) in fixedAssignments) {
                currentAssignment[consumer] = value
                sortedCurrentSubscriptions.add(consumer)
            }
            fixedAssignments.clear()
        }

        private fun performReassignments(
            reassignablePartitions: List<TopicPartition>,
            prevAssignment: Map<TopicPartition, ConsumerGenerationPair>,
        ): Boolean {
            var reassignmentPerformed = false
            var modified: Boolean

            // repeat reassignment until no partition can be moved to improve the balance
            do {
                modified = false
                // reassign all reassignable partitions (starting from the partition with least potential
                // consumers and if needed) until the full list is processed or a balance is achieved
                val partitionIterator = reassignablePartitions.iterator()
                while (partitionIterator.hasNext() && !isBalanced) {
                    val partition = partitionIterator.next()

                    // the partition must have at least two consumers
                    if (topic2AllPotentialConsumers[partition.topic]!!.size <= 1) log.error(
                        "Expected more than one potential consumer for partition '{}'",
                        partition
                    )

                    // the partition must have a current consumer
                    val consumer = currentPartitionConsumer[partition]
                    if (consumer == null) log.error("Expected partition '{}' to be assigned to a consumer", partition)
                    if (prevAssignment.containsKey(partition) &&
                        currentAssignment[consumer]!!.size > currentAssignment[prevAssignment[partition]!!.consumer]!!.size + 1
                    ) {
                        reassignPartition(partition, prevAssignment[partition]!!.consumer)
                        reassignmentPerformed = true
                        modified = true
                        continue
                    }

                    // check if a better-suited consumer exist for the partition; if so, reassign it
                    // Use consumer within rack if possible
                    val consumerRack = rackInfo.consumerRacks!![consumer!!]
                    val partitionRacks = rackInfo.partitionRacks!![partition]!!
                    var foundRackConsumer = false
                    if (consumerRack != null && partitionRacks.isNotEmpty() && partitionRacks.contains(consumerRack)) {
                        for (otherConsumer in topic2AllPotentialConsumers[partition.topic]!!) {
                            val otherConsumerRack = rackInfo.consumerRacks!![otherConsumer]
                            if (otherConsumerRack == null || !partitionRacks.contains(otherConsumerRack)) continue
                            if (currentAssignment[consumer]!!.size > currentAssignment[otherConsumer]!!.size + 1) {
                                reassignPartition(partition)
                                reassignmentPerformed = true
                                modified = true
                                foundRackConsumer = true
                                break
                            }
                        }
                    }
                    if (!foundRackConsumer) {
                        for (otherConsumer in topic2AllPotentialConsumers[partition.topic]!!) {
                            if (currentAssignment[consumer]!!.size > currentAssignment[otherConsumer]!!.size + 1) {
                                reassignPartition(partition)
                                reassignmentPerformed = true
                                modified = true
                                break
                            }
                        }
                    }
                }
            } while (modified)
            return reassignmentPerformed
        }

        private fun reassignPartition(partition: TopicPartition) {
            // find the new consumer
            var newConsumer: String? = null
            for (anotherConsumer in sortedCurrentSubscriptions) {
                if (consumer2AllPotentialTopics[anotherConsumer]!!.contains(partition.topic)) {
                    newConsumer = anotherConsumer
                    break
                }
            }
            assert(newConsumer != null)
            reassignPartition(partition, newConsumer!!)
        }

        private fun reassignPartition(partition: TopicPartition, newConsumer: String) {
            val consumer = currentPartitionConsumer[partition]!!
            // find the correct partition movement considering the stickiness requirement
            val partitionToBeMoved =
                partitionMovements!!.getTheActualPartitionToBeMoved(partition, consumer, newConsumer)
            processPartitionMovement(partitionToBeMoved, newConsumer)
        }

        private fun processPartitionMovement(partition: TopicPartition, newConsumer: String) {
            val oldConsumer = currentPartitionConsumer[partition]!!
            sortedCurrentSubscriptions.remove(oldConsumer)
            sortedCurrentSubscriptions.remove(newConsumer)
            partitionMovements!!.movePartition(partition, oldConsumer, newConsumer)
            currentAssignment[oldConsumer]!!.remove(partition)
            currentAssignment[newConsumer]!!.add(partition)
            currentPartitionConsumer[partition] = newConsumer
            sortedCurrentSubscriptions.add(newConsumer)
            sortedCurrentSubscriptions.add(oldConsumer)
        }

        private fun deepCopy(
            source: Map<String, List<TopicPartition>>,
            dest: MutableMap<String, MutableList<TopicPartition>>,
        ) {
            dest.clear()
            for ((key, value) in source) dest[key] =
                java.util.ArrayList(value)
        }

        private fun deepCopy(assignment: Map<String, List<TopicPartition>>): Map<String, List<TopicPartition>> {
            val copy = mutableMapOf<String, MutableList<TopicPartition>>()
            deepCopy(assignment, copy)
            return copy
        }
    }

    internal data class ConsumerGenerationPair(
        val consumer: String,
        val generation: Int,
    )

    data class MemberData(
        val partitions: List<TopicPartition>,
        val generation: Int?,
        val rackId: String? = null,
    )

    companion object {

        private val log = LoggerFactory.getLogger(AbstractStickyAssignor::class.java)

        const val DEFAULT_GENERATION = -1
    }
}
