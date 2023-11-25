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
import org.apache.kafka.clients.consumer.internals.Utils.PartitionComparator
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.*
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.max

abstract class AbstractStickyAssignor : AbstractPartitionAssignor() {

    var maxGeneration = DEFAULT_GENERATION

    private var partitionMovements: PartitionMovements? = null

    // Keep track of the partitions being migrated from one consumer to another during assignment
    // so the cooperative assignor can adjust the assignment
    var partitionsTransferringOwnership: MutableMap<TopicPartition, String>? = mutableMapOf()

    abstract fun memberData(
        subscription: ConsumerPartitionAssignor.Subscription,
    ): MemberData

    override fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): Map<String, List<TopicPartition>> {
        val consumerToOwnedPartitions = mutableMapOf<String, MutableList<TopicPartition>>()
        val partitionsWithMultiplePreviousOwners = mutableSetOf<TopicPartition>()

        if (
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
            return constrainedAssign(
                partitionsPerTopic = partitionsPerTopic,
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
            return generalAssign(
                partitionsPerTopic = partitionsPerTopic,
                subscriptions = subscriptions,
                currentAssignment = consumerToOwnedPartitions,
            )
        }
    }

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
            maxGeneration = max(maxGeneration, memberGeneration)
            val ownedPartitions = mutableListOf<TopicPartition>()
            consumerToOwnedPartitions[consumer] = ownedPartitions

            // the member has a valid generation, so we can consider its owned partitions if it has
            // the highest generation amongst
            memberData.partitions.forEach { topicPartition ->
                if (!allTopics.contains(topicPartition.topic)) return@forEach

                val otherConsumer = allPreviousPartitionsToOwner.put(topicPartition, consumer)
                if (otherConsumer == null) {
                    // this partition is not owned by other consumer in the same generation
                    ownedPartitions.add(topicPartition)
                    return@forEach
                }

                val otherMemberGeneration =
                    subscriptions[otherConsumer]!!.generationId ?: DEFAULT_GENERATION

                if (memberGeneration == otherMemberGeneration) {
                    // if two members of the same generation own the same partition, revoke the
                    // partition
                    log.error(
                        "Found multiple consumers {} and {} claiming the same TopicPartition {} " +
                                "in the same generation {}, this will be invalidated and removed " +
                                "from their previous assignment.",
                        consumer,
                        otherConsumer,
                        topicPartition,
                        memberGeneration,
                    )
                    partitionsWithMultiplePreviousOwners.add(topicPartition)
                    consumerToOwnedPartitions[otherConsumer]!!.remove(topicPartition)
                    allPreviousPartitionsToOwner[topicPartition] = consumer
                } else if (memberGeneration > otherMemberGeneration) {
                    // move partition from the member with an older generation to the member with
                    // the newer generation
                    ownedPartitions.add(topicPartition)
                    consumerToOwnedPartitions[otherConsumer]!!.remove(topicPartition)
                    allPreviousPartitionsToOwner[topicPartition] = consumer

                    log.warn(
                        "Consumer {} in generation {} and consumer {} in generation {} claiming " +
                                "the same TopicPartition {} in different generations. The topic " +
                                "partition wil be assigned to the member with the higher " +
                                "generation {}.",
                        consumer,
                        memberGeneration,
                        otherConsumer,
                        otherMemberGeneration,
                        topicPartition,
                        memberGeneration,
                    )
                } else {
                    // let the other member continue to own the topic partition
                    log.warn(
                        "Consumer {} in generation {} and consumer {} in generation {} claiming " +
                                "the same TopicPartition {} in different generations. The topic " +
                                "partition wil be assigned to the member with the higher " +
                                "generation {}.",
                        consumer,
                        memberGeneration,
                        otherConsumer,
                        otherMemberGeneration,
                        topicPartition,
                        otherMemberGeneration,
                    )
                }
            }
        }
        return isAllSubscriptionsEqual
    }

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
    ): Map<String, MutableList<TopicPartition>> {
        if (log.isDebugEnabled) log.debug(
            "Performing constrained assign with partitionsPerTopic: {}, " +
                    "consumerToOwnedPartitions: {}.",
            partitionsPerTopic,
            consumerToOwnedPartitions,
        )

        val allRevokedPartitions: MutableSet<TopicPartition> = HashSet()

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
        val assignment: Map<String, MutableList<TopicPartition>> = consumerToOwnedPartitions.keys
            .associateWith { ArrayList<TopicPartition>(maxQuota) }

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
    ): Map<String, MutableList<TopicPartition>> {
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
        totalPartitionCount: Int
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
        val preBalancePartitionConsumers: Map<TopicPartition, String> =
            currentPartitionConsumer.toMap()

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

    val isSticky: Boolean
        get() = partitionMovements!!.isSticky

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
        private val map: Map<String, MutableList<String>>,
    ) : Comparator<String>, Serializable {
            override fun compare(o1: String, o2: String): Int {
            var ret = map[o1]!!.size - map[o2]!!.size
            if (ret == 0) ret = o1.compareTo(o2)

            return ret
        }

        companion object {
            private const val serialVersionUID = 1L
        }
    }

    private class SubscriptionComparator(
        private val map: Map<String, MutableList<TopicPartition>>,
    ) : Comparator<String>, Serializable {
        override fun compare(o1: String, o2: String): Int {
            var ret = map[o1]!!.size - map[o2]!!.size
            if (ret == 0) ret = o1.compareTo(o2)
            return ret
        }

        companion object {
            private const val serialVersionUID = 1L
        }
    }

    /**
     * This class maintains some data structures to simplify lookup of partition movements among
     * consumers. At each point of time during a partition rebalance it keeps track of partition
     * movements corresponding to each topic, and also possible movement (in form a `ConsumerPair`
     * object) for each partition.
     */
    private class PartitionMovements() {

        private val partitionMovementsByTopic =
            mutableMapOf<String, MutableMap<ConsumerPair, MutableSet<TopicPartition>>>()

        private val partitionMovements: MutableMap<TopicPartition, ConsumerPair> = HashMap()

        private fun removeMovementRecordOfPartition(partition: TopicPartition): ConsumerPair? {
            val pair = partitionMovements.remove(partition)
            val topic = partition.topic
            val partitionMovementsForThisTopic = (partitionMovementsByTopic[topic])!!
            partitionMovementsForThisTopic[pair]!!.remove(partition)

            if (partitionMovementsForThisTopic[pair]!!.isEmpty())
                partitionMovementsForThisTopic.remove(pair)

            if (partitionMovementsByTopic[topic]!!.isEmpty())
                partitionMovementsByTopic.remove(topic)

            return pair
        }

        private fun addPartitionMovementRecord(partition: TopicPartition, pair: ConsumerPair) {
            partitionMovements[partition] = pair
            val topic = partition.topic
            partitionMovementsByTopic.computeIfAbsent(topic) { mutableMapOf() }

            val partitionMovementsForThisTopic = partitionMovementsByTopic[topic]!!
            val set = partitionMovementsForThisTopic.computeIfAbsent(pair) { mutableSetOf() }
            set.add(partition)
        }

        fun movePartition(
            partition: TopicPartition,
            oldConsumer: String,
            newConsumer: String,
        ) {
            val pair = ConsumerPair(oldConsumer, newConsumer)
            if (partitionMovements.containsKey(partition)) {
                // this partition has previously moved
                val existingPair = removeMovementRecordOfPartition(partition)
                assert(existingPair!!.dstMemberId == oldConsumer)

                if (existingPair.srcMemberId != newConsumer) {
                    // the partition is not moving back to its previous consumer
                    // return new ConsumerPair2(existingPair.src, newConsumer);
                    addPartitionMovementRecord(
                        partition = partition,
                        pair = ConsumerPair(existingPair.srcMemberId, newConsumer),
                    )
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
                assert((oldConsumer == partitionMovements[partition]!!.dstMemberId))
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
            if ((src == dst)) return false
            if (pairs.isEmpty()) return false

            if (ConsumerPair(src, dst).`in`(pairs)) {
                currentPath.add(src)
                currentPath.add(dst)
                return true
            }

            for (pair: ConsumerPair? in pairs) if ((pair!!.srcMemberId == src)) {
                val reducedSet: MutableSet<ConsumerPair> = pairs.toMutableSet()
                reducedSet.remove(pair)
                currentPath.add(pair.srcMemberId)
                return isLinked(
                    src = pair.dstMemberId,
                    dst = dst,
                    pairs = reducedSet,
                    currentPath = currentPath
                )
            }
            return false
        }

        private fun `in`(cycle: List<String?>, cycles: Set<List<String>>): Boolean {
            val superCycle: MutableList<String?> = ArrayList(cycle)
            superCycle.removeAt(superCycle.size - 1)
            superCycle.addAll(cycle)
            cycles.forEach { foundCycle ->
                if (
                    foundCycle.size == cycle.size
                    && Collections.indexOfSubList(superCycle, foundCycle) != -1
                ) return true
            }
            return false
        }

        private fun hasCycles(pairs: Set<ConsumerPair>): Boolean {
            val cycles = mutableSetOf<List<String>>()

            for (pair in pairs) {
                val reducedPairs = pairs.toMutableSet()
                reducedPairs.remove(pair)
                val path: MutableList<String> = mutableListOf(pair.srcMemberId)
                if (
                    isLinked(pair.dstMemberId, pair.srcMemberId, reducedPairs, path)
                    && !`in`(path, cycles)
                ) {
                    cycles.add(path.toList())
                    log.error("A cycle of length {} was found: {}", path.size - 1, path.toString())
                }
            }

            // for now we want to make sure there is no partition movements of the same topic
            // between a pair of consumers. the odds of finding a cycle among more than two
            // consumers seem to be very low (according to various randomized tests with the given
            // sticky algorithm) that it should not worth the added complexity of handling those
            // cases.
            for (cycle: List<String?> in cycles) if (cycle.size == 3) return true // indicates a cycle of length 2
            return false
        }

        val isSticky: Boolean
            get() {
                partitionMovementsByTopic.forEach { topicMovements ->
                    val topicMovementPairs = topicMovements.value.keys
                    if (hasCycles(topicMovementPairs)) {
                        log.error(
                            "Stickiness is violated for topic {}"
                                    + "\nPartition movements for this topic occurred among the " +
                                    "following consumer pairs:"
                                    + "\n{}",
                            topicMovements.key,
                            topicMovements.value.toString(),
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

        fun `in`(pairs: Set<ConsumerPair>): Boolean {
            for (pair in pairs) if ((this == pair)) return true
            return false
        }
    }

    internal data class ConsumerGenerationPair(
        val consumer: String,
        val generation: Int,
    )

    data class MemberData(
        val partitions: List<TopicPartition>,
        val generation: Int?,
    )

    companion object {

        private val log = LoggerFactory.getLogger(AbstractStickyAssignor::class.java)

        const val DEFAULT_GENERATION = -1
    }
}
