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

import java.nio.ByteBuffer
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.types.Type

/**
 * A cooperative version of the [AbstractStickyAssignor]. This follows the same (sticky) assignment
 * logic as [StickyAssignor] but allows for cooperative rebalancing while the [StickyAssignor]
 * follows the eager rebalancing protocol. See [ConsumerPartitionAssignor.RebalanceProtocol] for an
 * explanation of the rebalancing protocols.
 *
 * Users should prefer this assignor for newer clusters.
 *
 * To turn on cooperative rebalancing you must set all your consumers to use this
 * `PartitionAssignor`, or implement a custom one that returns `RebalanceProtocol.COOPERATIVE` in
 * [CooperativeStickyAssignor.supportedProtocols].
 *
 * IMPORTANT: if upgrading from 2.3 or earlier, you must follow a specific upgrade path in order to
 * safely turn on cooperative rebalancing. See the
 * [upgrade guide](https://kafka.apache.org/documentation/#upgrade_240_notable) for details.
 */
class CooperativeStickyAssignor : AbstractStickyAssignor() {

    private var generation = DEFAULT_GENERATION // consumer group generation

    override fun name(): String = COOPERATIVE_STICKY_ASSIGNOR_NAME

    override fun supportedProtocols(): List<RebalanceProtocol> =
        listOf(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER)

    override fun onAssignment(
        assignment: ConsumerPartitionAssignor.Assignment?,
        metadata: ConsumerGroupMetadata?,
    ) {
        generation = metadata!!.generationId
    }

    override fun subscriptionUserData(topics: Set<String>): ByteBuffer? {
        val struct = Struct(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0)
        struct[GENERATION_KEY_NAME] = generation
        val buffer = ByteBuffer.allocate(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct))
        COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct)
        buffer.flip()
        return buffer
    }

    override fun memberData(subscription: ConsumerPartitionAssignor.Subscription): MemberData {
        // In ConsumerProtocolSubscription v2 or higher, we can take member data from fields directly
        if (subscription.generationId != null) {
            return MemberData(subscription.ownedPartitions, subscription.generationId)
        }
        val buffer = subscription.userData
        val encodedGeneration: Int? = if (buffer == null) null
        else try {
            val struct = COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.read(buffer)
            struct.getInt(GENERATION_KEY_NAME)
        } catch (_: Exception) {
            DEFAULT_GENERATION
        }

        return MemberData(
            subscription.ownedPartitions,
            encodedGeneration,
            subscription.rackId
        )
    }

    override fun assignPartitions(
        partitionsPerTopic: MutableMap<String, MutableList<PartitionInfo>>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        val assignments = super.assignPartitions(partitionsPerTopic, subscriptions)
        val partitionsTransferringOwnership = super.partitionsTransferringOwnership
            ?: computePartitionsTransferringOwnership(subscriptions, assignments)
        adjustAssignment(assignments, partitionsTransferringOwnership)
        return assignments
    }

    // Following the cooperative rebalancing protocol requires removing partitions that must first
    // be revoked from the assignment
    private fun adjustAssignment(
        assignments: Map<String, MutableList<TopicPartition>>,
        partitionsTransferringOwnership: Map<TopicPartition, String>,
    ) {
        for ((topicPartition, key) in partitionsTransferringOwnership) {
            assignments[key]!!.remove(topicPartition)
        }
    }

    private fun computePartitionsTransferringOwnership(
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
        assignments: Map<String, List<TopicPartition>>,
    ): Map<TopicPartition, String> {
        val allAddedPartitions = mutableMapOf<TopicPartition, String>()
        val allRevokedPartitions = mutableSetOf<TopicPartition>()

        for ((consumer, assignedPartitions) in assignments) {
            val ownedPartitions = subscriptions[consumer]!!.ownedPartitions
            val ownedPartitionsSet: Set<TopicPartition> = ownedPartitions.toSet()

            for (tp in assignedPartitions) {
                if (!ownedPartitionsSet.contains(tp)) allAddedPartitions[tp] = consumer
            }
            val assignedPartitionsSet = assignedPartitions.toSet()
            for (tp in ownedPartitions) {
                if (!assignedPartitionsSet.contains(tp)) allRevokedPartitions.add(tp)
            }
        }
        allAddedPartitions.keys.retainAll(allRevokedPartitions)
        return allAddedPartitions
    }

    companion object {

        const val COOPERATIVE_STICKY_ASSIGNOR_NAME = "cooperative-sticky"

        // these schemas are used for preserving useful metadata for the assignment, such as the
        // last stable generation
        private const val GENERATION_KEY_NAME = "generation"

        private val COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0 =
            Schema(Field(GENERATION_KEY_NAME, Type.INT32))
    }
}
