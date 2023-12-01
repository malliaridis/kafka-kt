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

import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.ArrayOf
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.types.Type
import org.apache.kafka.common.utils.CollectionUtils.groupPartitionsByTopic
import java.nio.ByteBuffer

/**
 * The sticky assignor serves two purposes. First, it guarantees an assignment that is as balanced
 * as possible, meaning either:
 *
 * - the numbers of topic partitions assigned to consumers differ by at most one; or
 * - each consumer that has 2+ fewer topic partitions than some other consumer cannot get any of
 *   those topic partitions transferred to it.
 *
 * Second, it preserved as many existing assignment as possible when a reassignment occurs. This
 * helps in saving some of the overhead processing when topic partitions move from one consumer to
 * another.
 *
 * Starting fresh it would work by distributing the partitions over consumers as evenly as possible.
 * Even though this may sound similar to how round robin assignor works, the second example below
 * shows that it is not. During a reassignment it would perform the reassignment in such a way that
 * in the new assignment
 *
 * 1. topic partitions are still distributed as evenly as possible, and
 * 2. topic partitions stay with their previously assigned consumers as much as possible.
 *
 * Of course, the first goal above takes precedence over the second one.
 *
 * **Example 1.** Suppose there are three consumers `C0`, `C1`, `C2`, four topics `t0,` `t1`, `t2`,
 * `t3`, and each topic has 2 partitions, resulting in partitions `t0p0`, `t0p1`, `t1p0`, `t1p1`,
 * `t2p0`, `t2p1`, `t3p0`, `t3p1`. Each consumer is subscribed to all three topics.
 *
 * The assignment with both sticky and round robin assignors will be:
 *
 * - `C0: [t0p0, t1p1, t3p0]`
 * - `C1: [t0p1, t2p0, t3p1]`
 * - `C2: [t1p0, t2p1]`
 *
 * Now, let's assume `C1` is removed and a reassignment is about to happen. The round robin assignor
 * would produce:
 *
 * - `C0: [t0p0, t1p0, t2p0, t3p0]`
 * - `C2: [t0p1, t1p1, t2p1, t3p1]`
 *
 * while the sticky assignor would result in:
 *
 * - `C0 [t0p0, t1p1, t3p0, t2p0]`
 * - `C2 [t1p0, t2p1, t0p1, t3p1]`
 *
 * preserving all the previous assignments (unlike the round robin assignor).
 *
 * **Example 2.** There are three consumers `C0`, `C1`, `C2`, and three topics `t0`, `t1`, `t2`,
 * with 1, 2, and 3 partitions respectively. Therefore, the partitions are `t0p0`, `t1p0`, `t1p1`,
 * `t2p0`, `t2p1`, `t2p2`. `C0` is subscribed to `t0`; `C1` is subscribed to `t0`, `t1`; and `C2` is
 * subscribed to `t0`, `t1`, `t2`.
 *
 * The round robin assignor would come up with the following assignment:
 *
 * - `C0 [t0p0]`
 * - `C1 [t1p0]`
 * - `C2 [t1p1, t2p0, t2p1, t2p2]`
 *
 * which is not as balanced as the assignment suggested by sticky assignor:
 *
 * - `C0 [t0p0]`
 * - `C1 [t1p0, t1p1]`
 * - `C2 [t2p0, t2p1, t2p2]`
 *
 * Now, if consumer `C0` is removed, these two assignors would produce the following assignments.
 * Round Robin (preserves 3 partition assignments):
 *
 * - `C1 [t0p0, t1p1]`
 * - `C2 [t1p0, t2p0, t2p1, t2p2]`
 *
 * Sticky (preserves 5 partition assignments):
 *
 * - `C1 [t1p0, t1p1, t0p0]`
 * - `C2 [t2p0, t2p1, t2p2]`
 *
 * ### Impact on `ConsumerRebalanceListener`
 * The sticky assignment strategy can provide some optimization to those consumers that have some
 * partition cleanup code in their `onPartitionsRevoked()` callback listeners. The cleanup code is
 * placed in that callback listener because the consumer has no assumption or hope of preserving any
 * of its assigned partitions after a rebalance when it is using range or round robin assignor. The
 * listener code would look like this:
 * ```java
 * class TheOldRebalanceListener implements ConsumerRebalanceListener {
 *
 *  void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       commitOffsets(partition);
 *       cleanupState(partition);
 *     }
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       initializeState(partition);
 *       initializeOffset(partition);
 *     }
 *   }
 * }
 * ```
 *
 * As mentioned above, one advantage of the sticky assignor is that, in general, it reduces the
 * number of partitions that actually move from one consumer to another during a reassignment.
 * Therefore, it allows consumers to do their cleanup more efficiently. Of course, they still can
 * perform the partition cleanup in the `onPartitionsRevoked()` listener, but they can be more
 * efficient and make a note of their partitions before and after the rebalance, and do the cleanup
 * after the rebalance only on the partitions they have lost (which is normally not a lot). The code
 * snippet below clarifies this point:
 * ```java
 * class TheNewRebalanceListener implements ConsumerRebalanceListener {
 *   Collection<TopicPartition> lastAssignment = Collections.emptyList();
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions)
 *       commitOffsets(partition);
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> assignment) {
 *     for (TopicPartition partition: difference(lastAssignment, assignment))
 *       cleanupState(partition);
 *
 *     for (TopicPartition partition: difference(assignment, lastAssignment))
 *       initializeState(partition);
 *
 *     for (TopicPartition partition: assignment)
 *       initializeOffset(partition);
 *
 *     this.lastAssignment = assignment;
 *   }
 * }
 *```
 *
 * Any consumer that uses sticky assignment can leverage this listener like this:
 *
 * ```java
 * consumer.subscribe(topics, new TheNewRebalanceListener());
 * ```
 *
 * Note that you can leverage the [CooperativeStickyAssignor] so that only partitions which are
 * being reassigned to another consumer will be revoked. That is the preferred assignor for newer
 * cluster. See [ConsumerPartitionAssignor.RebalanceProtocol] for a detailed explanation of
 * cooperative rebalancing.
 */
class StickyAssignor : AbstractStickyAssignor() {

    private var memberAssignment: List<TopicPartition>? = null

    private var generation = DEFAULT_GENERATION // consumer group generation

    override fun name(): String = STICKY_ASSIGNOR_NAME

    override fun onAssignment(
        assignment: ConsumerPartitionAssignor.Assignment?,
        metadata: ConsumerGroupMetadata?,
    ) {
        memberAssignment = assignment!!.partitions
        generation = metadata!!.generationId
    }

    override fun subscriptionUserData(topics: Set<String>): ByteBuffer? {
        return memberAssignment?.let {
            serializeTopicPartitionAssignment(MemberData(it, generation))
        }
    }

    override fun memberData(subscription: ConsumerPartitionAssignor.Subscription): MemberData {
        // Always deserialize ownedPartitions and generation id from user data since StickyAssignor
        // is an eager rebalance protocol that will revoke all existing partitions before joining
        // group
        val userData = subscription.userData
        return if (userData == null || !userData.hasRemaining())
            MemberData(emptyList(), null)
        else deserializeTopicPartitionAssignment(userData)
    }

    companion object {

        const val STICKY_ASSIGNOR_NAME = "sticky"

        // these schemas are used for preserving consumer's previously assigned partitions
        // list and sending it as user data to the leader during a rebalance
        const val TOPIC_PARTITIONS_KEY_NAME = "previous_assignment"

        const val TOPIC_KEY_NAME = "topic"

        const val PARTITIONS_KEY_NAME = "partitions"

        private const val GENERATION_KEY_NAME = "generation"

        val TOPIC_ASSIGNMENT = Schema(
            Field(TOPIC_KEY_NAME, Type.STRING),
            Field(PARTITIONS_KEY_NAME, ArrayOf(Type.INT32)),
        )

        val STICKY_ASSIGNOR_USER_DATA_V0 = Schema(
            Field(TOPIC_PARTITIONS_KEY_NAME, ArrayOf(TOPIC_ASSIGNMENT)),
        )

        private val STICKY_ASSIGNOR_USER_DATA_V1 = Schema(
            Field(TOPIC_PARTITIONS_KEY_NAME, ArrayOf(TOPIC_ASSIGNMENT)),
            Field(GENERATION_KEY_NAME, Type.INT32),
        )

        // visible for testing
        fun serializeTopicPartitionAssignment(memberData: MemberData): ByteBuffer {
            val struct = Struct(STICKY_ASSIGNOR_USER_DATA_V1)
            val topicAssignments: MutableList<Struct> = ArrayList()

            for ((key, value) in groupPartitionsByTopic(memberData.partitions)) {
                val topicAssignment = Struct(TOPIC_ASSIGNMENT)
                topicAssignment[TOPIC_KEY_NAME] = key
                topicAssignment[PARTITIONS_KEY_NAME] = value.toTypedArray()
                topicAssignments.add(topicAssignment)
            }

            struct[TOPIC_PARTITIONS_KEY_NAME] = topicAssignments.toTypedArray()
            if (memberData.generation != null)
                struct[GENERATION_KEY_NAME] = memberData.generation

            val buffer = ByteBuffer.allocate(STICKY_ASSIGNOR_USER_DATA_V1.sizeOf(struct))
            STICKY_ASSIGNOR_USER_DATA_V1.write(buffer, struct)
            buffer.flip()
            return buffer
        }

        private fun deserializeTopicPartitionAssignment(buffer: ByteBuffer): MemberData {
            val copy = buffer.duplicate()

            val struct = try {
                STICKY_ASSIGNOR_USER_DATA_V1.read(buffer)
            } catch (e1: Exception) {
                try {
                    // fall back to older schema
                    STICKY_ASSIGNOR_USER_DATA_V0.read(copy)
                } catch (e2: Exception) {
                    // ignore the consumer's previous assignment if it cannot be parsed
                    return MemberData(emptyList(), DEFAULT_GENERATION)
                }
            }

            val partitions: MutableList<TopicPartition> = ArrayList()
            for (structObj in struct.getArray(TOPIC_PARTITIONS_KEY_NAME)!!) {
                val assignment = structObj as Struct?
                val topic = assignment!!.getString(TOPIC_KEY_NAME)
                for (partitionObj in assignment.getArray(PARTITIONS_KEY_NAME)!!) {
                    val partition = partitionObj as Int?
                    partitions.add(TopicPartition(topic!!, partition!!))
                }
            }

            // make sure this is backward compatible
            val generation: Int? =
                if (struct.hasField(GENERATION_KEY_NAME)) struct.getInt(GENERATION_KEY_NAME)
                else null

            return MemberData(partitions, generation)
        }
    }
}