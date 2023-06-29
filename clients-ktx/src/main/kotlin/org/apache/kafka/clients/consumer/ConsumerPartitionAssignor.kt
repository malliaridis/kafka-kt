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
import java.util.*
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils.contextOrKafkaClassLoader
import org.apache.kafka.common.utils.Utils.newInstance

/**
 * This interface is used to define custom partition assignment for use in [KafkaConsumer]. Members
 * of the consumer group subscribe to the topics they are interested in and forward their
 * subscriptions to a Kafka broker serving as the group coordinator. The coordinator selects one
 * member to perform the group assignment and propagates the subscriptions of all members to it.
 * Then [assign] is called to perform the assignment and the results are forwarded back to each
 * respective members.
 *
 * In some cases, it is useful to forward additional metadata to the assignor in order to make
 * assignment decisions. For this, you can override [subscriptionUserData] and provide custom
 * userData in the returned Subscription. For example, to have a rack-aware assignor, an
 * implementation can use this user data to forward the rackId belonging to each member.
 */
interface ConsumerPartitionAssignor {

    /**
     * Return serialized data that will be included in the [Subscription] sent to the leader and can
     * be leveraged in [assign] ((e.g. local host/rack information)
     *
     * @param topics Topics subscribed to through [KafkaConsumer.subscribe] and variants
     * @return nullable subscription user data
     */
    fun subscriptionUserData(topics: Set<String>): ByteBuffer? {
        return null
    }

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     *
     * @param metadata Current topic/broker metadata known by consumer
     * @param groupSubscription Subscriptions from all members including metadata provided through
     * [subscriptionUserData]
     * @return A map from the members to their respective assignments. This should have one entry
     * for each member in the input subscription map.
     */
    fun assign(metadata: Cluster, groupSubscription: GroupSubscription): GroupAssignment

    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * @param assignment The local member's assignment as provided by the leader in [assign]
     * @param metadata Additional metadata on the consumer (optional)
     */
    fun onAssignment(assignment: Assignment, metadata: ConsumerGroupMetadata?) {}

    /**
     * Indicate which rebalance protocol this assignor works with;
     * By default it should always work with [RebalanceProtocol.EAGER].
     */
    fun supportedProtocols(): List<RebalanceProtocol?>? {
        return listOf(RebalanceProtocol.EAGER)
    }

    /**
     * Return the version of the assignor which indicates how the user metadata encodings
     * and the assignment algorithm gets evolved.
     */
    fun version(): Short = 0.toShort()

    /**
     * Unique name for this assignor (e.g. "range" or "roundrobin" or "sticky"). Note, this is not
     * required to be the same as the class name specified in
     * [ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG].
     *
     * @return non-null unique name
     */
    fun name(): String

    class Subscription(
        val topics: List<String>,
        val userData: ByteBuffer? = null,
        val ownedPartitions: List<TopicPartition> = emptyList(),
        generationId: Int = AbstractStickyAssignor.DEFAULT_GENERATION,
        val rackId: String? = null
    ) {

        var groupInstanceId: String? = null

        val generationId: Int? = if (generationId < 0) null else generationId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("topics"),
        )
        fun topics(): List<String> = topics

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("userData"),
        )
        fun userData(): ByteBuffer? = userData

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("ownedPartitions"),
        )
        fun ownedPartitions(): List<TopicPartition> = ownedPartitions

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("rackId"),
        )
        fun rackId(): String? = rackId

        @Deprecated("Use property instead")
        fun setGroupInstanceId(groupInstanceId: String?) {
            this.groupInstanceId = groupInstanceId
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("groupInstanceId"),
        )
        fun groupInstanceId(): String? = groupInstanceId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("generationId"),
        )
        fun generationId(): Int? = generationId

        override fun toString(): String {
            return "Subscription(" +
                    "topics=$topics" +
                    (userData?.remaining()?.let { ", userDataSize=$it" } ?: "") +
                    ", ownedPartitions=$ownedPartitions" +
                    ", groupInstanceId=$groupInstanceId" +
                    ", generationId=${generationId ?: -1}" +
                    ", rackId=$rackId" +
                    ")"
        }
    }

    data class Assignment(
        val partitions: List<TopicPartition>,
        val userData: ByteBuffer? = null,
    ) {

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("partitions"),
        )
        fun partitions(): List<TopicPartition> = partitions

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("userData"),
        )
        fun userData(): ByteBuffer? = userData

        override fun toString(): String {
            return "Assignment(" +
                    "partitions=$partitions" +
                    (userData?.remaining()?.let { ", userDataSize=$it" } ?: "") +
                    ')'
        }
    }

    class GroupSubscription(val subscriptions: Map<String, Subscription>) {

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("subscriptions"),
        )
        fun groupSubscription(): Map<String, Subscription> = subscriptions

        override fun toString(): String {
            return "GroupSubscription(" +
                    "subscriptions=$subscriptions" +
                    ")"
        }
    }

    class GroupAssignment(val assignments: Map<String, Assignment>) {

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("assignments"),
        )
        fun groupAssignment(): Map<String, Assignment> {
            return assignments
        }

        override fun toString(): String {
            return "GroupAssignment(" +
                    "assignments=$assignments" +
                    ")"
        }
    }

    /**
     * The rebalance protocol defines partition assignment and revocation semantics. The purpose is
     * to establish a consistent set of rules that all consumers in a group follow in order to
     * transfer ownership of a partition. [ConsumerPartitionAssignor] implementors can claim
     * supporting one or more rebalance protocols via the
     * [ConsumerPartitionAssignor.supportedProtocols], and it is their responsibility to respect the
     * rules of those protocols in their [ConsumerPartitionAssignor.assign] implementations.
     * Failures to follow the rules of the supported protocols would lead to runtime error or
     * undefined behavior.
     *
     * The [RebalanceProtocol.EAGER] rebalance protocol requires a consumer to always revoke all its
     * owned partitions before participating in a rebalance event. It therefore allows a complete
     * reshuffling of the assignment.
     *
     * [RebalanceProtocol.COOPERATIVE] rebalance protocol allows a consumer to retain its currently
     * owned partitions before participating in a rebalance event. The assignor should not reassign
     * any owned partitions immediately, but instead may indicate consumers the need for partition
     * revocation so that the revoked partitions can be reassigned to other consumers in the next
     * rebalance event. This is designed for sticky assignment logic which attempts to minimize
     * partition reassignment with cooperative adjustments.
     */
    enum class RebalanceProtocol(val id: Byte) {

        EAGER(0.toByte()),

        COOPERATIVE(1.toByte());

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("id"),
        )
        fun id(): Byte {
            return id
        }

        companion object {

            fun forId(id: Byte): RebalanceProtocol = when (id.toInt()) {
                0 -> EAGER
                1 -> COOPERATIVE
                else -> throw IllegalArgumentException("Unknown rebalance protocol id: $id")
            }
        }
    }

    companion object {

        /**
         * Get a list of configured instances of [ConsumerPartitionAssignor] based on the class
         * names/types specified by [ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG].
         */
        fun getAssignorInstances(
            assignorClasses: List<String>,
            configs: Map<String, Any>,
        ): List<ConsumerPartitionAssignor> {

            if (assignorClasses.isEmpty()) return emptyList()

            val assignors: MutableList<ConsumerPartitionAssignor> = ArrayList()
            // a map to store assignor name -> assignor class name
            val assignorNameMap: MutableMap<String, String> = HashMap()

            assignorClasses.forEach { assignerClass ->
                var klass: Any = assignerClass

                // first try to get the class
                try {
                    klass = Class.forName(assignerClass, true, contextOrKafkaClassLoader)
                } catch (classNotFound: ClassNotFoundException) {
                    throw KafkaException(
                        "$assignerClass ClassNotFoundException exception occurred",
                        classNotFound
                    )
                }

                val assignor = newInstance(klass)

                if (assignor is Configurable) assignor.configure(configs)

                if (assignor is ConsumerPartitionAssignor) {
                    val assignorName = assignor.name()
                    if (assignorNameMap.containsKey(assignorName)) {
                        throw KafkaException(
                            "The assignor name: '$assignorName' is used in more than one assignor: " +
                                    "${assignorNameMap[assignorName]}, ${assignor.javaClass.name}"
                        )
                    }

                    assignorNameMap[assignorName] = assignor.javaClass.name
                    assignors.add(assignor)
                } else throw KafkaException(
                    "$klass is not an instance of ${ConsumerPartitionAssignor::class.java.name}"
                )
            }
            return assignors
        }
    }
}
