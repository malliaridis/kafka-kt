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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ConsumerProtocolAssignment
import org.apache.kafka.common.message.ConsumerProtocolSubscription
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.MessageUtil.toVersionPrefixedByteBuffer
import org.apache.kafka.common.protocol.types.SchemaException
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer

/**
 * ConsumerProtocol contains the schemas for consumer subscriptions and assignments for use with
 * Kafka's generalized group management protocol.
 *
 * The current implementation assumes that future versions will not break compatibility. When
 * it encounters a newer version, it parses it using the current format. This basically means
 * that new versions cannot remove or reorder any of the existing fields.
 */
object ConsumerProtocol {

    const val PROTOCOL_TYPE = "consumer"

    init {
        // Safety check to ensure that both parts of the consumer protocol remain in sync.
        check(
            ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION == ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION
        ) {
            "Subscription and Assignment schemas must have the same lowest version"
        }
        check(
            ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION == ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION
        ) {
            "Subscription and Assignment schemas must have the same highest version"
        }
    }

    fun deserializeVersion(buffer: ByteBuffer): Short {
        return try {
            buffer.getShort()
        } catch (e: BufferUnderflowException) {
            throw SchemaException("Buffer underflow while parsing consumer protocol's header", e)
        }
    }

    fun serializeSubscription(
        subscription: ConsumerPartitionAssignor.Subscription,
        version: Short = ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION,
    ): ByteBuffer {
        var version = version
        version = checkSubscriptionVersion(version)
        val data = ConsumerProtocolSubscription()
        val topics = subscription.topics.sorted()

        data.setTopics(topics)
        data.setUserData(
            if (subscription.userData != null) subscription.userData.duplicate()
            else null
        )

        val ownedPartitions = subscription.ownedPartitions.sortedWith(
            Comparator.comparing { (topic): TopicPartition -> topic }
            .thenComparing { (_, partition) -> partition }
        )

        var partition: ConsumerProtocolSubscription.TopicPartition? = null
        for ((topic, partition1) in ownedPartitions) {
            if (partition == null || partition.topic() != topic) {
                partition = ConsumerProtocolSubscription.TopicPartition().setTopic(topic)
                data.ownedPartitions().add(partition)
            }
            partition!!.partitions().add(partition1)
        }
        subscription.rackId?.let { data.setRackId(it) }
        data.setGenerationId(subscription.generationId ?: -1)
        return toVersionPrefixedByteBuffer(version, data)
    }

    @JvmOverloads
    fun deserializeSubscription(
        buffer: ByteBuffer,
        version: Short = deserializeVersion(buffer)
    ): ConsumerPartitionAssignor.Subscription {
        var version = version
        version = checkSubscriptionVersion(version)
        return try {
            val data = ConsumerProtocolSubscription(ByteBufferAccessor(buffer), version)
            val ownedPartitions = mutableListOf<TopicPartition>()

            for (tp in data.ownedPartitions())
                for (partition in tp.partitions())
                    ownedPartitions.add(TopicPartition(tp.topic(), partition!!))

            ConsumerPartitionAssignor.Subscription(
                topics = data.topics(),
                userData = if (data.userData() != null) data.userData().duplicate() else null,
                ownedPartitions = ownedPartitions,
                generationId = data.generationId(),
                rackId = if (data.rackId() == null || data.rackId().isEmpty()) null
                else data.rackId()
            )
        } catch (e: BufferUnderflowException) {
            throw SchemaException(
                "Buffer underflow while parsing consumer protocol's subscription",
                e
            )
        }
    }

    fun serializeAssignment(
        assignment: ConsumerPartitionAssignor.Assignment,
        version: Short = ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION,
    ): ByteBuffer {
        var version = version
        version = checkAssignmentVersion(version)
        val data = ConsumerProtocolAssignment()
        data.setUserData(assignment.userData?.duplicate())
        assignment.partitions.forEach { (topic, partition1) ->
            val partition = data.assignedPartitions().find(topic)
                ?: ConsumerProtocolAssignment.TopicPartition().setTopic(topic)
                    .also { data.assignedPartitions().add(it) }

            partition.partitions().add(partition1)
        }
        return toVersionPrefixedByteBuffer(version, data)
    }

    @JvmOverloads
    fun deserializeAssignment(
        buffer: ByteBuffer,
        version: Short = deserializeVersion(buffer)
    ): ConsumerPartitionAssignor.Assignment {
        var version = version
        version = checkAssignmentVersion(version)
        return try {
            val data = ConsumerProtocolAssignment(ByteBufferAccessor(buffer), version)
            val assignedPartitions = mutableListOf<TopicPartition>()

            for (tp in data.assignedPartitions())
                for (partition in tp.partitions())
                    assignedPartitions.add(TopicPartition(tp.topic(), partition!!))

            ConsumerPartitionAssignor.Assignment(
                partitions = assignedPartitions,
                userData = if (data.userData() != null) data.userData().duplicate() else null
            )
        } catch (e: BufferUnderflowException) {
            throw SchemaException(
                "Buffer underflow while parsing consumer protocol's assignment",
                e
            )
        }
    }

    private fun checkSubscriptionVersion(version: Short): Short {
        return if (version < ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION)
            throw SchemaException("Unsupported subscription version: $version")
        else if (version > ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION)
            ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION
        else version
    }

    private fun checkAssignmentVersion(version: Short): Short {
        return if (version < ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION)
            throw SchemaException("Unsupported assignment version: $version")
        else if (version > ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION)
            ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION
        else version
    }
}
