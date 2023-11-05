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
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.deserializeAssignment
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.deserializeSubscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.deserializeVersion
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeAssignment
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol.serializeSubscription
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ConsumerProtocolAssignment
import org.apache.kafka.common.message.ConsumerProtocolAssignment.Companion.HIGHEST_SUPPORTED_VERSION
import org.apache.kafka.common.message.ConsumerProtocolAssignment.Companion.LOWEST_SUPPORTED_VERSION
import org.apache.kafka.common.message.ConsumerProtocolSubscription
import org.apache.kafka.common.protocol.types.ArrayOf
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.types.Type
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ConsumerProtocolTest {

    private val tp1 = TopicPartition("foo", 1)

    private val tp2 = TopicPartition("bar", 2)

    private val groupInstanceId = "instance.id"

    private val generationId = 1

    private val rackId = "rack-a"

    @Test
    fun serializeDeserializeSubscriptionAllVersions() {
        val ownedPartitions = listOf(
            TopicPartition("foo", 0),
            TopicPartition("bar", 0),
        )
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = ByteBuffer.wrap("hello".toByteArray()),
            ownedPartitions = ownedPartitions,
            generationId = generationId,
            rackId = rackId,
        )
        for (version in ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION..ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION) {
            val buffer = serializeSubscription(subscription, version.toShort())
            val parsedSubscription = deserializeSubscription(buffer)
            assertEquals(subscription.topics.toSet(), parsedSubscription.topics.toSet())
            assertEquals(subscription.userData, parsedSubscription.userData)
            assertNull(parsedSubscription.groupInstanceId)
            if (version >= 1) assertEquals(
                subscription.ownedPartitions.toSet(),
                parsedSubscription.ownedPartitions.toSet()
            )
            else assertEquals(emptyList(), parsedSubscription.ownedPartitions)
            if (version >= 2) assertEquals(
                expected = generationId,
                actual = parsedSubscription.generationId ?: AbstractStickyAssignor.DEFAULT_GENERATION,
            )
            else assertNull(parsedSubscription.generationId)
            if (version >= 3) assertEquals(rackId, parsedSubscription.rackId)
            else assertNull(parsedSubscription.rackId)
        }
    }

    @Test
    fun serializeDeserializeMetadata() {
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = ByteBuffer.wrap(ByteArray(0)),
        )
        val buffer = serializeSubscription(subscription)
        val parsedSubscription = deserializeSubscription(buffer)
        assertEquals(subscription.topics.toSet(), parsedSubscription.topics.toSet())
        assertEquals(0, parsedSubscription.userData!!.limit())
        assertNull(parsedSubscription.groupInstanceId)
        assertNull(parsedSubscription.generationId)
        assertNull(parsedSubscription.rackId)
    }

    @Test
    fun serializeDeserializeMetadataAndGroupInstanceId() {
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = ByteBuffer.wrap(ByteArray(0)),
        )
        val buffer = serializeSubscription(subscription)
        val parsedSubscription = deserializeSubscription(buffer)
        parsedSubscription.groupInstanceId = groupInstanceId
        assertEquals(subscription.topics.toSet(), parsedSubscription.topics.toSet())
        assertEquals(0, parsedSubscription.userData!!.limit())
        assertEquals(groupInstanceId, parsedSubscription.groupInstanceId)
        assertNull(parsedSubscription.generationId)
        assertNull(parsedSubscription.rackId)
    }

    @Test
    fun serializeDeserializeNullSubscriptionUserData() {
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = null,
        )
        val buffer = serializeSubscription(subscription)
        val parsedSubscription = deserializeSubscription(buffer)
        assertEquals(subscription.topics.toSet(), parsedSubscription.topics.toSet())
        assertNull(parsedSubscription.userData)
        assertNull(parsedSubscription.rackId)
    }

    @Test
    fun serializeSubscriptionShouldOrderTopics() {
        assertEquals(
            serializeSubscription(
                ConsumerPartitionAssignor.Subscription(
                    topics = listOf("foo", "bar"),
                    userData = null,
                    ownedPartitions = listOf(tp1, tp2),
                )
            ),
            serializeSubscription(
                ConsumerPartitionAssignor.Subscription(
                    topics = listOf("bar", "foo"),
                    userData = null,
                    ownedPartitions = listOf(tp1, tp2),
                )
            )
        )
    }

    @Test
    fun serializeSubscriptionShouldOrderOwnedPartitions() {
        assertEquals(
            serializeSubscription(
                subscription = ConsumerPartitionAssignor.Subscription(
                    topics = listOf("foo", "bar"),
                    userData = null,
                    ownedPartitions = listOf(tp1, tp2),
                ),
            ),
            serializeSubscription(
                subscription = ConsumerPartitionAssignor.Subscription(
                    topics = listOf("foo", "bar"),
                    userData = null,
                    ownedPartitions = listOf(tp2, tp1),
                ),
            )
        )
    }

    @Test
    fun deserializeOldSubscriptionVersion() {
        val subscription = ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = null
        )
        val buffer = serializeSubscription(subscription, 0.toShort())
        val parsedSubscription = deserializeSubscription(buffer)
        assertEquals(parsedSubscription.topics.toSet(), parsedSubscription.topics.toSet())
        assertNull(parsedSubscription.userData)
        assertTrue(parsedSubscription.ownedPartitions.isEmpty())
        assertNull(parsedSubscription.generationId)
        assertNull(parsedSubscription.rackId)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun deserializeNewSubscriptionWithOldVersion(hasGenerationIdAndRack: Boolean) {
        val subscription = if (hasGenerationIdAndRack) ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = null,
            ownedPartitions = listOf(tp2),
            generationId = generationId,
            rackId = rackId,
        )
        else ConsumerPartitionAssignor.Subscription(
            topics = listOf("foo", "bar"),
            userData = null,
            ownedPartitions = listOf(tp2),
        )

        val buffer = serializeSubscription(subscription)
        // ignore the version assuming it is the old byte code, as it will blindly deserialize as V0
        deserializeVersion(buffer)
        val parsedSubscription = deserializeSubscription(buffer, 0.toShort())
        assertEquals(subscription.topics.toSet(), parsedSubscription.topics.toSet())
        assertNull(parsedSubscription.userData)
        assertTrue(parsedSubscription.ownedPartitions.isEmpty())
        assertNull(parsedSubscription.groupInstanceId)
        assertNull(parsedSubscription.generationId)
        assertNull(parsedSubscription.rackId)
    }

    @Test
    fun deserializeFutureSubscriptionVersion() {
        val buffer = generateFutureSubscriptionVersionData()
        val subscription = deserializeSubscription(buffer)
        subscription.groupInstanceId = groupInstanceId
        assertEquals(setOf("topic"), subscription.topics.toSet())
        assertEquals(setOf(tp2), subscription.ownedPartitions.toSet())
        assertEquals(groupInstanceId, subscription.groupInstanceId)
        assertEquals(generationId, subscription.generationId ?: AbstractStickyAssignor.DEFAULT_GENERATION)
        assertEquals(rackId, subscription.rackId)
    }

    @Test
    fun serializeDeserializeAssignmentAllVersions() {
        val partitions = listOf(tp1, tp2)
        val assignment = ConsumerPartitionAssignor.Assignment(partitions, ByteBuffer.wrap("hello".toByteArray()))
        for (version in LOWEST_SUPPORTED_VERSION..HIGHEST_SUPPORTED_VERSION) {
            val buffer = serializeAssignment(assignment, version.toShort())
            val (partitions1, userData) = deserializeAssignment(buffer)
            assertEquals(partitions.toSet(), partitions1.toSet())
            assertEquals(assignment.userData, userData)
        }
    }

    @Test
    fun serializeDeserializeAssignment() {
        val partitions = listOf(tp1, tp2)
        val buffer = serializeAssignment(
            ConsumerPartitionAssignor.Assignment(
                partitions = partitions,
                userData = ByteBuffer.wrap(ByteArray(0)),
            )
        )
        val (partitions1, userData) = deserializeAssignment(buffer)
        assertEquals(partitions.toSet(), partitions1.toSet())
        assertEquals(0, userData!!.limit())
    }

    @Test
    fun deserializeNullAssignmentUserData() {
        val partitions = listOf(tp1, tp2)
        val buffer = serializeAssignment(
            ConsumerPartitionAssignor.Assignment(partitions = partitions, userData = null)
        )
        val (partitions1, userData) = deserializeAssignment(buffer)
        assertEquals(partitions.toSet(), partitions1.toSet())
        assertNull(userData)
    }

    @Test
    fun deserializeFutureAssignmentVersion() {
        // verify that a new version which adds a field is still parseable
        val version: Short = 100
        val assignmentSchemaV100 = Schema(
            Field("assigned_partitions", ArrayOf(ConsumerProtocolAssignment.TopicPartition.SCHEMA_0)),
            Field("user_data", Type.BYTES),
            Field("foo", Type.STRING),
        )
        val assignmentV100 = Struct(assignmentSchemaV100)
        assignmentV100["assigned_partitions"] = arrayOf(
            Struct(ConsumerProtocolAssignment.TopicPartition.SCHEMA_0)
                .set("topic", tp1.topic)
                .set("partitions", arrayOf(tp1.partition))
        )
        assignmentV100["user_data"] = ByteBuffer.wrap(ByteArray(0))
        assignmentV100["foo"] = "bar"
        val headerV100 = Struct(Schema(Field("version", Type.INT16)))
        headerV100["version"] = version
        val buffer = ByteBuffer.allocate(assignmentV100.sizeOf() + headerV100.sizeOf())
        headerV100.writeTo(buffer)
        assignmentV100.writeTo(buffer)
        buffer.flip()
        val (partitions) = deserializeAssignment(buffer)
        assertEquals(listOf(tp1).toSet(), partitions.toSet())
    }

    private fun generateFutureSubscriptionVersionData(): ByteBuffer {
        // verify that a new version which adds a field is still parseable
        val version: Short = 100
        val subscriptionSchemaV100 = Schema(
            Field("topics", ArrayOf(Type.STRING)),
            Field("user_data", Type.NULLABLE_BYTES),
            Field("owned_partitions", ArrayOf(ConsumerProtocolSubscription.TopicPartition.SCHEMA_1)),
            Field("generation_id", Type.INT32),
            Field("rack_id", Type.STRING),
            Field("bar", Type.STRING)
        )
        val subscriptionV100 = Struct(subscriptionSchemaV100)
        subscriptionV100["topics"] = arrayOf("topic")
        subscriptionV100["user_data"] = ByteBuffer.wrap(ByteArray(0))
        subscriptionV100["owned_partitions"] = arrayOf(
            Struct(ConsumerProtocolSubscription.TopicPartition.SCHEMA_1)
                .set("topic", tp2.topic)
                .set("partitions", arrayOf(tp2.partition))
        )
        subscriptionV100["generation_id"] = generationId
        subscriptionV100["rack_id"] = rackId
        subscriptionV100["bar"] = "bar"
        val headerV100 = Struct(Schema(Field("version", Type.INT16)))
        headerV100["version"] = version
        val buffer = ByteBuffer.allocate(subscriptionV100.sizeOf() + headerV100.sizeOf())
        headerV100.writeTo(buffer)
        subscriptionV100.writeTo(buffer)
        buffer.flip()
        return buffer
    }
}
