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

package org.apache.kafka.clients.admin

import org.apache.kafka.clients.HostResolver
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.admin.internals.CoordinatorKey
import org.apache.kafka.clients.admin.internals.MetadataOperationContext
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.KafkaFutureImpl
import java.util.*

object AdminClientTestUtils {

    /**
     * Helper to create a ListPartitionReassignmentsResult instance for a given Throwable.
     * ListPartitionReassignmentsResult's constructor is only accessible from within the
     * admin package.
     */
    fun listPartitionReassignmentsResult(t: Throwable?): ListPartitionReassignmentsResult {
        val future = KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>>()
        future.completeExceptionally(t!!)
        return ListPartitionReassignmentsResult(future)
    }

    /**
     * Helper to create a CreateTopicsResult instance for a given Throwable.
     * CreateTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    fun createTopicsResult(topic: String, t: Throwable?): CreateTopicsResult {
        val future = KafkaFutureImpl<TopicMetadataAndConfig>()
        future.completeExceptionally(t!!)
        return CreateTopicsResult(Collections.singletonMap(topic, future))
    }

    /**
     * Helper to create a DeleteTopicsResult instance for a given Throwable.
     * DeleteTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    fun deleteTopicsResult(topic: String, t: Throwable?): DeleteTopicsResult {
        val future = KafkaFutureImpl<Unit>()
        future.completeExceptionally(t!!)
        return DeleteTopicsResult.ofTopicNames(mapOf(topic to future))
    }

    /**
     * Helper to create a ListTopicsResult instance for a given topic.
     * ListTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    fun listTopicsResult(topic: String): ListTopicsResult {
        val future = KafkaFutureImpl<Map<String, TopicListing>>()
        future.complete(Collections.singletonMap(topic, TopicListing(topic, Uuid.ZERO_UUID, false)))
        return ListTopicsResult(future)
    }

    /**
     * Helper to create a CreatePartitionsResult instance for a given Throwable.
     * CreatePartitionsResult's constructor is only accessible from within the
     * admin package.
     */
    fun createPartitionsResult(topic: String, t: Throwable?): CreatePartitionsResult {
        val future = KafkaFutureImpl<Unit>()
        future.completeExceptionally(t!!)
        return CreatePartitionsResult(mapOf(topic to future))
    }

    /**
     * Helper to create a DescribeTopicsResult instance for a given topic.
     * DescribeTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    fun describeTopicsResult(topic: String?, description: TopicDescription): DescribeTopicsResult {
        val future = KafkaFutureImpl<TopicDescription>()
        future.complete(description)
        return DescribeTopicsResult.ofTopicNames(Collections.singletonMap(topic, future))
    }

    fun describeTopicsResult(topicDescriptions: Map<String, TopicDescription>): DescribeTopicsResult {
        return DescribeTopicsResult.ofTopicNames(
            topicDescriptions.mapValues { (_, e) -> KafkaFuture.completedFuture(e) }
        )
    }

    fun listConsumerGroupOffsetsResult(offsets: Map<String, Map<TopicPartition, OffsetAndMetadata?>>): ListConsumerGroupOffsetsResult {
        val resultMap = offsets.entries.associate { (key, value) ->
            CoordinatorKey.byGroupId(key) to KafkaFuture.completedFuture(value)
        }.toMap()
        return ListConsumerGroupOffsetsResult(resultMap)
    }

    fun listConsumerGroupOffsetsResult(
        group: String,
        exception: KafkaException,
    ): ListConsumerGroupOffsetsResult {
        val future = KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata?>>()
        future.completeExceptionally(exception)
        return ListConsumerGroupOffsetsResult(
            mapOf(CoordinatorKey.byGroupId(group) to future)
        )
    }

    /**
     * Used for benchmark. KafkaAdminClient.getListOffsetsCalls is only accessible
     * from within the admin package.
     */
    internal fun getListOffsetsCalls(
        adminClient: KafkaAdminClient,
        context: MetadataOperationContext<ListOffsetsResultInfo, ListOffsetsOptions>,
        topicPartitionOffsets: Map<TopicPartition, OffsetSpec>,
        futures: Map<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>>,
    ): List<KafkaAdminClient.Call> {
        return adminClient.getListOffsetsCalls(context, topicPartitionOffsets, futures)
    }

    /**
     * Helper to create a KafkaAdminClient with a custom HostResolver accessible to tests outside this package.
     */
    fun create(conf: Map<String, Any>, hostResolver: HostResolver?): Admin {
        return KafkaAdminClient.createInternal(
            config = AdminClientConfig(conf, true),
            hostResolver = hostResolver,
        )
    }
}
