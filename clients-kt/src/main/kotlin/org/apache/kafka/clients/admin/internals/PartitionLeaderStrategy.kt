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

package org.apache.kafka.clients.admin.internals

import java.util.function.Function
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * Base driver implementation for APIs which target partition leaders.
 */
class PartitionLeaderStrategy(logContext: LogContext) : AdminApiLookupStrategy<TopicPartition> {
    private val log: Logger = logContext.logger(PartitionLeaderStrategy::class.java)


    override fun lookupScope(key: TopicPartition): ApiRequestScope {
        // Metadata requests can group topic partitions arbitrarily, so they can all share
        // the same request context
        return SINGLE_REQUEST_SCOPE
    }

    override fun buildRequest(keys: Set<TopicPartition>): MetadataRequest.Builder {
        val request = MetadataRequestData()
        request.setAllowAutoTopicCreation(false)
        keys.map { obj -> obj.topic }
            .distinct()
            .forEach { topic -> request.topics = request.topics!! + MetadataRequestTopic().setName(topic) }
        return MetadataRequest.Builder(request)
    }

    private fun handleTopicError(
        topic: String,
        topicError: Errors,
        requestPartitions: Set<TopicPartition>,
        failed: MutableMap<TopicPartition, Throwable>
    ) {
        when (topicError) {
            Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.LEADER_NOT_AVAILABLE, Errors.BROKER_NOT_AVAILABLE -> log.debug(
                "Metadata request for topic {} returned topic-level error {}. Will retry",
                topic, topicError
            )

            Errors.TOPIC_AUTHORIZATION_FAILED -> {
                log.error(
                    "Received authorization failure for topic {} in `Metadata` response", topic,
                    topicError.exception
                )
                failAllPartitionsForTopic(
                    topic, requestPartitions, failed
                ) { tp: TopicPartition ->
                    TopicAuthorizationException(
                        "Failed to fetch metadata for partition $tp due to topic authorization failure",
                        setOf(topic)
                    )
                }
            }

            Errors.INVALID_TOPIC_EXCEPTION -> {
                log.error(
                    "Received invalid topic error for topic {} in `Metadata` response", topic,
                    topicError.exception
                )
                failAllPartitionsForTopic(
                    topic, requestPartitions, failed
                ) { tp: TopicPartition ->
                    InvalidTopicException(
                        message = "Failed to fetch metadata for partition $tp due to invalid topic `$topic`",
                        invalidTopics = setOf(topic)
                    )
                }
            }
            Errors.NONE -> {} // Catch before `else` to avoid NullPointerException
            else -> {
                log.error(
                    "Received unexpected error for topic {} in `Metadata` response", topic,
                    topicError.exception
                )
                failAllPartitionsForTopic(
                    topic, requestPartitions, failed
                ) { tp: TopicPartition ->
                    topicError.exception(
                        "Failed to fetch metadata for partition $tp due to unexpected error for topic `$topic`"
                    )!!
                }
            }
        }
    }

    private fun failAllPartitionsForTopic(
        topic: String,
        partitions: Set<TopicPartition>,
        failed: MutableMap<TopicPartition, Throwable>,
        exceptionGenerator: Function<TopicPartition, Throwable>
    ) {
        partitions.stream().filter { tp: TopicPartition -> tp.topic == topic }
            .forEach { tp: TopicPartition ->
                failed[tp] = exceptionGenerator.apply(tp)
            }
    }

    private fun handlePartitionError(
        topicPartition: TopicPartition,
        partitionError: Errors,
        failed: MutableMap<TopicPartition, Throwable>
    ) {
        when (partitionError) {
            Errors.NOT_LEADER_OR_FOLLOWER,
            Errors.REPLICA_NOT_AVAILABLE,
            Errors.LEADER_NOT_AVAILABLE,
            Errors.BROKER_NOT_AVAILABLE,
            Errors.KAFKA_STORAGE_ERROR -> log.debug(
                "Metadata request for partition {} returned partition-level error {}. Will retry",
                topicPartition, partitionError
            )
            Errors.NONE -> {} // Catch before `else` to avoid NullPointerException
            else -> {
                log.error(
                    "Received unexpected error for partition {} in `Metadata` response",
                    topicPartition, partitionError.exception
                )
                failed[topicPartition] = partitionError.exception(
                    "Unexpected error during metadata lookup for $topicPartition"
                )!!
            }
        }
    }

    override fun handleResponse(
        keys: Set<TopicPartition>,
        response: AbstractResponse
    ): AdminApiLookupStrategy.LookupResult<TopicPartition> {
        val failed: MutableMap<TopicPartition, Throwable> = HashMap()
        val mapped: MutableMap<TopicPartition, Int> = HashMap()
        (response as MetadataResponse).data().topics.forEach { topicMetadata ->
            val topic = topicMetadata.name!!
            val topicError = Errors.forCode(topicMetadata.errorCode)
            if (topicError != Errors.NONE) {
                handleTopicError(topic, topicError, keys, failed)
                return@forEach
            }
            for (partitionMetadata in topicMetadata.partitions) {
                val topicPartition = TopicPartition(topic, partitionMetadata.partitionIndex)
                val partitionError = Errors.forCode(partitionMetadata.errorCode)
                if (!keys.contains(topicPartition)) {
                    // The `Metadata` response always returns all partitions for requested
                    // topics, so we have to filter any that we are not interested in.
                    return@forEach
                }
                if (partitionError != Errors.NONE) {
                    handlePartitionError(topicPartition, partitionError, failed)
                    return@forEach
                }
                val leaderId = partitionMetadata.leaderId
                if (leaderId >= 0) mapped[topicPartition] = leaderId
                else log.debug(
                    "Metadata request for {} returned no error, but the leader is unknown. Will retry",
                    topicPartition
                )
            }
        }
        return AdminApiLookupStrategy.LookupResult(
            failedKeys = failed,
            mappedKeys = mapped,
        )
    }

    companion object {
        private val SINGLE_REQUEST_SCOPE: ApiRequestScope = object : ApiRequestScope {}
    }
}
