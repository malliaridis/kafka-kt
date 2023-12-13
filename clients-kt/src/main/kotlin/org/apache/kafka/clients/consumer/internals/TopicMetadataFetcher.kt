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

import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger

/**
 * [TopicMetadataFetcher] is responsible for fetching the [PartitionInfo] for a given set of topics.
 * All methods are blocking up to the [timeout][Timer] provided.
 */
class TopicMetadataFetcher(
    logContext: LogContext,
    private val client: ConsumerNetworkClient,
    private val retryBackoffMs: Long,
) {
    private val log: Logger = logContext.logger(javaClass)

    /**
     * Fetches the [partition information][PartitionInfo] for the given topic in the cluster, or `null`.
     *
     * @param timer Timer bounding how long this method can block
     * @return The [list][List] of [partition information][PartitionInfo], or `null` if the topic is
     * unknown
     */
    fun getTopicMetadata(topic: String, allowAutoTopicCreation: Boolean, timer: Timer): List<PartitionInfo>? {
        val request = MetadataRequest.Builder(
            topics = listOf(topic),
            allowAutoTopicCreation = allowAutoTopicCreation,
        )
        val topicMetadata = getTopicMetadata(request, timer)
        return topicMetadata[topic]
    }

    /**
     * Fetches the [partition information][PartitionInfo] for all topics in the cluster.
     *
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their [partition information][PartitionInfo]
     */
    fun getAllTopicMetadata(timer: Timer): Map<String, List<PartitionInfo>> {
        val request = MetadataRequest.Builder.allTopics()
        return getTopicMetadata(request, timer)
    }

    /**
     * Get metadata for all topics present in Kafka cluster.
     *
     * @param request The MetadataRequest to send
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    private fun getTopicMetadata(request: MetadataRequest.Builder, timer: Timer): Map<String, List<PartitionInfo>> {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics && request.emptyTopicList()) return emptyMap()

        do {
            val future = sendMetadataRequest(request)
            client.poll(future, timer)

            if (future.failed() && !future.isRetriable) throw future.exception()

            if (future.succeeded()) {
                val response = future.value()!!.responseBody as MetadataResponse
                val cluster = response.buildCluster()

                val unauthorizedTopics = cluster.unauthorizedTopics
                if (unauthorizedTopics.isNotEmpty()) throw TopicAuthorizationException(unauthorizedTopics)

                var shouldRetry = false
                val errors = response.errors()
                if (errors.isNotEmpty()) {
                    // if there were errors, we need to check whether they were fatal or whether
                    // we should just retry
                    log.debug("Topic metadata fetch included errors: {}", errors)
                    for ((topic, error) in errors) {
                        shouldRetry = if (error == Errors.INVALID_TOPIC_EXCEPTION)
                            throw InvalidTopicException("Topic '$topic' is invalid")
                        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                            // if a requested topic is unknown, we just continue and let it be absent
                            // in the returned map
                            continue
                        else if (error.exception is RetriableException) true
                        else throw KafkaException(
                            "Unexpected error fetching metadata for topic $topic",
                            error.exception,
                        )
                    }
                }
                if (!shouldRetry)
                    return cluster.topics.associateWith { topic -> cluster.partitionsForTopic(topic) }
            }
            timer.sleep(retryBackoffMs)
        } while (timer.isNotExpired)

        throw TimeoutException("Timeout expired while fetching topic metadata")
    }

    /**
     * Send Metadata Request to the least loaded node in Kafka cluster asynchronously
     * @return A future that indicates result of sent metadata request
     */
    private fun sendMetadataRequest(request: MetadataRequest.Builder): RequestFuture<ClientResponse> {
        val node = client.leastLoadedNode()
        return if (node == null) RequestFuture.noBrokersAvailable()
        else client.send(node, request)
    }
}
