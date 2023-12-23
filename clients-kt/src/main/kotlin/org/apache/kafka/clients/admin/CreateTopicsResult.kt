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

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.errors.ApiException

/**
 * The result of [Admin.createTopics].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class CreateTopicsResult(
    private val futures: Map<String, KafkaFuture<TopicMetadataAndConfig>>,
) {

    /**
     * Return a map from topic names to futures, which can be used to check the status of individual
     * topic creations.
     */
    fun values(): Map<String, KafkaFuture<Unit>> =
        futures.mapValues { (_, value) -> value.thenApply {} }

    /**
     * Return a future which succeeds if all the topic creations succeed.
     */
    fun all(): KafkaFuture<Unit> = KafkaFuture.allOf(futures.values)

    /**
     * Returns a future that provides topic configs for the topic when the request completes.
     *
     * If broker version doesn't support replication factor in the response, throw
     * [org.apache.kafka.common.errors.UnsupportedVersionException].
     *
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * [org.apache.kafka.common.errors.TopicAuthorizationException] is thrown if user does not
     * have permission to describe topic configs.
     */
    fun config(topic: String): KafkaFuture<Config> =
        futures[topic]!!.thenApply(CreateTopicsResult.TopicMetadataAndConfig::config)

    /**
     * Returns a future that provides topic ID for the topic when the request completes.
     *
     * If broker version doesn't support replication factor in the response, throw
     * [org.apache.kafka.common.errors.UnsupportedVersionException].
     *
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * [org.apache.kafka.common.errors.TopicAuthorizationException] is thrown if user does not
     * have permission to describe topic configs.
     */
    fun topicId(topic: String): KafkaFuture<Uuid> =
        futures[topic]!!.thenApply(CreateTopicsResult.TopicMetadataAndConfig::topicId)

    /**
     * Returns a future that provides number of partitions in the topic when the request completes.
     *
     *
     * If broker version doesn't support replication factor in the response, throw
     * [org.apache.kafka.common.errors.UnsupportedVersionException].
     *
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * [org.apache.kafka.common.errors.TopicAuthorizationException] is thrown if user does not
     * have permission to describe topic configs.
     */
    fun numPartitions(topic: String): KafkaFuture<Int> =
        futures[topic]!!.thenApply(CreateTopicsResult.TopicMetadataAndConfig::numPartitions)

    /**
     * Returns a future that provides replication factor for the topic when the request completes.
     *
     *
     * If broker version doesn't support replication factor in the response, throw
     * [org.apache.kafka.common.errors.UnsupportedVersionException].
     *
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * [org.apache.kafka.common.errors.TopicAuthorizationException] is thrown if user does not
     * have permission to describe topic configs.
     */
    fun replicationFactor(topic: String): KafkaFuture<Int> =
        futures[topic]!!.thenApply(CreateTopicsResult.TopicMetadataAndConfig::replicationFactor)

    class TopicMetadataAndConfig {
        private val exception: ApiException?
        private val topicId: Uuid
        private val numPartitions: Int
        private val replicationFactor: Int
        private val config: Config?

        constructor(topicId: Uuid, numPartitions: Int, replicationFactor: Int, config: Config) {
            exception = null
            this.topicId = topicId
            this.numPartitions = numPartitions
            this.replicationFactor = replicationFactor
            this.config = config
        }

        constructor(exception: ApiException) {
            this.exception = exception
            topicId = Uuid.ZERO_UUID
            numPartitions = UNKNOWN
            replicationFactor = UNKNOWN
            config = null
        }

        fun topicId(): Uuid {
            ensureSuccess()
            return topicId
        }

        fun numPartitions(): Int {
            ensureSuccess()
            return numPartitions
        }

        fun replicationFactor(): Int {
            ensureSuccess()
            return replicationFactor
        }

        fun config(): Config {
            ensureSuccess()
            return config!!
        }

        private fun ensureSuccess() {
            if (exception != null) throw exception
        }
    }

    companion object {
        const val UNKNOWN = -1
    }
}
