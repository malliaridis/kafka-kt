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
import java.util.concurrent.ExecutionException

/**
 * The result of the [KafkaAdminClient.describeTopics] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeTopicsResult internal constructor(
    topicIdFutures: Map<Uuid, KafkaFuture<TopicDescription>>? = null,
    nameFutures: Map<String, KafkaFuture<TopicDescription>>? = null,
) {
    private val topicIdFutures: Map<Uuid, KafkaFuture<TopicDescription>>?
    private val nameFutures: Map<String, KafkaFuture<TopicDescription>>?

    @Deprecated("")
    internal constructor(futures: Map<String, KafkaFuture<TopicDescription>>) : this(
        topicIdFutures = null,
        nameFutures = futures,
    )

    // VisibleForTesting
    init {
        require(topicIdFutures == null || nameFutures == null) {
            "topicIdFutures and nameFutures cannot both be specified."
        }
        require(topicIdFutures != null || nameFutures != null) {
            "topicIdFutures and nameFutures cannot both be null."
        }

        this.topicIdFutures = topicIdFutures
        this.nameFutures = nameFutures
    }

    /**
     * Use when [Admin.describeTopics] used a TopicIdCollection
     *
     * @return a map from topic IDs to futures which can be used to check the status of individual
     * topics if the request used topic IDs, otherwise return `null`.
     */
    fun topicIdValues(): Map<Uuid, KafkaFuture<TopicDescription>>? = topicIdFutures

    /**
     * Use when [Admin.describeTopics] used a TopicNameCollection
     *
     * @return a map from topic names to futures which can be used to check the status of individual
     * topics if the request used topic names, otherwise return `null`.
     */
    fun topicNameValues(): Map<String, KafkaFuture<TopicDescription>>? = nameFutures

    /**
     * @return a map from topic names to futures which can be used to check the status of individual
     * topics if the request used topic names, otherwise return null.
     */
    @Deprecated("Since 3.1.0 use {@link #topicNameValues} instead")
    fun values(): Map<String, KafkaFuture<TopicDescription>>? = nameFutures

    /**
     * @return A future map from topic names to descriptions which can be used to check the status
     * of individual description if the describe topic request used topic names, otherwise return
     * `null`, this request succeeds only if all the topic descriptions succeed
     */
    @Deprecated("Since 3.1.0 use {@link #allTopicNames()} instead")
    fun all(): KafkaFuture<Map<String, TopicDescription>> = all(nameFutures!!)

    /**
     * @return A future map from topic names to descriptions which can be used to check the status
     * of individual description if the describe topic request used topic names, otherwise return
     * `null`, this request succeeds only if all the topic descriptions succeed
     */
    fun allTopicNames(): KafkaFuture<Map<String, TopicDescription>> = all(nameFutures!!)

    /**
     * @return A future map from topic ids to descriptions which can be used to check the status of
     * individual description if the describe topic request used topic ids, otherwise return `null`,
     * this request succeeds only if all the topic descriptions succeed
     */
    fun allTopicIds(): KafkaFuture<Map<Uuid, TopicDescription>> = all(topicIdFutures!!)

    companion object {

        fun ofTopicIds(
            topicIdFutures: Map<Uuid, KafkaFuture<TopicDescription>>?,
        ): DescribeTopicsResult = DescribeTopicsResult(topicIdFutures, null)

        fun ofTopicNames(
            nameFutures: Map<String, KafkaFuture<TopicDescription>>?,
        ): DescribeTopicsResult = DescribeTopicsResult(null, nameFutures)

        /**
         * Return a future which succeeds only if all the topic descriptions succeed.
         */
        private fun <T> all(
            futures: Map<T, KafkaFuture<TopicDescription>>,
        ): KafkaFuture<Map<T, TopicDescription>> {
            val future: KafkaFuture<Unit> = KafkaFuture.allOf(futures.values)
            return future.thenApply {
                val descriptions: MutableMap<T, TopicDescription> = HashMap(futures.size)
                for ((key, value) in futures) {
                    try {
                        descriptions[key] = value.get()
                    } catch (e: InterruptedException) {
                        // This should be unreachable, because allOf ensured that all the futures
                        // completed successfully.
                        throw RuntimeException(e)
                    } catch (e: ExecutionException) {
                        throw RuntimeException(e)
                    }
                }
                descriptions
            }
        }
    }
}
