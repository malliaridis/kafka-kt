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

/**
 * The result of the [Admin.deleteTopics] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @property topicIdFutures Map from topic IDs to futures which can be used to check the status of
 * individual deletions if the deleteTopics request used topic IDs. Cannot coexist with [nameFutures].
 * @property nameFutures Map from topic names to futures which can be used to check the status of
 * individual deletions if the deleteTopics request used topic names. Cannot coexist with [topicIdFutures].
 */
@Evolving
class DeleteTopicsResult internal constructor(
    val topicIdFutures: Map<Uuid, KafkaFuture<Unit>>? = null,
    val nameFutures: Map<String, KafkaFuture<Unit>>? = null,
) {

    init {
        require(!(topicIdFutures != null && nameFutures != null)) {
            "topicIdFutures and nameFutures cannot both be specified."
        }
        require(!(topicIdFutures == null && nameFutures == null)) {
            "topicIdFutures and nameFutures cannot both be null."
        }
    }

    /**
     * Use when [Admin.deleteTopics] used a TopicIdCollection
     * @return a map from topic IDs to futures which can be used to check the status of
     * individual deletions if the deleteTopics request used topic IDs. Otherwise return null.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("topicIdFutures")
    )
    fun topicIdValues(): Map<Uuid, KafkaFuture<Unit>>? {
        return topicIdFutures
    }

    /**
     * Use when [Admin.deleteTopics] used a TopicNameCollection
     * @return a map from topic names to futures which can be used to check the status of
     * individual deletions if the deleteTopics request used topic names. Otherwise return null.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("nameFutures")
    )
    fun topicNameValues(): Map<String, KafkaFuture<Unit>>? {
        return nameFutures
    }

    /**
     * @return a map from topic names to futures which can be used to check the status of
     * individual deletions if the deleteTopics request used topic names. Otherwise return null.
     */
    @Deprecated("Since 3.0 use {@link #topicNameValues} instead")
    fun values(): Map<String, KafkaFuture<Unit>>? {
        return nameFutures
    }

    /**
     * @return a future which succeeds only if all the topic deletions succeed.
     */
    fun all(): KafkaFuture<Unit> {
        return if (topicIdFutures == null) KafkaFuture.allOf(*nameFutures!!.values.toTypedArray())
        else KafkaFuture.allOf(*topicIdFutures.values.toTypedArray())
    }

    companion object {
        fun ofTopicIds(topicIdFutures: Map<Uuid, KafkaFuture<Unit>>): DeleteTopicsResult {
            return DeleteTopicsResult(topicIdFutures = topicIdFutures)
        }

        fun ofTopicNames(nameFutures: Map<String, KafkaFuture<Unit>>): DeleteTopicsResult {
            return DeleteTopicsResult(nameFutures = nameFutures)
        }
    }
}
