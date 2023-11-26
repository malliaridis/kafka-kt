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

package org.apache.kafka.common

import java.util.Objects

/**
 * This represents universally unique identifier with topic id for a topic partition. This makes sure that topics
 * recreated with the same name will always have unique topic identifiers.
 *
 * @property topicId The topic id
 * @property topicPartition The topic partition
 */
data class TopicIdPartition(
    val topicId: Uuid,
    val topicPartition: TopicPartition,
) {

    /**
     * Create an instance with the provided parameters.
     *
     * @param topicId the topic id
     * @param partition the partition id
     * @param topic the topic name or null
     */
    constructor(
        topicId: Uuid,
        partition: Int,
        topic: String,
    ) : this(
        topicId = topicId,
        topicPartition = TopicPartition(topic, partition),
    )

    /**
     * @return Universally unique id representing this topic partition.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("topicId")
    )
    fun topicId(): Uuid {
        return topicId
    }

    /**
     * @return the topic name or null if it is unknown.
     */
    @Deprecated(
        message = "Use property access instead.",
        replaceWith = ReplaceWith("topic")
    )
    fun topic(): String {
        return topicPartition.topic
    }

    /**
     * The topic name or null if it is unknown.
     */
    val topic: String
        get() = topicPartition.topic

    /**
     * @return the partition id.
     */
    @Deprecated(
        message = "Use property access instead.",
        replaceWith = ReplaceWith("partition")
    )
    fun partition(): Int {
        return topicPartition.partition
    }

    /**
     * The partition id.
     */
    val partition: Int
        get() = topicPartition.partition

    /**
     * @return Topic partition representing this instance.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("topicPartition")
    )
    fun topicPartition(): TopicPartition {
        return topicPartition
    }

    override fun toString(): String = "$topicId:$topic-$partition"
}
