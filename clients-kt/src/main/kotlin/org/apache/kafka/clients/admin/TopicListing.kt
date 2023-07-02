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

import org.apache.kafka.common.Uuid

/**
 * A listing of a topic in the cluster.
 *
 * @constructor Create an instance with the specified parameters.
 * @property name The topic name
 * @property topicId The topic id.
 * @property isInternal Whether the topic is internal to Kafka. An example of an internal topic is
 * the offsets and group management topic: __consumer_offsets.
 */
data class TopicListing(
    val name: String,
    val topicId: Uuid,
    private val isInternal: Boolean,
) {

    /**
     * Create an instance with the specified parameters.
     *
     * @param name The topic name
     * @param internal Whether the topic is internal to Kafka
     */
    @Deprecated("Since 3.0 use {@link #TopicListing(String, Uuid, boolean)} instead")
    constructor(name: String, internal: Boolean) : this(
        name = name,
        isInternal = internal,
        topicId = Uuid.ZERO_UUID,
    )

    /**
     * The id of the topic.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topicId"),
    )
    fun topicId(): Uuid = topicId

    /**
     * The name of the topic.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String = name

    override fun toString(): String = "(name=$name, topicId=$topicId, internal=$isInternal)"
}
