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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import java.util.*

/**
 * Specification of consumer group offsets to list using [Admin.listConsumerGroupOffsets].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
data class ListConsumerGroupOffsetsSpec(
    var topicPartitions: Collection<TopicPartition>? = null,
) {

    /**
     * Set the topic partitions whose offsets are to be listed for a consumer group.
     * `null` includes all topic partitions.
     *
     * @param topicPartitions List of topic partitions to include
     * @return This ListConsumerGroupOffsetSpec
     */
    @Deprecated("User property instead")
    fun topicPartitions(topicPartitions: Collection<TopicPartition>?): ListConsumerGroupOffsetsSpec {
        this.topicPartitions = topicPartitions
        return this
    }

    /**
     * Returns the topic partitions whose offsets are to be listed for a consumer group.
     * `null` indicates that offsets of all partitions of the group are to be listed.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topicPartitions"),
    )
    fun topicPartitions(): Collection<TopicPartition>? = topicPartitions

    override fun toString(): String =
        "ListConsumerGroupOffsetsSpec(topicPartitions=$topicPartitions)"
}
