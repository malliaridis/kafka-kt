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

/**
 * Options for [Admin.listConsumerGroupOffsets] and [Admin.listConsumerGroupOffsets].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class ListConsumerGroupOffsetsOptions : AbstractOptions<ListConsumerGroupOffsetsOptions>() {


    @set:Deprecated("Since 3.3." +
            "Use [Admin#listConsumerGroupOffsets(Map, ListConsumerGroupOffsetsOptions)]" +
            " to specify topic partitions.")
    var topicPartitions: List<TopicPartition>? = null

    var requireStable = false

    /**
     * Set the topic partitions to list as part of the result. `null` includes all topic partitions.
     *
     * @param topicPartitions List of topic partitions to include
     * @return This ListGroupOffsetsOptions
     */
    @Deprecated("Since 3.3." +
            "Use [Admin#listConsumerGroupOffsets(Map, ListConsumerGroupOffsetsOptions)]" +
            " to specify topic partitions.")
    fun topicPartitions(topicPartitions: List<TopicPartition>?): ListConsumerGroupOffsetsOptions {
        this.topicPartitions = topicPartitions
        return this
    }

    /**
     * Sets an optional requireStable flag.
     */
    fun requireStable(requireStable: Boolean): ListConsumerGroupOffsetsOptions {
        this.requireStable = requireStable
        return this
    }

    /**
     * Returns a list of topic partitions to add as part of the result.
     */
    @Deprecated(
        """Since 3.3.
      Use {@link Admin#listConsumerGroupOffsets(java.util.Map, ListConsumerGroupOffsetsOptions)}
      to specify topic partitions."""
    )
    fun topicPartitions(): List<TopicPartition>? = topicPartitions

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("requireStable"),
    )
    fun requireStable(): Boolean = requireStable
}
