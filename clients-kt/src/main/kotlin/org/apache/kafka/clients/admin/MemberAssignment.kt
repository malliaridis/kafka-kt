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

/**
 * A description of the assignments of a specific group member.
 *
 * @constructor Creates an instance with the specified parameters.
 * @property topicPartitions List of topic partitions
 */
class MemberAssignment(topicPartitions: Set<TopicPartition> = emptySet()) {

    val topicPartitions: Set<TopicPartition>

    init {
        this.topicPartitions = topicPartitions.toSet()
    }

    /**
     * The topic partitions assigned to a group member.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topicPartitions"),
    )
    fun topicPartitions(): Set<TopicPartition> = topicPartitions

    override fun toString(): String = "(topicPartitions=${topicPartitions.joinToString(",")})"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MemberAssignment

        return topicPartitions == other.topicPartitions
    }

    override fun hashCode(): Int {
        return topicPartitions.hashCode()
    }
}
