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

import java.io.Serializable

/**
 * The topic name, partition number and the brokerId of the replica
 */
data class TopicPartitionReplica(
    val topic: String,
    val partition: Int,
    val brokerId: Int,
) : Serializable {

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("topic")
    )
    fun topic(): String {
        return topic
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("partition")
    )
    fun partition(): Int {
        return partition
    }

    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("brokerId")
    )
    fun brokerId(): Int {
        return brokerId
    }

    override fun toString(): String = "$topic-$partition-$brokerId"
}
