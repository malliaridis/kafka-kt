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

@Evolving
data class TransactionDescription(
    val coordinatorId: Int,
    val state: TransactionState,
    val producerId: Long,
    val producerEpoch: Int,
    val transactionTimeoutMs: Long,
    val transactionStartTimeMs: Long?,
    val topicPartitions: Set<TopicPartition>
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("coordinatorId"),
    )
    fun coordinatorId(): Int = coordinatorId

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("state"),
    )
    fun state(): TransactionState = state

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("producerId"),
    )
    fun producerId(): Long = producerId

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("producerEpoch"),
    )
    fun producerEpoch(): Int = producerEpoch

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("transactionTimeoutMs"),
    )
    fun transactionTimeoutMs(): Long = transactionTimeoutMs

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("transactionStartTimeMs"),
    )
    fun transactionStartTimeMs(): Long? = transactionStartTimeMs

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("topicPartitions"),
    )
    fun topicPartitions(): Set<TopicPartition> = topicPartitions

    override fun toString(): String {
        return "TransactionDescription(" +
                "coordinatorId=$coordinatorId" +
                ", state=$state" +
                ", producerId=$producerId" +
                ", producerEpoch=$producerEpoch" +
                ", transactionTimeoutMs=$transactionTimeoutMs" +
                ", transactionStartTimeMs=$transactionStartTimeMs" +
                ", topicPartitions=$topicPartitions" +
                ')'
    }
}
