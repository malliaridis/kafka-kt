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

import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.Node
import org.apache.kafka.common.acl.AclOperation

/**
 * A detailed description of a single consumer group in the cluster.
 *
 * @property groupId
 * @property isSimpleConsumerGroup If consumer group is simple or not.
 */
class ConsumerGroupDescription(
    val groupId: String = "",
    val isSimpleConsumerGroup: Boolean,
    val members: Collection<MemberDescription> = emptyList(),
    val partitionAssignor: String = "",
    private val state: ConsumerGroupState,
    private val coordinator: Node,
    val authorizedOperations: Set<AclOperation> = emptySet()
) {

    /**
     * The id of the consumer group.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("groupId"),
    )
    fun groupId(): String = groupId

    /**
     * A list of the members of the consumer group.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("members"),
    )
    fun members(): Collection<MemberDescription> = members

    /**
     * The consumer group partition assignor.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("partitionAssignor"),
    )
    fun partitionAssignor(): String = partitionAssignor

    /**
     * The consumer group state, or UNKNOWN if the state is too new for us to parse.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("state"),
    )
    fun state(): ConsumerGroupState = state

    /**
     * The consumer group coordinator, or null if the coordinator is not known.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("coordinator"),
    )
    fun coordinator(): Node = coordinator

    /**
     * authorizedOperations for this group, or null if that information is not known.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("authorizedOperations"),
    )
    fun authorizedOperations(): Set<AclOperation> = authorizedOperations

    override fun toString(): String {
        return "(groupId=$groupId" +
                ", isSimpleConsumerGroup=$isSimpleConsumerGroup" +
                ", members=${members.joinToString(",")}" +
                ", partitionAssignor=$partitionAssignor" +
                ", state=$state" +
                ", coordinator=$coordinator" +
                ", authorizedOperations=$authorizedOperations" +
                ")"
    }
}
