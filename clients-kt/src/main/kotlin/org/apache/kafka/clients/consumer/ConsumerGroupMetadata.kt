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

package org.apache.kafka.clients.consumer

import org.apache.kafka.common.requests.JoinGroupRequest
import java.util.*

/**
 * A metadata struct containing the consumer group information.
 *
 * Note: Any change to this class is considered public and requires a KIP.
 */
data class ConsumerGroupMetadata(
    val groupId: String,
    val generationId: Int = JoinGroupRequest.UNKNOWN_GENERATION_ID,
    val memberId: String = JoinGroupRequest.UNKNOWN_MEMBER_ID,
    val groupInstanceId: String? = null,
) {

    init {
        requireNotNull(groupInstanceId) { "group.instance.id can't be null" }
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("groupId"),
    )
    fun groupId(): String = groupId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("generationId"),
    )
    fun generationId(): Int = generationId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("memberId"),
    )
    fun memberId(): String = memberId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("groupInstanceId"),
    )
    fun groupInstanceId(): String? = groupInstanceId

    override fun toString(): String =
        "GroupMetadata(" +
                "groupId = $groupId" +
                ", generationId = $generationId" +
                ", memberId = $memberId" +
                ", groupInstanceId = ${groupInstanceId ?: ""}" +
                ")"
}
