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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * Options for [AdminClient.removeMembersFromConsumerGroup]. It carries the members to be removed
 * from the consumer group.
 *
 * The API of this class is evolving, see [AdminClient] for details.
 */
@Evolving
class RemoveMembersFromConsumerGroupOptions(
    val members: Set<MemberToRemove> = emptySet(),
) : AbstractOptions<RemoveMembersFromConsumerGroupOptions>() {
    var reason: String? = null

    /**
     * Sets an optional reason.
     */
    @Deprecated("User property instead")
    fun reason(reason: String?) {
        this.reason = reason
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("members"),
    )
    fun members(): Set<MemberToRemove> = members

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("reason"),
    )
    fun reason(): String? = reason

    fun removeAll(): Boolean = members.isEmpty()
}
