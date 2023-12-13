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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.OffsetCommitRequest

class GroupState {

    val groupId: String?

    val groupInstanceId: String?

    var generation = Generation.NO_GENERATION

    constructor(groupId: String?, groupInstanceId: String?) {
        this.groupId = groupId
        this.groupInstanceId = groupInstanceId
    }

    constructor(config: GroupRebalanceConfig) {
        groupId = config.groupId
        groupInstanceId = config.groupInstanceId
    }

    data class Generation internal constructor(
        val generationId: Int,
        val memberId: String,
        val protocolName: String?,
    ) {

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        fun hasMemberId(): Boolean = memberId.isNotEmpty()

        override fun toString(): String =
            "Generation{generationId=$generationId, memberId='$memberId', protocol='$protocolName'}"

        companion object {
            val NO_GENERATION = Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null
            )
        }
    }
}
