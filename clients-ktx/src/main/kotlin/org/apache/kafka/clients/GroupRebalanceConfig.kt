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

package org.apache.kafka.clients

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.requests.JoinGroupRequest

/**
 * Class to extract group rebalance related configs.
 */
// constructor for testing purposes
data class GroupRebalanceConfig(
    val sessionTimeoutMs: Int,
    val rebalanceTimeoutMs: Int,
    val heartbeatIntervalMs: Int,
    val groupId: String,
    val groupInstanceId: String?,
    val retryBackoffMs: Long,
    val leaveGroupOnClose: Boolean,
) {

    constructor(
        config: AbstractConfig,
        protocolType: ProtocolType,
    ) : this(
        sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG),

        // Consumer and Connect use different config names for defining rebalance timeout
        rebalanceTimeoutMs =
            if (protocolType == ProtocolType.CONSUMER)
                config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG)
            else config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG),

        heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG),
        groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG),

        // Static membership is only introduced in consumer API.
        groupInstanceId = if (protocolType == ProtocolType.CONSUMER) {
            val groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG)
            JoinGroupRequest.validateGroupInstanceId(groupInstanceId)
            groupInstanceId
        } else null,

        retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),

        // Internal leave group config is only defined in Consumer.
        leaveGroupOnClose =
            if (protocolType == ProtocolType.CONSUMER)
                config.getBoolean("internal.leave.group.on.close")
            else true,
    )

    enum class ProtocolType {
        CONSUMER,
        CONNECT;

        override fun toString(): String = super.toString().lowercase()
    }
}
