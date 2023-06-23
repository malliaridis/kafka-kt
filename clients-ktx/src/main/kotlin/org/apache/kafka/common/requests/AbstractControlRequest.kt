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

package org.apache.kafka.common.requests

import org.apache.kafka.common.protocol.ApiKeys

// Abstract class for all control requests including UpdateMetadataRequest, LeaderAndIsrRequest and
// StopReplicaRequest
abstract class AbstractControlRequest protected constructor(
    api: ApiKeys,
    version: Short,
) : AbstractRequest(api, version) {

    abstract fun controllerId(): Int

    abstract val isKRaftController: Boolean

    abstract fun controllerEpoch(): Int

    abstract fun brokerEpoch(): Long

    abstract class Builder<T : AbstractRequest> protected constructor(
        api: ApiKeys,
        version: Short,
        protected val controllerId: Int,
        protected val controllerEpoch: Int,
        protected val brokerEpoch: Long,
        protected val kraftController: Boolean = false
    ) : AbstractRequest.Builder<T>(api, version)

    companion object {
        const val UNKNOWN_BROKER_EPOCH = -1L
    }
}
