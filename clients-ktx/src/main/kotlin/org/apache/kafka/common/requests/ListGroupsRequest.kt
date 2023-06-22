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

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.ListGroupsRequestData
import org.apache.kafka.common.message.ListGroupsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

/**
 * Possible error codes:
 *
 * - COORDINATOR_LOAD_IN_PROGRESS (14)
 * - COORDINATOR_NOT_AVAILABLE (15)
 * - AUTHORIZATION_FAILED (29)
 */
class ListGroupsRequest(
    private val data: ListGroupsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.LIST_GROUPS, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): ListGroupsResponse {
        val listGroupsResponseData = ListGroupsResponseData()
            .setGroups(emptyList())
            .setErrorCode(Errors.forException(e).code)

        if (version >= 1) listGroupsResponseData.setThrottleTimeMs(throttleTimeMs)
        return ListGroupsResponse(listGroupsResponseData)
    }

    override fun data(): ListGroupsRequestData = data

    class Builder(
        private val data: ListGroupsRequestData,
    ) : AbstractRequest.Builder<ListGroupsRequest>(ApiKeys.LIST_GROUPS) {

        override fun build(version: Short): ListGroupsRequest {
            if (data.statesFilter().isNotEmpty() && version < 4)
                throw UnsupportedVersionException(
                    "The broker only supports ListGroups v$version, but we need v4 or newer to " +
                        "request groups by states."
                )

            return ListGroupsRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ListGroupsRequest =
            ListGroupsRequest(
                ListGroupsRequestData(ByteBufferAccessor(buffer), version),
                version
            )
    }
}
