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

import org.apache.kafka.common.message.DescribeGroupsRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DescribeGroupsRequest private constructor(
    private val data: DescribeGroupsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_GROUPS, version) {

    override fun data(): DescribeGroupsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        DescribeGroupsResponse.fromError(
            throttleTimeMs = if (version.toInt() == 0) AbstractResponse.DEFAULT_THROTTLE_TIME
            else throttleTimeMs,
            error = Errors.forException(e),
            groupIds = data.groups
        )

    class Builder(
        private val data: DescribeGroupsRequestData,
    ) : AbstractRequest.Builder<DescribeGroupsRequest>(ApiKeys.DESCRIBE_GROUPS) {

        override fun build(version: Short): DescribeGroupsRequest =
            DescribeGroupsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeGroupsRequest =
            DescribeGroupsRequest(
                DescribeGroupsRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
