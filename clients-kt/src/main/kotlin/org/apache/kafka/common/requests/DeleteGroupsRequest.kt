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

import org.apache.kafka.common.message.DeleteGroupsRequestData
import org.apache.kafka.common.message.DeleteGroupsResponseData
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DeleteGroupsRequest(
    private val data: DeleteGroupsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DELETE_GROUPS, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val error = Errors.forException(e)
        val groupResults = DeletableGroupResultCollection()
        for (groupId in data.groupsNames) {
            groupResults.add(
                DeletableGroupResult()
                    .setGroupId(groupId)
                    .setErrorCode(error.code)
            )
        }
        return DeleteGroupsResponse(
            DeleteGroupsResponseData()
                .setResults(groupResults)
                .setThrottleTimeMs(throttleTimeMs)
        )
    }

    override fun data(): DeleteGroupsRequestData = data

    class Builder(
        private val data: DeleteGroupsRequestData,
    ) : AbstractRequest.Builder<DeleteGroupsRequest>(ApiKeys.DELETE_GROUPS) {

        override fun build(version: Short): DeleteGroupsRequest =
            DeleteGroupsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DeleteGroupsRequest =
            DeleteGroupsRequest(
                DeleteGroupsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
