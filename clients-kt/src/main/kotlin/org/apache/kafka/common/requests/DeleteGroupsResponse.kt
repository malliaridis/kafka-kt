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

import org.apache.kafka.common.message.DeleteGroupsResponseData
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

/**
 * Possible error codes:
 *
 * - COORDINATOR_LOAD_IN_PROGRESS (14)
 * - COORDINATOR_NOT_AVAILABLE(15)
 * - NOT_COORDINATOR (16)
 * - INVALID_GROUP_ID(24)
 * - GROUP_AUTHORIZATION_FAILED(30)
 * - NON_EMPTY_GROUP(68)
 * - GROUP_ID_NOT_FOUND(69)
 */
class DeleteGroupsResponse(
    private val data: DeleteGroupsResponseData,
) : AbstractResponse(ApiKeys.DELETE_GROUPS) {

    override fun data(): DeleteGroupsResponseData = data

    fun errors(): Map<String, Errors> = data.results.associate { result ->
        result.groupId to Errors.forCode(result.errorCode)
    }

    @Throws(IllegalArgumentException::class)
    operator fun get(group: String): Errors {
        val result = requireNotNull(data.results.find(group)) {
            "could not find group $group in the delete group response"
        }

        return Errors.forCode(result.errorCode)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()

        data.results.forEach { result: DeletableGroupResult ->
            updateErrorCounts(counts, Errors.forCode(result.errorCode))
        }

        return counts
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DeleteGroupsResponse =
            DeleteGroupsResponse(DeleteGroupsResponseData(ByteBufferAccessor(buffer), version))
    }
}
