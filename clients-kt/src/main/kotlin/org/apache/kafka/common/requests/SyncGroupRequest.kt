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

import java.nio.ByteBuffer
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.SyncGroupRequestData
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment
import org.apache.kafka.common.message.SyncGroupResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class SyncGroupRequest(
    private val data: SyncGroupRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.SYNC_GROUP, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        SyncGroupResponse(
            SyncGroupResponseData()
                .setErrorCode(Errors.forException(e).code)
                .setAssignment(ByteArray(0))
                .setThrottleTimeMs(throttleTimeMs)
        )

    fun groupAssignments(): Map<String, ByteBuffer> {
        val groupAssignments = mutableMapOf<String, ByteBuffer>()

        for (assignment: SyncGroupRequestAssignment in data.assignments())
            groupAssignments[assignment.memberId()] = ByteBuffer.wrap(assignment.assignment())

        return groupAssignments
    }

    /**
     * ProtocolType and ProtocolName are mandatory since version 5. This methods verifies that
     * they are defined for version 5 or higher, or returns true otherwise for older versions.
     */
    fun areMandatoryProtocolTypeAndNamePresent(): Boolean {
        return if (version >= 5) data.protocolType() != null && data.protocolName() != null
        else true
    }

    override fun data(): SyncGroupRequestData = data

    class Builder(
        private val data: SyncGroupRequestData,
    ) : AbstractRequest.Builder<SyncGroupRequest>(ApiKeys.SYNC_GROUP) {

        override fun build(version: Short): SyncGroupRequest {
            if (data.groupInstanceId() != null && version < 3)
                throw UnsupportedVersionException(
                    "The broker sync group protocol version $version does not support usage of " +
                            "config group.instance.id."
                )

            return SyncGroupRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): SyncGroupRequest =
            SyncGroupRequest(SyncGroupRequestData(ByteBufferAccessor((buffer)), version),
                version
            )
    }
}
