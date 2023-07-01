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
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.FindCoordinatorResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class FindCoordinatorRequest private constructor(
    private val data: FindCoordinatorRequestData,
    version: Short
) : AbstractRequest(ApiKeys.FIND_COORDINATOR, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val response = FindCoordinatorResponseData()
        if (version >= 2) response.setThrottleTimeMs(throttleTimeMs)

        val error = Errors.forException(e)

        return if (version < MIN_BATCHED_VERSION)
            FindCoordinatorResponse.prepareOldResponse(error, Node.noNode())
        else FindCoordinatorResponse.prepareErrorResponse(error, data.coordinatorKeys())
    }

    override fun data(): FindCoordinatorRequestData = data

    enum class CoordinatorType(val id: Byte) {

        GROUP(0.toByte()),
        TRANSACTION(1.toByte());

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("id")
        )
        fun id(): Byte = id

        companion object {
            fun forId(id: Byte): CoordinatorType {
                return when (id) {
                    0.toByte() -> GROUP
                    1.toByte() -> TRANSACTION
                    else -> throw InvalidRequestException("Unknown coordinator type received: $id")
                }
            }
        }
    }

    class Builder(
        private val data: FindCoordinatorRequestData,
    ) : AbstractRequest.Builder<FindCoordinatorRequest>(ApiKeys.FIND_COORDINATOR) {

        override fun build(version: Short): FindCoordinatorRequest {

            if (version < 1 && data.keyType() == CoordinatorType.TRANSACTION.id)
                throw UnsupportedVersionException(
                    "Cannot create a v$version FindCoordinator request because we require " +
                        "features supported only in 2 or later."
                )

            val batchedKeys = data.coordinatorKeys().size
            if (version < MIN_BATCHED_VERSION) {
                if (batchedKeys > 1) throw NoBatchedFindCoordinatorsException(
                    "Cannot create a v$version FindCoordinator request because we require " +
                        "features supported only in $MIN_BATCHED_VERSION or later."
                )
                if (batchedKeys == 1) {
                    data.setKey(data.coordinatorKeys()[0])
                    data.setCoordinatorKeys(emptyList())
                }
            } else if (batchedKeys == 0 && data.key() != null) {
                data.setCoordinatorKeys(listOf(data.key()))
                data.setKey("") // default value
            }
            return FindCoordinatorRequest(data, version)
        }

        override fun toString(): String = data.toString()

        fun data(): FindCoordinatorRequestData = data
    }

    /**
     * Indicates that it is not possible to lookup coordinators in batches with FindCoordinator.
     * Instead, coordinators must be looked up one by one.
     */
    class NoBatchedFindCoordinatorsException(
        message: String? = null,
        cause: Throwable? = null,
    ) : UnsupportedVersionException(message = message, cause = cause) {

        companion object {
            private const val serialVersionUID = 1L
        }
    }

    companion object {

        const val MIN_BATCHED_VERSION: Short = 4

        fun parse(buffer: ByteBuffer, version: Short): FindCoordinatorRequest {
            return FindCoordinatorRequest(
                FindCoordinatorRequestData(ByteBufferAccessor(buffer), version),
                version
            )
        }
    }
}
