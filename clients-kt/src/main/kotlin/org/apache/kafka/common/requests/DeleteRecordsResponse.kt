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

import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

/**
 * Possible error code:
 *
 * - OFFSET_OUT_OF_RANGE (1)
 * - UNKNOWN_TOPIC_OR_PARTITION (3)
 * - NOT_LEADER_OR_FOLLOWER (6)
 * - REQUEST_TIMED_OUT (7)
 * - UNKNOWN (-1)
 */
class DeleteRecordsResponse(
    private val data: DeleteRecordsResponseData,
) : AbstractResponse(ApiKeys.DELETE_RECORDS) {

    override fun data(): DeleteRecordsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()

        data.topics().forEach { topicResponses ->
            topicResponses.partitions().forEach { response ->
                updateErrorCounts(errorCounts, Errors.forCode(response.errorCode()))
            }
        }

        return errorCounts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        const val INVALID_LOW_WATERMARK = -1L

        fun parse(buffer: ByteBuffer, version: Short): DeleteRecordsResponse =
            DeleteRecordsResponse(DeleteRecordsResponseData(ByteBufferAccessor(buffer), version))
    }
}
