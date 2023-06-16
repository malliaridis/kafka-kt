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
import java.util.function.Consumer
import org.apache.kafka.common.message.OffsetDeleteResponseData
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * - Partition errors:
 * - [Errors.GROUP_SUBSCRIBED_TO_TOPIC]
 * - [Errors.TOPIC_AUTHORIZATION_FAILED]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 *
 * - Group or coordinator errors:
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.GROUP_AUTHORIZATION_FAILED]
 * - [Errors.INVALID_GROUP_ID]
 * - [Errors.GROUP_ID_NOT_FOUND]
 * - [Errors.NON_EMPTY_GROUP]
 */
class OffsetDeleteResponse(
    private val data: OffsetDeleteResponseData,
) : AbstractResponse(ApiKeys.OFFSET_DELETE) {

    override fun data(): OffsetDeleteResponseData = data

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()
        updateErrorCounts(counts, Errors.forCode(data.errorCode()))

        data.topics().forEach { topic ->
            topic.partitions().forEach { partition ->
                updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
            }
        }
        return counts
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 0

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): OffsetDeleteResponse =
            OffsetDeleteResponse(OffsetDeleteResponseData(ByteBufferAccessor(buffer), version))
    }
}
