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

import org.apache.kafka.common.message.CreateTopicsResponseData
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

/**
 * Possible error codes:
 *
 * - REQUEST_TIMED_OUT(7)
 * - INVALID_TOPIC_EXCEPTION(17)
 * - TOPIC_AUTHORIZATION_FAILED(29)
 * - TOPIC_ALREADY_EXISTS(36)
 * - INVALID_PARTITIONS(37)
 * - INVALID_REPLICATION_FACTOR(38)
 * - INVALID_REPLICA_ASSIGNMENT(39)
 * - INVALID_CONFIG(40)
 * - NOT_CONTROLLER(41)
 * - INVALID_REQUEST(42)
 * - POLICY_VIOLATION(44)
 */
class CreateTopicsResponse(
    private val data: CreateTopicsResponseData,
) : AbstractResponse(ApiKeys.CREATE_TOPICS) {

    override fun data(): CreateTopicsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val counts = HashMap<Errors, Int>()

        data.topics().forEach { result ->
            updateErrorCounts(counts, Errors.forCode(result.errorCode()))
        }

        return counts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 3

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): CreateTopicsResponse =
            CreateTopicsResponse(CreateTopicsResponseData(ByteBufferAccessor(buffer), version))
    }
}
