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

import org.apache.kafka.common.message.CreatePartitionsResponseData
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

class CreatePartitionsResponse(
    private val data: CreatePartitionsResponseData,
) : AbstractResponse(ApiKeys.CREATE_PARTITIONS) {

    override fun data(): CreatePartitionsResponseData = data

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()
        data.results.forEach { result ->
            updateErrorCounts(counts, Errors.forCode(result.errorCode))
        }
        return counts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): CreatePartitionsResponse =
            CreatePartitionsResponse(
                CreatePartitionsResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
