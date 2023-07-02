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

import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.message.AllocateProducerIdsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class AllocateProducerIdsRequest(
    private val data: AllocateProducerIdsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ALLOCATE_PRODUCER_IDS, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        AllocateProducerIdsResponse(
            AllocateProducerIdsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code)
        )

    override fun data(): AllocateProducerIdsRequestData = data

    class Builder(
        private val data: AllocateProducerIdsRequestData,
    ) : AbstractRequest.Builder<AllocateProducerIdsRequest>(ApiKeys.ALLOCATE_PRODUCER_IDS) {

        override fun build(version: Short): AllocateProducerIdsRequest =
            AllocateProducerIdsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AllocateProducerIdsRequest =
            AllocateProducerIdsRequest(
                AllocateProducerIdsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
