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

import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.message.InitProducerIdResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import java.nio.ByteBuffer

class InitProducerIdRequest private constructor(
    private val data: InitProducerIdRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.INIT_PRODUCER_ID, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse? {
        val response = InitProducerIdResponseData()
            .setErrorCode(Errors.forException(e).code)
            .setProducerId(RecordBatch.NO_PRODUCER_ID)
            .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
            .setThrottleTimeMs(0)

        return InitProducerIdResponse(response)
    }

    override fun data(): InitProducerIdRequestData = data

    class Builder(
        val data: InitProducerIdRequestData,
    ) : AbstractRequest.Builder<InitProducerIdRequest>(ApiKeys.INIT_PRODUCER_ID) {

        override fun build(version: Short): InitProducerIdRequest {
            require(data.transactionTimeoutMs() > 0) {
                "transaction timeout value is not positive: " + data.transactionTimeoutMs()
            }
            require(!(data.transactionalId() != null && data.transactionalId().isEmpty())) {
                "Must set either a null or a non-empty transactional id."
            }

            return InitProducerIdRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): InitProducerIdRequest =
            InitProducerIdRequest(
                InitProducerIdRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
