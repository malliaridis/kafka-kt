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

import org.apache.kafka.common.message.CreatePartitionsRequestData
import org.apache.kafka.common.message.CreatePartitionsResponseData
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer

class CreatePartitionsRequest internal constructor(
    private val data: CreatePartitionsRequestData,
    apiVersion: Short,
) : AbstractRequest(ApiKeys.CREATE_PARTITIONS, apiVersion) {

    override fun data(): CreatePartitionsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val response = CreatePartitionsResponseData()
        response.setThrottleTimeMs(throttleTimeMs)
        val (error, message) = ApiError.fromThrowable(e)

        response.results += data.topics.map { topic ->
            CreatePartitionsTopicResult()
                .setName(topic.name())
                .setErrorCode(error.code)
                .setErrorMessage(message)
        }

        return CreatePartitionsResponse(response)
    }

    class Builder(
        private val data: CreatePartitionsRequestData,
    ) : AbstractRequest.Builder<CreatePartitionsRequest>(ApiKeys.CREATE_PARTITIONS) {

        override fun build(version: Short): CreatePartitionsRequest =
            CreatePartitionsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): CreatePartitionsRequest =
            CreatePartitionsRequest(
                CreatePartitionsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
