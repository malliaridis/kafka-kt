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

import org.apache.kafka.common.message.DescribeProducersRequestData
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest
import org.apache.kafka.common.message.DescribeProducersResponseData
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DescribeProducersRequest private constructor(
    private val data: DescribeProducersRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_PRODUCERS, version) {

    override fun data(): DescribeProducersRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): DescribeProducersResponse {
        val error = Errors.forException(e)
        val response = DescribeProducersResponseData()

        response.topics().addAll(
            data.topics().map { topicRequest ->
                TopicResponse().setName(topicRequest.name())
                    .setPartitions(
                        topicRequest.partitionIndexes().map { partitionId ->
                            DescribeProducersResponseData.PartitionResponse()
                                .setPartitionIndex(partitionId)
                                .setErrorCode(error.code)
                        }
                    )
            }
        )

        return DescribeProducersResponse(response)
    }

    override fun toString(verbose: Boolean): String = data.toString()

    class Builder(
        val data: DescribeProducersRequestData,
    ) : AbstractRequest.Builder<DescribeProducersRequest>(ApiKeys.DESCRIBE_PRODUCERS) {

        fun addTopic(topic: String?): TopicRequest {
            val topicRequest = TopicRequest().setName(topic)
            data.topics().add(topicRequest)
            return topicRequest
        }

        override fun build(version: Short): DescribeProducersRequest =
            DescribeProducersRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeProducersRequest =
            DescribeProducersRequest(
                DescribeProducersRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
