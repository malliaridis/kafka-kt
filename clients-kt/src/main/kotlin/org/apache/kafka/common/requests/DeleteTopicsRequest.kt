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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.message.DeleteTopicsResponseData
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer
import java.util.stream.Collectors

class DeleteTopicsRequest private constructor(
    private val data: DeleteTopicsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DELETE_TOPICS, version) {

    override fun data(): DeleteTopicsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val response = DeleteTopicsResponseData()
        if (version >= 1) response.setThrottleTimeMs(throttleTimeMs)

        val (error) = ApiError.fromThrowable(e)

        response.responses().addAll(
            topics().map { topic ->
                DeletableTopicResult()
                    .setName(topic.name())
                    .setTopicId(topic.topicId())
                    .setErrorCode(error.code)
            }
        )

        return DeleteTopicsResponse(response)
    }

    fun topicNames(): List<String> =
        if (version >= 6) data.topics().map(DeleteTopicState::name)
        else data.topicNames()

    fun numberOfTopics(): Int =
        if (version >= 6) data.topics().size
        else data.topicNames().size

    fun topicIds(): List<Uuid> =
        if (version >= 6) data.topics().map(DeleteTopicState::topicId)
        else emptyList()

    fun topics(): List<DeleteTopicState> =
        if (version >= 6) data.topics()
        else data.topicNames().map { name -> DeleteTopicState().setName(name)}

    class Builder(
        private val data: DeleteTopicsRequestData,
    ) : AbstractRequest.Builder<DeleteTopicsRequest>(ApiKeys.DELETE_TOPICS) {

        override fun build(version: Short): DeleteTopicsRequest {
            if (version >= 6 && data.topicNames().isNotEmpty())
                data.setTopics(groupByTopic(data.topicNames()))

            return DeleteTopicsRequest(data, version)
        }

        private fun groupByTopic(topics: List<String>): List<DeleteTopicState> =
            topics.map { DeleteTopicState().setName(it) }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DeleteTopicsRequest =
            DeleteTopicsRequest(
                DeleteTopicsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
