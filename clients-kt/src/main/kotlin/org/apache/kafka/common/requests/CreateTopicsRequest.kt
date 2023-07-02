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

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer
import java.util.stream.Collectors

class CreateTopicsRequest(
    private val data: CreateTopicsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.CREATE_TOPICS, version) {

    override fun data(): CreateTopicsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val response = CreateTopicsResponseData()
        if (version >= 2) response.setThrottleTimeMs(throttleTimeMs)
        val apiError = ApiError.fromThrowable(e)

        response.topics().addAll(
            data.topics().map { topic ->
                CreatableTopicResult()
                    .setName(topic.name())
                    .setErrorCode(apiError.error.code)
                    .setErrorMessage(apiError.message)
            }
        )

        return CreateTopicsResponse(response)
    }

    class Builder(
        private val data: CreateTopicsRequestData,
    ) : AbstractRequest.Builder<CreateTopicsRequest>(ApiKeys.CREATE_TOPICS) {

        override fun build(version: Short): CreateTopicsRequest {
            if (data.validateOnly() && version.toInt() == 0) throw UnsupportedVersionException(
                "validateOnly is not supported in version 0 of CreateTopicsRequest"
            )

            val topicsWithDefaults = data.topics()
                .filter { topic -> topic.assignments().isEmpty() }
                .filter { topic ->
                    topic.numPartitions() == NO_NUM_PARTITIONS
                            || topic.replicationFactor() == NO_REPLICATION_FACTOR
                }
                .map { obj: CreatableTopic -> obj.name() }

            if (topicsWithDefaults.isNotEmpty() && version < 4) throw UnsupportedVersionException(
                "Creating topics with default partitions/replication factor are only " +
                        "supported in CreateTopicRequest version 4+. The following topics " +
                        "need values for partitions and replicas: " + topicsWithDefaults
            )

            return CreateTopicsRequest(data, version)
        }

        override fun toString(): String = data.toString()

        override fun equals(other: Any?): Boolean = other is Builder && (data == other.data)

        override fun hashCode(): Int = data.hashCode()
    }

    companion object {

        val NO_NUM_PARTITIONS = -1

        val NO_REPLICATION_FACTOR: Short = -1

        fun parse(buffer: ByteBuffer, version: Short): CreateTopicsRequest =
            CreateTopicsRequest(
                CreateTopicsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
