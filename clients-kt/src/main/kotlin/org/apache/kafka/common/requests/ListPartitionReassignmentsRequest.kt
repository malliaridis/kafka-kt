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

import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer
import java.util.stream.Collectors

class ListPartitionReassignmentsRequest private constructor(
    private val data: ListPartitionReassignmentsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.LIST_PARTITION_REASSIGNMENTS, version) {

    override fun data(): ListPartitionReassignmentsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, message) = ApiError.fromThrowable(e)
        val ongoingTopicReassignments = data.topics().map { topic ->
            OngoingTopicReassignment()
                .setName(topic.name())
                .setPartitions(
                    topic.partitionIndexes().map { partitionIndex ->
                        OngoingPartitionReassignment().setPartitionIndex(partitionIndex)
                    }
                )
        }

        val responseData = ListPartitionReassignmentsResponseData()
            .setTopics(ongoingTopicReassignments)
            .setErrorCode(error.code)
            .setErrorMessage(message)
            .setThrottleTimeMs(throttleTimeMs)

        return ListPartitionReassignmentsResponse(responseData)
    }

    class Builder(
        private val data: ListPartitionReassignmentsRequestData,
    ) : AbstractRequest.Builder<ListPartitionReassignmentsRequest>(ApiKeys.LIST_PARTITION_REASSIGNMENTS) {

        override fun build(version: Short): ListPartitionReassignmentsRequest =
            ListPartitionReassignmentsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ListPartitionReassignmentsRequest =
            ListPartitionReassignmentsRequest(
                ListPartitionReassignmentsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
