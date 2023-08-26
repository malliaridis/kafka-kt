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

import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer
import java.util.stream.Collectors

class AlterPartitionReassignmentsRequest private constructor(
    private val data: AlterPartitionReassignmentsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ALTER_PARTITION_REASSIGNMENTS, version) {

    override fun data(): AlterPartitionReassignmentsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, message) = ApiError.fromThrowable(e)
        val topicResponses = data.topics.map { topic ->
            val partitionResponses = topic.partitions.map { partition ->
                ReassignablePartitionResponse()
                    .setPartitionIndex(partition.partitionIndex)
                    .setErrorCode(error.code)
                    .setErrorMessage(message)
            }

            ReassignableTopicResponse()
                .setName(topic.name)
                .setPartitions(partitionResponses)
        }

        val responseData = AlterPartitionReassignmentsResponseData()
            .setResponses(topicResponses)
            .setErrorCode(error.code)
            .setErrorMessage(message)
            .setThrottleTimeMs(throttleTimeMs)

        return AlterPartitionReassignmentsResponse(responseData)
    }

    class Builder(
        private val data: AlterPartitionReassignmentsRequestData,
    ) : AbstractRequest.Builder<AlterPartitionReassignmentsRequest>(
        ApiKeys.ALTER_PARTITION_REASSIGNMENTS,
    ) {

        override fun build(version: Short): AlterPartitionReassignmentsRequest =
            AlterPartitionReassignmentsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterPartitionReassignmentsRequest =
            AlterPartitionReassignmentsRequest(
                AlterPartitionReassignmentsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
