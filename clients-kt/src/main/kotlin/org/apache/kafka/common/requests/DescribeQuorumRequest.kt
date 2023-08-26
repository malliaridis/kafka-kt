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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DescribeQuorumRequestData
import org.apache.kafka.common.message.DescribeQuorumResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DescribeQuorumRequest private constructor(
    private val data: DescribeQuorumRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_QUORUM, version) {

    override fun data(): DescribeQuorumRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        DescribeQuorumResponse(getTopLevelErrorResponse(Errors.forException(e)))

    class Builder(
        private val data: DescribeQuorumRequestData,
    ) : AbstractRequest.Builder<DescribeQuorumRequest>(ApiKeys.DESCRIBE_QUORUM) {

        override fun build(version: Short): DescribeQuorumRequest =
            DescribeQuorumRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeQuorumRequest =
            DescribeQuorumRequest(
                DescribeQuorumRequestData(ByteBufferAccessor(buffer), version),
                version
            )

        fun singletonRequest(topicPartition: TopicPartition): DescribeQuorumRequestData =
            DescribeQuorumRequestData().setTopics(
                listOf(
                    DescribeQuorumRequestData.TopicData()
                        .setTopicName(topicPartition.topic)
                        .setPartitions(
                            listOf(
                                DescribeQuorumRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition)
                            )
                        )
                )
            )

        fun getPartitionLevelErrorResponse(
            data: DescribeQuorumRequestData,
            error: Errors
        ): DescribeQuorumResponseData {
            val errorCode = error.code
            val topicResponses = data.topics.map { topic ->
                DescribeQuorumResponseData.TopicData()
                    .setTopicName(topic.topicName)
                    .setPartitions(
                        topic.partitions.map { requestPartition ->
                            DescribeQuorumResponseData.PartitionData()
                                .setPartitionIndex(requestPartition.partitionIndex)
                                .setErrorCode(errorCode)
                        }
                    )
            }

            return DescribeQuorumResponseData().setTopics(topicResponses)
        }

        fun getTopLevelErrorResponse(topLevelError: Errors): DescribeQuorumResponseData =
            DescribeQuorumResponseData().setErrorCode(topLevelError.code)
    }
}
