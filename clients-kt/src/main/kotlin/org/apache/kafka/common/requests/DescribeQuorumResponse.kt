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
import org.apache.kafka.common.message.DescribeQuorumResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

/**
 * Possible error codes.
 *
 * Top level errors:
 * - [Errors.CLUSTER_AUTHORIZATION_FAILED]
 * - [Errors.BROKER_NOT_AVAILABLE]
 *
 * Partition level errors:
 * - [Errors.NOT_LEADER_OR_FOLLOWER]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 */
class DescribeQuorumResponse(
    private val data: DescribeQuorumResponseData,
) : AbstractResponse(ApiKeys.DESCRIBE_QUORUM) {

    override fun errorCounts(): Map<Errors, Int> {
        val errors = mutableMapOf<Errors, Int>()
        errors[Errors.forCode(data.errorCode())] = 1
        for (topicResponse in data.topics())
            for (partitionResponse in topicResponse.partitions())
                updateErrorCounts(errors, Errors.forCode(partitionResponse.errorCode()))

        return errors
    }

    override fun data(): DescribeQuorumResponseData = data

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    companion object {

        fun singletonErrorResponse(
            topicPartition: TopicPartition,
            error: Errors
        ): DescribeQuorumResponseData = DescribeQuorumResponseData()
            .setTopics(
                listOf(
                    DescribeQuorumResponseData.TopicData()
                        .setTopicName(topicPartition.topic)
                        .setPartitions(
                            listOf(
                                DescribeQuorumResponseData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition)
                                    .setErrorCode(error.code)
                            )
                        )
                )
            )

        fun singletonResponse(
            topicPartition: TopicPartition,
            partitionData: DescribeQuorumResponseData.PartitionData
        ): DescribeQuorumResponseData = DescribeQuorumResponseData()
            .setTopics(
                listOf(
                    DescribeQuorumResponseData.TopicData()
                        .setTopicName(topicPartition.topic)
                        .setPartitions(
                            listOf(partitionData.setPartitionIndex(topicPartition.partition))
                        )
                )
            )

        fun parse(buffer: ByteBuffer, version: Short): DescribeQuorumResponse =
            DescribeQuorumResponse(
                DescribeQuorumResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
