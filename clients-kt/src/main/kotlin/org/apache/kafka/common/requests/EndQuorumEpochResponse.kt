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
import org.apache.kafka.common.message.EndQuorumEpochResponseData
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
 * - [Errors.FENCED_LEADER_EPOCH]
 * - [Errors.INVALID_REQUEST]
 * - [Errors.INCONSISTENT_VOTER_SET]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 */
class EndQuorumEpochResponse(
    private val data: EndQuorumEpochResponseData,
) : AbstractResponse(ApiKeys.END_QUORUM_EPOCH) {

    override fun errorCounts(): Map<Errors, Int> {
        val errors = mutableMapOf<Errors, Int>()
        errors[Errors.forCode(data.errorCode)] = 1

        for (topicResponse in data.topics)
            for (partitionResponse in topicResponse.partitions)
                updateErrorCounts(errors, Errors.forCode(partitionResponse.errorCode))

        return errors
    }

    override fun data(): EndQuorumEpochResponseData = data

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    companion object {

        fun singletonResponse(
            topLevelError: Errors,
            topicPartition: TopicPartition,
            partitionLevelError: Errors,
            leaderEpoch: Int,
            leaderId: Int
        ): EndQuorumEpochResponseData = EndQuorumEpochResponseData()
            .setErrorCode(topLevelError.code)
            .setTopics(
                listOf(
                    EndQuorumEpochResponseData.TopicData()
                        .setTopicName(topicPartition.topic)
                        .setPartitions(
                            listOf(
                                EndQuorumEpochResponseData.PartitionData()
                                    .setErrorCode(partitionLevelError.code)
                                    .setLeaderId(leaderId)
                                    .setLeaderEpoch(leaderEpoch)
                            )
                        )
                )
            )

        fun parse(buffer: ByteBuffer, version: Short): EndQuorumEpochResponse =
            EndQuorumEpochResponse(EndQuorumEpochResponseData(ByteBufferAccessor(buffer), version))
    }
}
