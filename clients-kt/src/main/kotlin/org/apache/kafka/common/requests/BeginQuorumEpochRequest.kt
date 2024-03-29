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
import org.apache.kafka.common.message.BeginQuorumEpochRequestData
import org.apache.kafka.common.message.BeginQuorumEpochResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class BeginQuorumEpochRequest private constructor(
    private val data: BeginQuorumEpochRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.BEGIN_QUORUM_EPOCH, version) {

    override fun data(): BeginQuorumEpochRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): BeginQuorumEpochResponse =
        BeginQuorumEpochResponse(
            BeginQuorumEpochResponseData().setErrorCode(Errors.forException(e).code)
        )

    class Builder(
        private val data: BeginQuorumEpochRequestData,
    ) : AbstractRequest.Builder<BeginQuorumEpochRequest>(ApiKeys.BEGIN_QUORUM_EPOCH) {

        override fun build(version: Short): BeginQuorumEpochRequest =
            BeginQuorumEpochRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): BeginQuorumEpochRequest =
            BeginQuorumEpochRequest(
                BeginQuorumEpochRequestData(ByteBufferAccessor(buffer), version), version
            )

        @Deprecated("Use singletonRequest with default params instead.")
        fun singletonRequest(
            topicPartition: TopicPartition,
            leaderEpoch: Int,
            leaderId: Int
        ): BeginQuorumEpochRequestData =
            singletonRequest(
                topicPartition = topicPartition,
                clusterId = null,
                leaderEpoch = leaderEpoch,
                leaderId = leaderId
            )

        fun singletonRequest(
            topicPartition: TopicPartition,
            clusterId: String? = null,
            leaderEpoch: Int,
            leaderId: Int
        ): BeginQuorumEpochRequestData = BeginQuorumEpochRequestData()
            .setClusterId(clusterId)
            .setTopics(
                listOf(
                    BeginQuorumEpochRequestData.TopicData()
                        .setTopicName(topicPartition.topic)
                        .setPartitions(
                            listOf(
                                BeginQuorumEpochRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition)
                                    .setLeaderEpoch(leaderEpoch)
                                    .setLeaderId(leaderId)
                            )
                        )
                )
            )
    }
}
