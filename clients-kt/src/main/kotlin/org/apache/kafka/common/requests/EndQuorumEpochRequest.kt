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
import org.apache.kafka.common.message.EndQuorumEpochRequestData
import org.apache.kafka.common.message.EndQuorumEpochResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class EndQuorumEpochRequest private constructor(
    private val data: EndQuorumEpochRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.END_QUORUM_EPOCH, version) {

    override fun data(): EndQuorumEpochRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): EndQuorumEpochResponse =
        EndQuorumEpochResponse(
            EndQuorumEpochResponseData().setErrorCode(Errors.forException(e).code)
        )

    class Builder(
        private val data: EndQuorumEpochRequestData,
    ) : AbstractRequest.Builder<EndQuorumEpochRequest>(ApiKeys.END_QUORUM_EPOCH) {

        override fun build(version: Short) =  EndQuorumEpochRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): EndQuorumEpochRequest =
            EndQuorumEpochRequest(
                EndQuorumEpochRequestData(ByteBufferAccessor(buffer), version),
                version,
            )

        fun singletonRequest(
            topicPartition: TopicPartition,
            clusterId: String? = null,
            leaderEpoch: Int,
            leaderId: Int,
            preferredSuccessors: List<Int>,
        ): EndQuorumEpochRequestData = EndQuorumEpochRequestData()
            .setClusterId(clusterId)
            .setTopics(
                listOf(
                    EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topicPartition.topic)
                        .setPartitions(
                            listOf(
                                EndQuorumEpochRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition)
                                    .setLeaderEpoch(leaderEpoch)
                                    .setLeaderId(leaderId)
                                    .setPreferredSuccessors(preferredSuccessors)
                            )
                        )
                )
            )
    }
}
