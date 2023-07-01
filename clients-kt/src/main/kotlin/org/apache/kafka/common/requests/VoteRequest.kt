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

import java.nio.ByteBuffer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.VoteRequestData
import org.apache.kafka.common.message.VoteResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class VoteRequest private constructor(
    private val data: VoteRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.VOTE, version) {

    override fun data(): VoteRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        VoteResponse(VoteResponseData().setErrorCode(Errors.forException(e).code))

    class Builder(
        private val data: VoteRequestData,
    ) : AbstractRequest.Builder<VoteRequest>(ApiKeys.VOTE) {

        override fun build(version: Short): VoteRequest = VoteRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): VoteRequest =
            VoteRequest(VoteRequestData(ByteBufferAccessor(buffer), version), version)

        fun singletonRequest(
            topicPartition: TopicPartition,
            candidateEpoch: Int,
            candidateId: Int,
            lastEpoch: Int,
            lastEpochEndOffset: Long,
        ): VoteRequestData {
            return singletonRequest(
                topicPartition,
                null,
                candidateEpoch,
                candidateId,
                lastEpoch,
                lastEpochEndOffset
            )
        }

        fun singletonRequest(
            topicPartition: TopicPartition,
            clusterId: String?,
            candidateEpoch: Int,
            candidateId: Int,
            lastEpoch: Int,
            lastEpochEndOffset: Long,
        ): VoteRequestData {
            return VoteRequestData()
                .setClusterId(clusterId)
                .setTopics(
                    listOf(
                        VoteRequestData.TopicData()
                            .setTopicName(topicPartition.topic)
                            .setPartitions(
                                listOf(
                                    VoteRequestData.PartitionData()
                                        .setPartitionIndex(topicPartition.partition)
                                        .setCandidateEpoch(candidateEpoch)
                                        .setCandidateId(candidateId)
                                        .setLastOffsetEpoch(lastEpoch)
                                        .setLastOffset(lastEpochEndOffset)
                                )
                            )
                    )
                )
        }
    }
}
