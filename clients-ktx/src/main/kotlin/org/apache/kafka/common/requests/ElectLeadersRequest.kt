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

import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.ElectLeadersRequestData
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.MessageUtil.deepToString
import java.nio.ByteBuffer
import java.util.function.Consumer

class ElectLeadersRequest private constructor(
    private val data: ElectLeadersRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ELECT_LEADERS, version) {

    override fun data(): ElectLeadersRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, message) = ApiError.fromThrowable(e)

        val electionResults = data.topicPartitions()?.map { topic ->
            val electionResult = ReplicaElectionResult()
            electionResult.setTopic(topic.topic())

            for (partitionId in topic.partitions()) {
                val partitionResult = PartitionResult()
                partitionResult.setPartitionId(partitionId!!)
                partitionResult.setErrorCode(error.code)
                partitionResult.setErrorMessage(message)
                electionResult.partitionResult().add(partitionResult)
            }
            electionResult
        } ?: emptyList()

        return ElectLeadersResponse(
            throttleTimeMs = throttleTimeMs,
            errorCode = error.code,
            electionResults = electionResults,
            version = version,
        )
    }

    class Builder(
        private val electionType: ElectionType,
        private val topicPartitions: Collection<TopicPartition>?,
        private val timeoutMs: Int,
    ) : AbstractRequest.Builder<ElectLeadersRequest>(ApiKeys.ELECT_LEADERS) {

        override fun build(version: Short): ElectLeadersRequest =
            ElectLeadersRequest(toRequestData(version), version)

        override fun toString(): String {
            return "ElectLeadersRequest(" +
                    "electionType=$electionType" +
                    ", topicPartitions=" +
                    (topicPartitions?.let { deepToString(topicPartitions.iterator()) } ?: "null") +
                    ", timeoutMs=$timeoutMs" +
                    ")"
        }

        private fun toRequestData(version: Short): ElectLeadersRequestData {
            if (electionType !== ElectionType.PREFERRED && version.toInt() == 0)
                throw UnsupportedVersionException(
                    "API Version 0 only supports PREFERRED election type"
                )

            val data = ElectLeadersRequestData().setTimeoutMs(timeoutMs)

            topicPartitions?.forEach { (topic, partition): TopicPartition ->
                var tps = data.topicPartitions().find(topic)
                if (tps == null) {
                    tps = TopicPartitions().setTopic(topic)
                    data.topicPartitions().add(tps)
                }
                tps.partitions().add(partition)
            } ?: data.setTopicPartitions(null)

            data.setElectionType(electionType.value)
            return data
        }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ElectLeadersRequest =
            ElectLeadersRequest(
                ElectLeadersRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
