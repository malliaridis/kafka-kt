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
import java.util.stream.Collectors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.StopReplicaRequestData
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionV0
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicV1
import org.apache.kafka.common.message.StopReplicaResponseData
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.MappedIterator
import org.apache.kafka.common.utils.Utils.join

class StopReplicaRequest private constructor(
    private val data: StopReplicaRequestData,
    version: Short,
) : AbstractControlRequest(ApiKeys.STOP_REPLICA, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): StopReplicaResponse {
        val error = Errors.forException(e)
        val data = StopReplicaResponseData()
        data.setErrorCode(error.code)

        val partitions = mutableListOf<StopReplicaPartitionError>()
        for (topic in topicStates())
            for (partition in topic.partitionStates)
                partitions.add(
                    StopReplicaPartitionError()
                        .setTopicName(topic.topicName)
                        .setPartitionIndex(partition.partitionIndex)
                        .setErrorCode(error.code)
                )

        data.setPartitionErrors(partitions)
        return StopReplicaResponse(data)
    }

    /**
     * Note that this method has allocation overhead per iterated element, so callers should copy
     * the result into another collection if they need to iterate more than once.
     *
     * Implementation note: we should strive to avoid allocation overhead per element, see
     * `UpdateMetadataRequest.partitionStates()` for the preferred approach. That's not possible in
     * this case and StopReplicaRequest should be relatively rare in comparison to other request
     * types.
     */
    fun topicStates(): Iterable<StopReplicaTopicState> {
        return if (version < 1) {
            val topicStates = mutableMapOf<String, StopReplicaTopicState>()

            for (partition in data.ungroupedPartitions) {
                val topicState = topicStates.computeIfAbsent(partition.topicName) { topic ->
                    StopReplicaTopicState().setTopicName(topic)
                }

                topicState.partitionStates += StopReplicaPartitionState()
                    .setPartitionIndex(partition.partitionIndex)
                    .setDeletePartition(data.deletePartitions)
            }
            topicStates.values

        } else if (version < 3) Iterable {
            MappedIterator(data.topics.iterator()) { topic ->
                StopReplicaTopicState()
                    .setTopicName(topic.name)
                    .setPartitionStates(
                        topic.partitionIndexes.map { partition ->
                            StopReplicaPartitionState()
                                .setPartitionIndex(partition)
                                .setDeletePartition(data.deletePartitions)
                        }
                    )
            }
        } else data.topicStates
    }

    fun partitionStates(): Map<TopicPartition, StopReplicaPartitionState> {
        val partitionStates = mutableMapOf<TopicPartition, StopReplicaPartitionState>()

        if (version < 1) for (partition in data.ungroupedPartitions)
            partitionStates[TopicPartition(partition.topicName, partition.partitionIndex)] =
                StopReplicaPartitionState()
                    .setPartitionIndex(partition.partitionIndex)
                    .setDeletePartition(data.deletePartitions)
        else if (version < 3) for (topic in data.topics)
            for (partitionIndex in topic.partitionIndexes)
                partitionStates[TopicPartition(topic.name, partitionIndex)] =
                    StopReplicaPartitionState()
                        .setPartitionIndex(partitionIndex)
                        .setDeletePartition(data.deletePartitions)
        else for (topicState in data.topicStates)
            for (partitionState in topicState.partitionStates)
                partitionStates[TopicPartition(
                    topicState.topicName,
                    partitionState.partitionIndex
                )] = partitionState

        return partitionStates
    }

    override fun controllerId(): Int = data.controllerId

    override val isKRaftController: Boolean
        get() = data.isKRaftController

    override fun controllerEpoch(): Int = data.controllerEpoch

    override fun brokerEpoch(): Long = data.brokerEpoch

    override fun data(): StopReplicaRequestData = data

    class Builder(
        version: Short,
        controllerId: Int,
        controllerEpoch: Int,
        brokerEpoch: Long,
        private val deletePartitions: Boolean,
        private val topicStates: List<StopReplicaTopicState>,
        kraftController: Boolean = false,
    ) : AbstractControlRequest.Builder<StopReplicaRequest>(
        ApiKeys.STOP_REPLICA,
        version,
        controllerId,
        controllerEpoch,
        brokerEpoch,
        kraftController
    ) {

        override fun build(version: Short): StopReplicaRequest {
            val data = StopReplicaRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)

            if (version >= 4) data.setIsKRaftController(kraftController)
            if (version >= 3) data.setTopicStates(topicStates)
            else if (version >= 1) {
                data.setDeletePartitions(deletePartitions)
                val topics = topicStates.map { topic: StopReplicaTopicState ->
                    StopReplicaTopicV1()
                        .setName(topic.topicName)
                        .setPartitionIndexes(
                            topic.partitionStates.map { it.partitionIndex }.toIntArray()
                        )
                }

                data.setTopics(topics)
            } else {
                data.setDeletePartitions(deletePartitions)
                val partitions = topicStates.flatMap { topic ->
                    topic.partitionStates.map { partition ->
                        StopReplicaPartitionV0()
                            .setTopicName(topic.topicName)
                            .setPartitionIndex(partition.partitionIndex)
                    }
                }

                data.setUngroupedPartitions(partitions)
            }
            return StopReplicaRequest(data, version)
        }

        override fun toString(): String {
            return "(type=StopReplicaRequest" +
                    ", controllerId=$controllerId" +
                    ", controllerEpoch=$controllerEpoch" +
                    ", brokerEpoch=$brokerEpoch" +
                    ", deletePartitions=$deletePartitions" +
                    ", topicStates=${topicStates.joinToString(",")}" +
                    ")"
        }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): StopReplicaRequest =
            StopReplicaRequest(
                StopReplicaRequestData(ByteBufferAccessor(buffer), version),
                version
            )
    }
}
