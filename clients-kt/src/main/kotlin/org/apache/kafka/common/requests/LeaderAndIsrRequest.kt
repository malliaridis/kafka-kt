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

import org.apache.kafka.common.Node
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.FlattenedIterator
import java.nio.ByteBuffer

class LeaderAndIsrRequest internal constructor(
    private val data: LeaderAndIsrRequestData,
    version: Short,
) : AbstractControlRequest(ApiKeys.LEADER_AND_ISR, version) {

    init {
        // Do this from the constructor to make it thread-safe (even though it's only needed when
        // some methods are called)
        normalize()
    }

    private fun normalize() {
        if (version >= 2) for (topicState in data.topicStates)
            for (partitionState in topicState.partitionStates)
            // Set the topic name so that we can always present the ungrouped view to callers
                partitionState.setTopicName(topicState.topicName)
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): LeaderAndIsrResponse {
        val responseData = LeaderAndIsrResponseData()
        val error = Errors.forException(e)
        responseData.setErrorCode(error.code)
        if (version < 5) {
            val partitions = partitionStates().map { partition ->
                LeaderAndIsrPartitionError()
                    .setTopicName(partition.topicName)
                    .setPartitionIndex(partition.partitionIndex)
                    .setErrorCode(error.code)
            }
            responseData.setPartitionErrors(partitions)
        } else for (topicState in data.topicStates) {
            val partitions = topicState.partitionStates.map { partition ->
                LeaderAndIsrPartitionError()
                    .setPartitionIndex(partition.partitionIndex)
                    .setErrorCode(error.code)
            }

            responseData.topics.add(
                LeaderAndIsrTopicError()
                    .setTopicId(topicState.topicId)
                    .setPartitionErrors(partitions)
            )
        }

        return LeaderAndIsrResponse(responseData, version)
    }

    override fun controllerId(): Int = data.controllerId

    override val isKRaftController: Boolean
        get() = data.isKRaftController

    override fun controllerEpoch(): Int = data.controllerEpoch

    override fun brokerEpoch(): Long = data.brokerEpoch

    fun partitionStates(): Iterable<LeaderAndIsrPartitionState> {
        return if (version >= 2) Iterable {
            FlattenedIterator(data.topicStates.iterator()) { topicState ->
                topicState.partitionStates.iterator()
            }
        } else data.ungroupedPartitionStates
    }

    fun topicIds(): Map<String, Uuid> = data.topicStates.associateBy(
        keySelector = { obj: LeaderAndIsrTopicState -> obj.topicName },
        valueTransform = { obj: LeaderAndIsrTopicState -> obj.topicId },
    )

    fun liveLeaders(): List<LeaderAndIsrLiveLeader> = data.liveLeaders

    override fun data(): LeaderAndIsrRequestData = data

    class Builder(
        version: Short,
        controllerId: Int,
        controllerEpoch: Int,
        brokerEpoch: Long,
        private val partitionStates: List<LeaderAndIsrPartitionState>,
        private val topicIds: Map<String, Uuid>,
        private val liveLeaders: Collection<Node>,
        kraftController: Boolean = false,
    ) : AbstractControlRequest.Builder<LeaderAndIsrRequest>(
        api = ApiKeys.LEADER_AND_ISR,
        version = version,
        controllerId = controllerId,
        controllerEpoch = controllerEpoch,
        brokerEpoch = brokerEpoch,
        kraftController = kraftController,
    ) {

        override fun build(version: Short): LeaderAndIsrRequest {
            val leaders = liveLeaders.map { (id, host, port): Node ->
                LeaderAndIsrLiveLeader()
                    .setBrokerId(id)
                    .setHostName(host)
                    .setPort(port)
            }
            val data = LeaderAndIsrRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveLeaders(leaders)

            if (version >= 7) data.setIsKRaftController(kraftController)
            if (version >= 2) {
                val topicStatesMap = groupByTopic(partitionStates, topicIds)
                data.setTopicStates(topicStatesMap.values.toList())
            } else data.setUngroupedPartitionStates(partitionStates)

            return LeaderAndIsrRequest(data, version)
        }

        override fun toString(): String {
            return "(type=LeaderAndIsRequest" +
                    ", controllerId=$controllerId" +
                    ", controllerEpoch=$controllerEpoch" +
                    ", brokerEpoch=$brokerEpoch" +
                    ", partitionStates=$partitionStates" +
                    ", topicIds=$topicIds" +
                    ", liveLeaders=(${liveLeaders.joinToString(", ")})" +
                    ")"
        }

        companion object {

            private fun groupByTopic(
                partitionStates: List<LeaderAndIsrPartitionState>,
                topicIds: Map<String, Uuid>,
            ): Map<String, LeaderAndIsrTopicState> {
                val topicStates: MutableMap<String, LeaderAndIsrTopicState> = HashMap()
                // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
                // the generated code if version >= 2
                for (partition in partitionStates) {
                    val topicState = topicStates.computeIfAbsent(partition.topicName) {
                        LeaderAndIsrTopicState()
                            .setTopicName(partition.topicName)
                            .setTopicId(topicIds[partition.topicName] ?: Uuid.ZERO_UUID)
                    }
                    topicState.partitionStates += partition
                }
                return topicStates
            }
        }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): LeaderAndIsrRequest =
            LeaderAndIsrRequest(
                LeaderAndIsrRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
