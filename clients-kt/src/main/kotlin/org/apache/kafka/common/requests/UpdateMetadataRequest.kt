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
import java.util.function.Function
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataTopicState
import org.apache.kafka.common.message.UpdateMetadataResponseData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.FlattenedIterator
import org.apache.kafka.common.utils.Utils.join

class UpdateMetadataRequest internal constructor(
    private val data: UpdateMetadataRequestData,
    version: Short,
) : AbstractControlRequest(ApiKeys.UPDATE_METADATA, version) {

    init {
        // Do this from the constructor to make it thread-safe (even though it's only needed when
        // some methods are called)
        normalize()
    }

    private fun normalize() {
        // Version 0 only supported a single host and port and the protocol was always plaintext
        // Version 1 added support for multiple endpoints, each with its own security protocol
        // Version 2 added support for rack
        // Version 3 added support for listener name, which we can infer from the security protocol for older versions
        if (version < 3) {
            for (liveBroker: UpdateMetadataBroker in data.liveBrokers) {
                // Set endpoints so that callers can rely on it always being present
                if (version.toInt() == 0 && liveBroker.endpoints.isEmpty()) {
                    val securityProtocol = SecurityProtocol.PLAINTEXT
                    liveBroker.setEndpoints(
                        listOf(
                            UpdateMetadataEndpoint()
                                .setHost(liveBroker.v0Host)
                                .setPort(liveBroker.v0Port)
                                .setSecurityProtocol(securityProtocol.id)
                                .setListener(
                                    ListenerName.forSecurityProtocol(securityProtocol).value
                                )
                        )
                    )
                } else for (endpoint: UpdateMetadataEndpoint in liveBroker.endpoints) {
                    // Set listener so that callers can rely on it always being present
                    if (endpoint.listener.isEmpty())
                        endpoint.setListener(listenerNameFromSecurityProtocol(endpoint))
                }
            }
        }
        if (version >= 5) {
            for (topicState: UpdateMetadataTopicState in data.topicStates) {
                for (partitionState: UpdateMetadataPartitionState in topicState.partitionStates) {
                    // Set the topic name so that we can always present the ungrouped view to callers
                    partitionState.setTopicName(topicState.topicName)
                }
            }
        }
    }

    override fun controllerId(): Int = data.controllerId

    override val isKRaftController: Boolean
        get() = data.isKRaftController

    override fun controllerEpoch(): Int = data.controllerEpoch

    override fun brokerEpoch(): Long = data.brokerEpoch

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): UpdateMetadataResponse {
        val data = UpdateMetadataResponseData().setErrorCode(Errors.forException(e).code)
        return UpdateMetadataResponse(data)
    }

    fun partitionStates(): Iterable<UpdateMetadataPartitionState> {
        return if (version >= 5) {
            Iterable {
                FlattenedIterator(data.topicStates.iterator()) { topicState ->
                    topicState.partitionStates.iterator()
                }
            }
        } else data.ungroupedPartitionStates
    }

    fun topicStates(): List<UpdateMetadataTopicState> {
        return if (version >= 5) data.topicStates
        else emptyList()
    }

    fun liveBrokers(): List<UpdateMetadataBroker> = data.liveBrokers

    override fun data(): UpdateMetadataRequestData = data

    class Builder(
        version: Short,
        controllerId: Int,
        controllerEpoch: Int,
        brokerEpoch: Long,
        private val partitionStates: List<UpdateMetadataPartitionState>,
        private val liveBrokers: List<UpdateMetadataBroker>,
        private val topicIds: Map<String, Uuid>,
        kraftController: Boolean = false,
    ) : AbstractControlRequest.Builder<UpdateMetadataRequest>(
        ApiKeys.UPDATE_METADATA,
        version,
        controllerId,
        controllerEpoch,
        brokerEpoch,
        kraftController
    ) {

        override fun build(version: Short): UpdateMetadataRequest {
            if (version < 3) {
                for (broker: UpdateMetadataBroker in liveBrokers) {
                    if (version.toInt() == 0) {
                        if (broker.endpoints.size != 1) throw UnsupportedVersionException(
                            "UpdateMetadataRequest v0 requires a single endpoint"
                        )

                        if (broker.endpoints[0].securityProtocol != SecurityProtocol.PLAINTEXT.id)
                            throw UnsupportedVersionException(
                                "UpdateMetadataRequest v0 only handles PLAINTEXT endpoints"
                            )

                        // Don't null out `endpoints` since it's ignored by the generated code if version >= 1
                        val endpoint = broker.endpoints[0]
                        broker.setV0Host(endpoint.host)
                        broker.setV0Port(endpoint.port)

                    } else if (broker.endpoints.any { endpoint ->
                            endpoint.listener.isNotEmpty()
                                    && endpoint.listener != listenerNameFromSecurityProtocol(
                                endpoint
                            )
                        }
                    ) throw UnsupportedVersionException(
                        "UpdateMetadataRequest v0-v3 does not support custom listeners, " +
                                "request version: $version, endpoints: ${broker.endpoints}"
                    )
                }
            }

            val data = UpdateMetadataRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveBrokers(liveBrokers)

            if (version >= 8) data.setIsKRaftController(kraftController)
            if (version >= 5) {
                val topicStatesMap = groupByTopic(topicIds, partitionStates)
                data.setTopicStates(topicStatesMap.values.toList())
            } else data.setUngroupedPartitionStates(partitionStates)

            return UpdateMetadataRequest(data, version)
        }

        override fun toString(): String {
            val bld = StringBuilder()
            bld.append("(type: UpdateMetadataRequest=")
                .append(", controllerId=")
                .append(controllerId)
                .append(", controllerEpoch=")
                .append(controllerEpoch)
                .append(", brokerEpoch=")
                .append(brokerEpoch)
                .append(", partitionStates=")
                .append(partitionStates)
                .append(", liveBrokers=")
                .append(liveBrokers.joinToString(", "))
                .append(")")

            return bld.toString()
        }

        companion object {
            private fun groupByTopic(
                topicIds: Map<String, Uuid>,
                partitionStates: List<UpdateMetadataPartitionState>,
            ): Map<String, UpdateMetadataTopicState> {
                val topicStates: MutableMap<String, UpdateMetadataTopicState> = HashMap()
                for (partition: UpdateMetadataPartitionState in partitionStates) {
                    // We don't null out the topic name in UpdateMetadataPartitionState since it's
                    // ignored by the generated code if version >= 5
                    val topicState = topicStates.computeIfAbsent(
                        partition.topicName
                    ) {
                        UpdateMetadataTopicState()
                            .setTopicName(partition.topicName)
                            .setTopicId(
                                topicIds.getOrDefault(partition.topicName, Uuid.ZERO_UUID)
                            )
                    }
                    topicState.partitionStates += partition
                }
                return topicStates
            }
        }
    }

    companion object {

        private fun listenerNameFromSecurityProtocol(endpoint: UpdateMetadataEndpoint): String {
            val securityProtocol = SecurityProtocol.forId(endpoint.securityProtocol)!!
            return ListenerName.forSecurityProtocol(securityProtocol).value
        }

        fun parse(buffer: ByteBuffer, version: Short): UpdateMetadataRequest {
            return UpdateMetadataRequest(
                UpdateMetadataRequestData(ByteBufferAccessor(buffer), version),
                version
            )
        }
    }
}
