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
import java.util.function.Consumer
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class OffsetsForLeaderEpochRequest(
    private val data: OffsetForLeaderEpochRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH, version) {

    override fun data(): OffsetForLeaderEpochRequestData = data

    fun replicaId(): Int = data.replicaId()

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val error = Errors.forException(e)
        val responseData = OffsetForLeaderEpochResponseData()

        data.topics().forEach { topic: OffsetForLeaderTopic ->
            val topicData = OffsetForLeaderTopicResult().setTopic(topic.topic())

            topic.partitions()
                .forEach { partition: OffsetForLeaderPartition ->
                    topicData.partitions().add(
                        OffsetForLeaderEpochResponseData.EpochEndOffset()
                            .setPartition(partition.partition())
                            .setErrorCode(error.code)
                            .setLeaderEpoch(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH)
                            .setEndOffset(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET)
                    )
                }
            responseData.topics().add(topicData)
        }
        return OffsetsForLeaderEpochResponse(responseData)
    }

    class Builder internal constructor(
        oldestAllowedVersion: Short,
        latestAllowedVersion: Short,
        private val data: OffsetForLeaderEpochRequestData,
    ) : AbstractRequest.Builder<OffsetsForLeaderEpochRequest>(
        ApiKeys.OFFSET_FOR_LEADER_EPOCH,
        oldestAllowedVersion,
        latestAllowedVersion
    ) {

        override fun build(version: Short): OffsetsForLeaderEpochRequest {
            if (version < oldestAllowedVersion || version > latestAllowedVersion)
                throw UnsupportedVersionException("Cannot build $this with version $version")

            return OffsetsForLeaderEpochRequest(data, version)
        }

        override fun toString(): String = data.toString()

        companion object {

            fun forConsumer(epochsByPartition: OffsetForLeaderTopicCollection?): Builder {
                // Old versions of this API require CLUSTER permission which is not typically
                // granted to clients. Beginning with version 3, the broker requires only TOPIC
                // Describe permission for the topic of each requested partition. In order to ensure
                // client compatibility, we only send this request when we can guarantee the relaxed
                // permissions.
                val data = OffsetForLeaderEpochRequestData()
                data.setReplicaId(CONSUMER_REPLICA_ID)
                data.setTopics(epochsByPartition)
                return Builder(3.toShort(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), data)
            }

            fun forFollower(
                version: Short,
                epochsByPartition: OffsetForLeaderTopicCollection?,
                replicaId: Int,
            ): Builder {
                val data = OffsetForLeaderEpochRequestData()
                data.setReplicaId(replicaId)
                data.setTopics(epochsByPartition)
                return Builder(version, version, data)
            }
        }
    }

    companion object {

        /**
         * Sentinel replica_id value to indicate a regular consumer rather than another broker
         */
        const val CONSUMER_REPLICA_ID = -1

        /**
         * Sentinel replica_id which indicates either a debug consumer or a replica which is using
         * an old version of the protocol.
         */
        const val DEBUGGING_REPLICA_ID = -2

        fun parse(buffer: ByteBuffer, version: Short): OffsetsForLeaderEpochRequest =
            OffsetsForLeaderEpochRequest(
                OffsetForLeaderEpochRequestData(ByteBufferAccessor(buffer), version),
                version
            )

        /**
         * Check whether a broker allows Topic-level permissions in order to use the
         * OffsetForLeaderEpoch API. Old versions require Cluster permission.
         */
        fun supportsTopicPermission(latestUsableVersion: Short): Boolean = latestUsableVersion >= 3
    }
}
