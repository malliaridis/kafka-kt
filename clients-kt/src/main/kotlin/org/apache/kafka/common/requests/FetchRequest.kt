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

import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.FetchRequestData
import org.apache.kafka.common.message.FetchRequestData.FetchPartition
import org.apache.kafka.common.message.FetchRequestData.FetchTopic
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import java.nio.ByteBuffer
import java.util.function.Consumer

class FetchRequest(
    private val data: FetchRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.FETCH, version) {

    @Volatile
    private var fetchData: LinkedHashMap<TopicIdPartition, PartitionData>? = null

    @Volatile
    private var toForget: List<TopicIdPartition>? = null

    // This is an immutable read-only structures derived from FetchRequestData
    private val metadata: FetchMetadata = FetchMetadata(data.sessionId(), data.sessionEpoch())

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        // For versions 13+ the error is indicated by setting the top-level error code, and no
        // partitions will be returned. For earlier versions, the error is indicated in two ways:
        // by setting the same error code in all partitions, and by setting the top-level error
        // code. The form where we set the same error code in all partitions is needed in order to
        // maintain backwards compatibility with older versions of the protocol in which there was
        // no top-level error code. Note that for incremental fetch responses, there may not be any
        // partitions at all in the response. For this reason, the top-level error code is
        // essential for them.
        val error = Errors.forException(e)


        // For version 13+, we know the client can handle a top level error code, so we don't need
        // to send back partitions too.
        val topicResponseList = if (version < 13) {
            data.topics().map { topic: FetchTopic ->
                val partitionResponses = topic.partitions().map { partition ->
                    FetchResponse.partitionResponse(partition.partition(), error)
                }

                FetchableTopicResponse()
                    .setTopic(topic.topic())
                    .setTopicId(topic.topicId())
                    .setPartitions(partitionResponses)
            }
        } else emptyList()

        return FetchResponse(
            FetchResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code)
                .setSessionId(data.sessionId())
                .setResponses(topicResponseList)
        )
    }

    fun replicaId(): Int = data.replicaId()

    fun maxWait(): Int = data.maxWaitMs()

    fun minBytes(): Int = data.minBytes()

    fun maxBytes(): Int = data.maxBytes()

    // For versions < 13, builds the partitionData map using only the FetchRequestData.
    // For versions 13+, builds the partitionData map using both the FetchRequestData and a mapping
    // of topic IDs to names.
    fun fetchData(topicNames: Map<Uuid, String>): Map<TopicIdPartition, PartitionData>? {
        if (fetchData == null) {
            synchronized(this) {
                if (fetchData == null) {
                    // Assigning the lazy-initialized `fetchData` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    val fetchDataTmp = LinkedHashMap<TopicIdPartition, PartitionData>()

                    data.topics().forEach { fetchTopic ->
                        val name: String = if (version < 13) fetchTopic.topic() // can't be null
                        else topicNames[fetchTopic.topicId()]!!

                        fetchTopic.partitions().forEach { fetchPartition ->
                            // Topic name may be null here if the topic name was unable to be
                            // resolved using the topicNames map.
                            fetchDataTmp[
                                TopicIdPartition(
                                    topicId = fetchTopic.topicId(),
                                    topicPartition = TopicPartition(
                                        name,
                                        fetchPartition.partition()
                                    ),
                                )
                            ] = PartitionData(
                                fetchTopic.topicId(),
                                fetchPartition.fetchOffset(),
                                fetchPartition.logStartOffset(),
                                fetchPartition.partitionMaxBytes(),
                                optionalEpoch(fetchPartition.currentLeaderEpoch()),
                                optionalEpoch(fetchPartition.lastFetchedEpoch())
                            )
                        }
                    }
                    fetchData = fetchDataTmp
                }
            }
        }
        return fetchData
    }

    // For versions < 13, builds the forgotten topics list using only the FetchRequestData.
    // For versions 13+, builds the forgotten topics list using both the FetchRequestData and a
    // mapping of topic IDs to names.
    fun forgottenTopics(topicNames: Map<Uuid?, String?>): List<TopicIdPartition>? {
        if (toForget == null) {
            synchronized(this) {
                if (toForget == null) {
                    // Assigning the lazy-initialized `toForget` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    val toForgetTmp: MutableList<TopicIdPartition> =
                        ArrayList()

                    data.forgottenTopicsData().forEach { forgottenTopic ->
                        val name: String =
                            if (version < 13) forgottenTopic.topic() // can't be null
                            else topicNames[forgottenTopic.topicId()]!!

                        // Topic name may be null here if the topic name was unable to be
                        // resolved using the topicNames map.
                        forgottenTopic.partitions().forEach { partitionId ->
                            toForgetTmp.add(
                                TopicIdPartition(
                                    forgottenTopic.topicId(),
                                    TopicPartition(name, partitionId)
                                )
                            )
                        }
                    }
                    toForget = toForgetTmp
                }
            }
        }
        return toForget
    }

    val isFromFollower: Boolean
        get() = replicaId() >= 0

    fun isolationLevel(): IsolationLevel = IsolationLevel.forId(data.isolationLevel())

    fun metadata(): FetchMetadata = metadata

    fun rackId(): String = data.rackId()

    override fun data(): FetchRequestData = data

    class Builder(
        minVersion: Short,
        maxVersion: Short,
        private val replicaId: Int,
        private val maxWait: Int,
        private val minBytes: Int,
        private val toFetch: Map<TopicPartition, PartitionData>,
    ) : AbstractRequest.Builder<FetchRequest>(ApiKeys.FETCH, minVersion, maxVersion) {

        private var isolationLevel = IsolationLevel.READ_UNCOMMITTED

        private var maxBytes = DEFAULT_RESPONSE_MAX_BYTES

        private var metadata = FetchMetadata.LEGACY

        private var removed = emptyList<TopicIdPartition>()

        private var replaced = emptyList<TopicIdPartition>()

        private var rackId = ""

        fun isolationLevel(isolationLevel: IsolationLevel): Builder {
            this.isolationLevel = isolationLevel
            return this
        }

        fun metadata(metadata: FetchMetadata): Builder {
            this.metadata = metadata
            return this
        }

        fun rackId(rackId: String): Builder {
            this.rackId = rackId
            return this
        }

        fun fetchData(): Map<TopicPartition, PartitionData> = toFetch

        fun setMaxBytes(maxBytes: Int): Builder {
            this.maxBytes = maxBytes
            return this
        }

        fun removed(): List<TopicIdPartition> = removed

        fun removed(removed: List<TopicIdPartition>): Builder {
            this.removed = removed
            return this
        }

        fun replaced(): List<TopicIdPartition> = replaced

        fun replaced(replaced: List<TopicIdPartition>): Builder {
            this.replaced = replaced
            return this
        }

        private fun addToForgottenTopicMap(
            toForget: List<TopicIdPartition>,
            forgottenTopicMap: MutableMap<String, ForgottenTopic>
        ) {
            toForget.forEach(Consumer { topicIdPartition ->
                val forgottenTopic = forgottenTopicMap.computeIfAbsent(topicIdPartition.topic) {
                    ForgottenTopic()
                        .setTopic(topicIdPartition.topic)
                        .setTopicId(topicIdPartition.topicId)
                }

                forgottenTopic.partitions().add(topicIdPartition.partition)
            })
        }

        override fun build(version: Short): FetchRequest {
            if (version < 3) maxBytes = DEFAULT_RESPONSE_MAX_BYTES

            val fetchRequestData = FetchRequestData()
                .setReplicaId(replicaId)
                .setMaxWaitMs(maxWait)
                .setMinBytes(minBytes)
                .setMaxBytes(maxBytes)
                .setIsolationLevel(isolationLevel.id())
                .setForgottenTopicsData(ArrayList())

            val forgottenTopicMap = mutableMapOf<String, ForgottenTopic>()
            addToForgottenTopicMap(removed, forgottenTopicMap)

            // If a version older than v13 is used, topic-partition which were replaced by a
            // topic-partition with the same name but a different topic ID are not sent out in the
            // "forget" set in order to not remove the newly added partition in the "fetch" set.
            if (version >= 13) addToForgottenTopicMap(replaced, forgottenTopicMap)

            fetchRequestData.forgottenTopicsData().addAll(forgottenTopicMap.values)

            // We collect the partitions in a single FetchTopic only if they appear sequentially in
            // the fetchData
            fetchRequestData.setTopics(emptyList())
            var fetchTopic: FetchTopic? = null

            for ((topicPartition, partitionData) in toFetch) {
                if (fetchTopic == null || topicPartition.topic != fetchTopic.topic()) {
                    fetchTopic = FetchTopic()
                        .setTopic(topicPartition.topic)
                        .setTopicId(partitionData.topicId)
                        .setPartitions(ArrayList())
                    fetchRequestData.topics().add(fetchTopic)
                }
                val fetchPartition = FetchPartition()
                    .setPartition(topicPartition.partition)
                    .setCurrentLeaderEpoch(
                        partitionData.currentLeaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                    )
                    .setLastFetchedEpoch(
                        partitionData.lastFetchedEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                    )
                    .setFetchOffset(partitionData.fetchOffset)
                    .setLogStartOffset(partitionData.logStartOffset)
                    .setPartitionMaxBytes(partitionData.maxBytes)

                fetchTopic!!.partitions().add(fetchPartition)
            }
            if (metadata != null) {
                fetchRequestData.setSessionEpoch(metadata!!.epoch())
                fetchRequestData.setSessionId(metadata!!.sessionId())
            }

            fetchRequestData.setRackId(rackId)
            return FetchRequest(fetchRequestData, version)
        }

        override fun toString(): String {
            return "(type=FetchRequest" +
                    ", replicaId=$replicaId" +
                    ", maxWait=$maxWait" +
                    ", minBytes=$minBytes" +
                    ", maxBytes=$maxBytes" +
                    ", fetchData=$toFetch" +
                    ", isolationLevel=$isolationLevel" +
                    ", removed=${removed.joinToString(", ")}" +
                    ", replaced=${replaced.joinToString(", ")}" +
                    ", metadata=${metadata}" +
                    ", rackId=${rackId}" +
                    ")"
        }

        companion object {
            fun forConsumer(
                maxVersion: Short,
                maxWait: Int,
                minBytes: Int,
                fetchData: Map<TopicPartition, PartitionData>
            ): Builder = Builder(
                minVersion = ApiKeys.FETCH.oldestVersion(),
                maxVersion = maxVersion,
                replicaId = CONSUMER_REPLICA_ID,
                maxWait = maxWait,
                minBytes = minBytes,
                toFetch = fetchData,
            )

            fun forReplica(
                allowedVersion: Short,
                replicaId: Int,
                maxWait: Int,
                minBytes: Int,
                fetchData: Map<TopicPartition, PartitionData>
            ): Builder = Builder(
                minVersion = allowedVersion,
                maxVersion = allowedVersion,
                replicaId = replicaId,
                maxWait = maxWait,
                minBytes = minBytes,
                toFetch = fetchData
            )
        }
    }

    data class PartitionData(
        val topicId: Uuid,
        val fetchOffset: Long,
        val logStartOffset: Long,
        val maxBytes: Int,
        val currentLeaderEpoch: Int?,
        val lastFetchedEpoch: Int? = null
    ) {

        override fun toString(): String = "PartitionData(" +
                "topicId=$topicId" +
                ", fetchOffset=$fetchOffset" +
                ", logStartOffset=$logStartOffset" +
                ", maxBytes=$maxBytes" +
                ", currentLeaderEpoch=$currentLeaderEpoch" +
                ", lastFetchedEpoch=$lastFetchedEpoch" +
                ')'
    }

    companion object {

        const val CONSUMER_REPLICA_ID = -1

        // default values for older versions where a request level limit did not exist
        const val DEFAULT_RESPONSE_MAX_BYTES = Int.MAX_VALUE

        const val INVALID_LOG_START_OFFSET = -1L

        private fun optionalEpoch(rawEpochValue: Int): Int? =
            if (rawEpochValue < 0) null
            else rawEpochValue

        fun parse(buffer: ByteBuffer, version: Short): FetchRequest =
            FetchRequest(
                FetchRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
