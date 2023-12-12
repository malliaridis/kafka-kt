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
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.Records

/**
 * This wrapper supports all versions of the Fetch API
 *
 * Possible error codes:
 *
 * - [Errors.OFFSET_OUT_OF_RANGE] If the fetch offset is out of range for a requested partition
 * - [Errors.TOPIC_AUTHORIZATION_FAILED] If the user does not have READ access to a requested topic
 * - [Errors.REPLICA_NOT_AVAILABLE] If the request is received by a broker with version < 2.6 which
 *   is not a replica
 * - [Errors.NOT_LEADER_OR_FOLLOWER] If the broker is not a leader or follower and either the
 *   provided leader epoch matches the known leader epoch on the broker or is empty
 * - [Errors.FENCED_LEADER_EPOCH] If the epoch is lower than the broker's epoch
 * - [Errors.UNKNOWN_LEADER_EPOCH] If the epoch is larger than the broker's epoch
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION] If the broker does not have metadata for a topic or
 *   partition
 * - [Errors.KAFKA_STORAGE_ERROR] If the log directory for one of the requested partitions is
 *   offline
 * - [Errors.UNSUPPORTED_COMPRESSION_TYPE] If a fetched topic is using a compression type which is
 *   not supported by the fetch request version
 * - [Errors.CORRUPT_MESSAGE] If corrupt message encountered, e.g. when the broker scans the log to
 *   find the fetch offset after the index lookup
 * - [Errors.UNKNOWN_TOPIC_ID] If the request contains a topic ID unknown to the broker
 * - [Errors.FETCH_SESSION_TOPIC_ID_ERROR] If the request version supports topic IDs but the session
 *   does not or vice versa, or a topic ID in the request is inconsistent with a topic ID in the
 *   session
 * - [Errors.INCONSISTENT_TOPIC_ID] If a topic ID in the session does not match the topic ID in the
 *   log
 * - [Errors.UNKNOWN_SERVER_ERROR] For any unexpected errors
 *
 * @constructor From version 3 or later, the authorized and existing entries in
 * `FetchRequest.fetchData` should be in the same order in `responseData`. Version 13 introduces
 * topic IDs which can lead to a few new errors. If there is any unknown topic ID in the request,
 * the response will contain a partition-level UNKNOWN_TOPIC_ID error for that partition. If a
 * request's topic ID usage is inconsistent with the session, we will return a top level
 * FETCH_SESSION_TOPIC_ID_ERROR error. We may also return INCONSISTENT_TOPIC_ID error as a
 * partition-level error when a partition in the session has a topic ID inconsistent with the log.
 */
class FetchResponse(private val data: FetchResponseData) : AbstractResponse(ApiKeys.FETCH) {

    // we build responseData when needed.
    @Volatile
    private var responseData: Map<TopicPartition, FetchResponseData.PartitionData>? = null

    override fun data(): FetchResponseData = data

    fun error(): Errors = Errors.forCode(data.errorCode)

    fun responseData(
        topicNames: Map<Uuid, String>,
        version: Short,
    ): Map<TopicPartition, FetchResponseData.PartitionData> {
        if (responseData == null) {
            synchronized(this) {
                if (responseData == null) {
                    // Assigning the lazy-initialized `responseData` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    val responseDataTmp =
                        LinkedHashMap<TopicPartition, FetchResponseData.PartitionData>()

                    data.responses.forEach { topicResponse ->
                        val name: String? = if (version < 13) topicResponse.topic
                        else topicNames[topicResponse.topicId]

                        if (name != null) topicResponse.partitions.forEach { partition ->
                            responseDataTmp[TopicPartition(name, partition.partitionIndex)] =
                                partition
                        }
                    }
                    responseData = responseDataTmp
                }
            }
        }
        return responseData!!
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun sessionId(): Int = data.sessionId

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()
        updateErrorCounts(errorCounts, error())

        data.responses.forEach { topicResponse ->
            topicResponse.partitions.forEach { partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode))
            }
        }

        return errorCounts
    }

    // Fetch versions 13 and above should have topic IDs for all topics.
    // Fetch versions < 13 should return the empty set.
    fun topicIds(): Set<Uuid> = data.responses
        .map { it.topicId }
        .filter { id -> id != Uuid.ZERO_UUID }
        .toSet()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 8

    @Suppress("TooManyFunctions")
    companion object {

        const val INVALID_HIGH_WATERMARK = -1L

        const val INVALID_LAST_STABLE_OFFSET = -1L

        const val INVALID_LOG_START_OFFSET = -1L

        const val INVALID_PREFERRED_REPLICA_ID = -1

        fun parse(buffer: ByteBuffer, version: Short): FetchResponse =
            FetchResponse(FetchResponseData(ByteBufferAccessor(buffer), version))

        /**
         * Convenience method to find the size of a response.
         *
         * @param version The version of the response to use.
         * @param partIterator The partition iterator.
         * @return The response size in bytes.
         */
        fun sizeOf(
            version: Short,
            partIterator: Iterator<Map.Entry<TopicIdPartition, FetchResponseData.PartitionData>>,
        ): Int {
            // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
            // use arbitrary values here without affecting the result.
            val data = toMessage(Errors.NONE, 0, FetchMetadata.INVALID_SESSION_ID, partIterator)
            val cache = ObjectSerializationCache()
            return 4 + data.size(cache, version)
        }

        fun divergingEpoch(
            partitionResponse: FetchResponseData.PartitionData,
        ): FetchResponseData.EpochEndOffset? =
            if (partitionResponse.divergingEpoch.epoch < 0) null
            else partitionResponse.divergingEpoch

        fun isDivergingEpoch(partitionResponse: FetchResponseData.PartitionData): Boolean =
            partitionResponse.divergingEpoch.epoch >= 0

        fun preferredReadReplica(partitionResponse: FetchResponseData.PartitionData): Int? =
            if (partitionResponse.preferredReadReplica == INVALID_PREFERRED_REPLICA_ID) null
            else partitionResponse.preferredReadReplica

        fun isPreferredReplica(partitionResponse: FetchResponseData.PartitionData): Boolean =
            partitionResponse.preferredReadReplica != INVALID_PREFERRED_REPLICA_ID

        fun partitionResponse(
            topicIdPartition: TopicIdPartition,
            error: Errors,
        ): FetchResponseData.PartitionData =
            partitionResponse(topicIdPartition.topicPartition.partition, error)

        fun partitionResponse(partition: Int, error: Errors): FetchResponseData.PartitionData =
            FetchResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setErrorCode(error.code)
                .setHighWatermark(INVALID_HIGH_WATERMARK)

        /**
         * Returns `partition.records` as `Records` (instead of `BaseRecords`). If `records` is
         * `null`, returns `MemoryRecords.EMPTY`.
         *
         * If this response was deserialized after a fetch, this method should never fail. An
         * example where this would fail is a down-converted response (e.g.
         * LazyDownConversionRecords) on the broker (before it's serialized and sent on the wire).
         *
         * @param partition partition data
         * @return Records or empty record if the records in PartitionData is null.
         */
        fun recordsOrFail(partition: FetchResponseData.PartitionData): Records {
            if (partition.records == null) return MemoryRecords.EMPTY
            return partition.records as? Records ?: throw ClassCastException(
                "The record type is ${partition.records?.javaClass?.simpleName}, which is not a " +
                        "subtype of ${Records::class.java.simpleName}. This method is only safe to " +
                        "call if the `FetchResponse` was deserialized from bytes."
            )
        }

        /**
         * @return The size in bytes of the records. 0 is returned if records of input partition is
         * null.
         */
        fun recordsSize(partition: FetchResponseData.PartitionData): Int =
            partition.records?.sizeInBytes() ?: 0

        // TODO: remove as a part of KAFKA-12410
        @Deprecated("Remove as part of KAFKA-12410")
        fun of(
            error: Errors,
            throttleTimeMs: Int,
            sessionId: Int,
            responseData: Map<TopicIdPartition, FetchResponseData.PartitionData>,
        ): FetchResponse {
            return FetchResponse(
                toMessage(
                    error = error,
                    throttleTimeMs = throttleTimeMs,
                    sessionId = sessionId,
                    partIterator = responseData.entries.iterator(),
                )
            )
        }

        private fun matchingTopic(
            previousTopic: FetchableTopicResponse?,
            currentTopic: TopicIdPartition,
        ): Boolean {
            return if (previousTopic == null) false
            else if (previousTopic.topicId != Uuid.ZERO_UUID)
                previousTopic.topicId == currentTopic.topicId
            else previousTopic.topic == currentTopic.topicPartition.topic
        }

        private fun toMessage(
            error: Errors,
            throttleTimeMs: Int,
            sessionId: Int,
            partIterator: Iterator<Map.Entry<TopicIdPartition, FetchResponseData.PartitionData>>,
        ): FetchResponseData {
            val topicResponseList = mutableListOf<FetchableTopicResponse>()

            while (partIterator.hasNext()) {
                val (key, partitionData) = partIterator.next()

                // Since PartitionData alone doesn't know the partition ID, we set it here
                partitionData.setPartitionIndex(key.topicPartition.partition)

                // We have to keep the order of input topic-partition. Hence, we batch the
                // partitions only if the last batch is in the same topic group.
                val previousTopic = if (topicResponseList.isEmpty()) null
                else topicResponseList[topicResponseList.size - 1]

                if (matchingTopic(previousTopic, key))
                    previousTopic!!.partitions += partitionData
                else topicResponseList.add(
                    FetchableTopicResponse()
                        .setTopic(key.topicPartition.topic)
                        .setTopicId(key.topicId)
                        .setPartitions(listOf(partitionData))
                )
            }

            return FetchResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code)
                .setSessionId(sessionId)
                .setResponses(topicResponseList)
        }
    }
}
