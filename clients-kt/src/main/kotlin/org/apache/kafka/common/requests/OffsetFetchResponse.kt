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
import org.apache.kafka.common.message.OffsetFetchResponseData
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 *   - [Errors.TOPIC_AUTHORIZATION_FAILED]
 *   - [Errors.UNSTABLE_OFFSET_COMMIT]
 * - Group or coordinator errors:
 *   - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 *   - [Errors.COORDINATOR_NOT_AVAILABLE]
 *   - [Errors.NOT_COORDINATOR]
 *   - [Errors.GROUP_AUTHORIZATION_FAILED]
 */
class OffsetFetchResponse : AbstractResponse {

    private val data: OffsetFetchResponseData
    private val error: Errors?
    private val groupLevelErrors: MutableMap<String, Errors?> = HashMap()

    constructor(data: OffsetFetchResponseData) : super(ApiKeys.OFFSET_FETCH) {
        this.data = data
        error = null
    }

    /**
     * Constructor without throttle time.
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    constructor(error: Errors, responseData: Map<TopicPartition, PartitionData>) : this(
        DEFAULT_THROTTLE_TIME, error, responseData
    )

    /**
     * Constructor with throttle time for version 0 to 7
     * @param throttleTimeMs The time in milliseconds that this response was throttled
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    constructor(
        throttleTimeMs: Int,
        error: Errors,
        responseData: Map<TopicPartition, PartitionData>
    ) : super(ApiKeys.OFFSET_FETCH) {
        val offsetFetchResponseTopicMap: MutableMap<String, OffsetFetchResponseTopic> = HashMap()

        responseData.forEach { (key, partitionData) ->

            val topicName = key.topic
            val topic = offsetFetchResponseTopicMap.getOrDefault(
                key = topicName,
                defaultValue = OffsetFetchResponseTopic().setName(topicName)
            )

            topic.partitions().add(
                OffsetFetchResponsePartition()
                    .setPartitionIndex(key.partition)
                    .setErrorCode(partitionData.error.code)
                    .setCommittedOffset(partitionData.offset)
                    .setCommittedLeaderEpoch(
                        partitionData.leaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                    )
                    .setMetadata(partitionData.metadata)
            )
            offsetFetchResponseTopicMap[topicName] = topic
        }

        data = OffsetFetchResponseData()
            .setTopics(ArrayList(offsetFetchResponseTopicMap.values))
            .setErrorCode(error.code)
            .setThrottleTimeMs(throttleTimeMs)

        this.error = error
    }

    /**
     * Constructor with throttle time for version 8 and above.
     * @param throttleTimeMs The time in milliseconds that this response was throttled
     * @param errors Potential coordinator or group level error code
     * @param responseData Fetched offset information grouped by topic-partition and by group
     */
    constructor(
        throttleTimeMs: Int,
        errors: Map<String, Errors>,
        responseData: Map<String, Map<TopicPartition, PartitionData>>
    ) : super(ApiKeys.OFFSET_FETCH) {

        val groupList: MutableList<OffsetFetchResponseGroup> = ArrayList()

        responseData.forEach { (groupName, partitionDataMap) ->

            val offsetFetchResponseTopicsMap = mutableMapOf<String, OffsetFetchResponseTopics>()

            partitionDataMap.forEach { (key, partitionData) ->

                val topicName = key.topic
                val topic = offsetFetchResponseTopicsMap.getOrDefault(
                    topicName,
                    OffsetFetchResponseTopics().setName(topicName)
                )

                topic.partitions().add(
                    OffsetFetchResponsePartitions()
                        .setPartitionIndex(key.partition)
                        .setErrorCode(partitionData.error.code)
                        .setCommittedOffset(partitionData.offset)
                        .setCommittedLeaderEpoch(
                            partitionData.leaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH
                        )
                        .setMetadata(partitionData.metadata)
                )

                offsetFetchResponseTopicsMap[topicName] = topic
            }
            groupList.add(
                OffsetFetchResponseGroup()
                    .setGroupId(groupName)
                    .setTopics(ArrayList(offsetFetchResponseTopicsMap.values))
                    .setErrorCode(errors[groupName]!!.code)
            )
            groupLevelErrors[groupName] = errors[groupName]
        }

        data = OffsetFetchResponseData()
            .setGroups(groupList)
            .setThrottleTimeMs(throttleTimeMs)

        error = null
    }

    constructor(data: OffsetFetchResponseData, version: Short) : super(ApiKeys.OFFSET_FETCH) {
        this.data = data
        // for version 2 and later use the top-level error code (in ERROR_CODE_KEY_NAME) from the response.
        // for older versions there is no top-level error in the response and all errors are partition errors,
        // so if there is a group or coordinator error at the partition level use that as the top-level error.
        // this way clients can depend on the top-level error regardless of the offset fetch version.
        // we return the error differently starting with version 8, so we will only populate the
        // error field if we are between version 2 and 7. if we are in version 8 or greater, then
        // we will populate the map of group id to error codes.
        if (version < 8) {
            error = if (version >= 2) Errors.forCode(data.errorCode()) else topLevelError(data)
        } else {
            data.groups().forEach { group ->
                groupLevelErrors[group.groupId()] = Errors.forCode(group.errorCode())
            }
            error = null
        }
    }

    data class PartitionData(
        val offset: Long,
        val leaderEpoch: Int?,
        val metadata: String,
        val error: Errors
    ) {

        fun hasError(): Boolean {
            return error !== Errors.NONE
        }

        override fun toString(): String {
            return "PartitionData(" +
                    "offset=$offset" +
                    ", leaderEpoch=${leaderEpoch ?: RecordBatch.NO_PARTITION_LEADER_EPOCH}" +
                    ", metadata=$metadata" +
                    ", error='$error" +
                    ")"
        }
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun hasError(): Boolean = error !== Errors.NONE

    fun groupHasError(groupId: String): Boolean {
        val error = groupLevelErrors[groupId] ?: return error != null && error !== Errors.NONE
        return error !== Errors.NONE
    }

    fun error(): Errors? = error

    fun groupLevelError(groupId: String): Errors? = error ?: groupLevelErrors[groupId]

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()

        if (groupLevelErrors.isNotEmpty()) {
            // built response with v8 or above
            groupLevelErrors.forEach { (_, value) -> updateErrorCounts(counts, value!!) }

            data.groups().forEach { group ->
                group.topics().forEach { topic ->
                    topic.partitions().forEach { partition ->
                        updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                    }
                }
            }
        } else {
            // built response with v0-v7
            updateErrorCounts(counts, error!!)

            data.topics().forEach { topic ->
                topic.partitions().forEach { partition ->
                    updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                }
            }
        }
        return counts
    }

    // package-private for testing purposes
    fun responseDataV0ToV7(): Map<TopicPartition, PartitionData> {
        val responseData: MutableMap<TopicPartition, PartitionData> = HashMap()

        data.topics().forEach { topic ->
            topic.partitions().forEach { partition ->

                responseData[TopicPartition(topic.name(), partition.partitionIndex())] =
                    PartitionData(
                        partition.committedOffset(),
                        RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()),
                        partition.metadata(),
                        Errors.forCode(partition.errorCode())
                    )
            }
        }
        return responseData
    }

    private fun buildResponseData(groupId: String): Map<TopicPartition, PartitionData> {
        val responseData: MutableMap<TopicPartition, PartitionData> = HashMap()
        val group = data
            .groups()
            .stream()
            .filter { g: OffsetFetchResponseGroup -> g.groupId() == groupId }
            .collect(Collectors.toList())[0]

        group.topics().forEach { topic ->
            topic.partitions().forEach { partition ->

                responseData[TopicPartition(topic.name(), partition.partitionIndex())] =
                    PartitionData(
                        offset = partition.committedOffset(),
                        leaderEpoch = RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()),
                        metadata = partition.metadata(),
                        error = Errors.forCode(partition.errorCode())
                    )
            }
        }
        return responseData
    }

    fun partitionDataMap(groupId: String): Map<TopicPartition, PartitionData> {
        return if (groupLevelErrors.isEmpty()) responseDataV0ToV7()
        else buildResponseData(groupId)
    }

    override fun data(): OffsetFetchResponseData = data

    override fun shouldClientThrottle(version: Short): Boolean = version >= 4

    companion object {

        const val INVALID_OFFSET = -1L

        const val NO_METADATA = ""

        val UNKNOWN_PARTITION = PartitionData(
            INVALID_OFFSET,
            null,
            NO_METADATA,
            Errors.UNKNOWN_TOPIC_OR_PARTITION
        )

        val UNAUTHORIZED_PARTITION = PartitionData(
            INVALID_OFFSET,
            null,
            NO_METADATA,
            Errors.TOPIC_AUTHORIZATION_FAILED
        )

        private val PARTITION_ERRORS = listOf(
            Errors.UNKNOWN_TOPIC_OR_PARTITION,
            Errors.TOPIC_AUTHORIZATION_FAILED,
        )

        private fun topLevelError(data: OffsetFetchResponseData): Errors {
            for (topic in data.topics()) {
                for (partition in topic.partitions()) {
                    val partitionError = Errors.forCode(partition.errorCode())
                    if (partitionError !== Errors.NONE && !PARTITION_ERRORS.contains(partitionError)) {
                        return partitionError
                    }
                }
            }
            return Errors.NONE
        }

        fun parse(buffer: ByteBuffer?, version: Short): OffsetFetchResponse {
            return OffsetFetchResponse(
                OffsetFetchResponseData(
                    ByteBufferAccessor(buffer!!),
                    version
                ), version
            )
        }
    }
}
