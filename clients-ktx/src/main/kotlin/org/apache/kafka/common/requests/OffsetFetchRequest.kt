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
import java.util.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.OffsetFetchRequestData
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.slf4j.LoggerFactory

class OffsetFetchRequest private constructor(
    private val data: OffsetFetchRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.OFFSET_FETCH, version) {

    fun groupId(): String = data.groupId()

    fun requireStable(): Boolean = data.requireStable()

    fun groupIdsToPartitions(): Map<String, List<TopicPartition>?> {
        val groupIdsToPartitions = mutableMapOf<String, List<TopicPartition>?>()

        for (group: OffsetFetchRequestGroup in data.groups()) {
            var tpList: MutableList<TopicPartition>? = null

            if (group.topics() !== ALL_TOPIC_PARTITIONS_BATCH) {
                tpList = ArrayList()

                for (topic: OffsetFetchRequestTopics in group.topics())
                    for (partitionIndex in topic.partitionIndexes())
                        tpList.add(TopicPartition(topic.name(), partitionIndex))
            }
            groupIdsToPartitions[group.groupId()] = tpList
        }

        return groupIdsToPartitions
    }

    fun groupIdsToTopics(): Map<String, List<OffsetFetchRequestTopics>> =
        data.groups().associateBy(
            keySelector = OffsetFetchRequestGroup::groupId,
            valueTransform = OffsetFetchRequestGroup::topics
        )

    fun groupIds(): List<String> = data.groups().map(OffsetFetchRequestGroup::groupId)

    fun getErrorResponse(error: Errors): OffsetFetchResponse =
        getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, error)

    fun getErrorResponse(throttleTimeMs: Int, error: Errors): OffsetFetchResponse {
        val responsePartitions = mutableMapOf<TopicPartition, OffsetFetchResponse.PartitionData>()

        if (version < 2) {
            val partitionError = OffsetFetchResponse.PartitionData(
                offset = OffsetFetchResponse.INVALID_OFFSET,
                leaderEpoch = null,
                metadata = OffsetFetchResponse.NO_METADATA,
                error = error
            )

            for (topic in data.topics()) {
                for (partitionIndex in topic.partitionIndexes())
                    responsePartitions[TopicPartition(topic.name(), partitionIndex)] =
                        partitionError
            }

            return OffsetFetchResponse(error, responsePartitions)
        }

        if (version.toInt() == 2)
            return OffsetFetchResponse(error, responsePartitions)

        if (version in 3..7)
            return OffsetFetchResponse(throttleTimeMs, error, responsePartitions)

        val groupIds = groupIds()
        val errorsMap: MutableMap<String, Errors> = HashMap(groupIds.size)
        val partitionMap: MutableMap<String, Map<TopicPartition, OffsetFetchResponse.PartitionData>> =
            HashMap(groupIds.size)

        for (groupId in groupIds) {
            errorsMap[groupId] = error
            partitionMap[groupId] = responsePartitions
        }

        return OffsetFetchResponse(throttleTimeMs, errorsMap, partitionMap)
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): OffsetFetchResponse =
        getErrorResponse(throttleTimeMs, Errors.forException(e))

    val isAllPartitions: Boolean
        get() = data.topics() === ALL_TOPIC_PARTITIONS

    fun isAllPartitionsForGroup(groupId: String): Boolean {
        val group = data.groups().first { group -> (group.groupId() == groupId) }
        return group.topics() === ALL_TOPIC_PARTITIONS_BATCH
    }

    override fun data(): OffsetFetchRequestData {
        return data
    }

    class Builder : AbstractRequest.Builder<OffsetFetchRequest> {

        val data: OffsetFetchRequestData

        private val throwOnFetchStableOffsetsUnsupported: Boolean

        constructor(
            groupId: String?,
            requireStable: Boolean,
            partitions: List<TopicPartition>?,
            throwOnFetchStableOffsetsUnsupported: Boolean,
        ) : super(ApiKeys.OFFSET_FETCH) {
            val topics: List<OffsetFetchRequestTopic>?
            if (partitions != null) {
                val offsetFetchRequestTopicMap = mutableMapOf<String, OffsetFetchRequestTopic>()

                for (topicPartition: TopicPartition in partitions) {
                    val topicName = topicPartition.topic
                    val topic = offsetFetchRequestTopicMap.getOrDefault(
                        topicName, OffsetFetchRequestTopic().setName(topicName)
                    )
                    topic.partitionIndexes().add(topicPartition.partition)
                    offsetFetchRequestTopicMap[topicName] = topic
                }
                topics = ArrayList(offsetFetchRequestTopicMap.values)
            } else {
                // If passed in partition list is null, it is requesting offsets for all topic partitions.
                topics = ALL_TOPIC_PARTITIONS
            }
            data = OffsetFetchRequestData()
                .setGroupId(groupId)
                .setRequireStable(requireStable)
                .setTopics(topics)

            this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported
        }

        val isAllTopicPartitions: Boolean
            get() = data.topics() === ALL_TOPIC_PARTITIONS

        constructor(
            groupIdToTopicPartitionMap: Map<String, List<TopicPartition>?>,
            requireStable: Boolean,
            throwOnFetchStableOffsetsUnsupported: Boolean,
        ) : super(ApiKeys.OFFSET_FETCH) {
            val groups: MutableList<OffsetFetchRequestGroup> = ArrayList()
            for (entry in groupIdToTopicPartitionMap) {
                val groupName = entry.key
                val tpList = entry.value
                val topics: List<OffsetFetchRequestTopics>?
                if (tpList != null) {
                    val offsetFetchRequestTopicMap =
                        mutableMapOf<String, OffsetFetchRequestTopics>()

                    for (topicPartition: TopicPartition in tpList) {
                        val topicName = topicPartition.topic
                        val topic = offsetFetchRequestTopicMap.getOrDefault(
                            topicName,
                            OffsetFetchRequestTopics().setName(topicName)
                        )

                        topic.partitionIndexes().add(topicPartition.partition)
                        offsetFetchRequestTopicMap[topicName] = topic
                    }
                    topics = ArrayList(offsetFetchRequestTopicMap.values)
                } else topics = ALL_TOPIC_PARTITIONS_BATCH

                groups.add(
                    OffsetFetchRequestGroup()
                        .setGroupId(groupName)
                        .setTopics(topics)
                )
            }
            data = OffsetFetchRequestData()
                .setGroups(groups)
                .setRequireStable(requireStable)

            this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported
        }

        override fun build(version: Short): OffsetFetchRequest {
            if (isAllTopicPartitions && version < 2) {
                throw UnsupportedVersionException(
                    "The broker only supports OffsetFetchRequest " +
                            "v" + version + ", but we need v2 or newer to request all topic partitions."
                )
            }
            if (data.groups().size > 1 && version < 8) {
                throw NoBatchedOffsetFetchRequestException(
                    ("Broker does not support"
                            + " batching groups for fetch offset request on version " + version)
                )
            }
            if (data.requireStable() && version < 7) {
                if (throwOnFetchStableOffsetsUnsupported) {
                    throw UnsupportedVersionException(
                        ("Broker unexpectedly " +
                                "doesn't support requireStable flag on version " + version)
                    )
                } else {
                    log.trace(
                        ("Fallback the requireStable flag to false as broker " +
                                "only supports OffsetFetchRequest version {}. Need " +
                                "v7 or newer to enable this feature"), version
                    )
                    data.setRequireStable(false)
                }
            }
            // convert data to use the appropriate version since version 8 uses different format
            if (version < 8) {
                var oldDataFormat: OffsetFetchRequestData? = null
                if (data.groups().isNotEmpty()) {
                    val group = data.groups()[0]
                    val groupName = group.groupId()
                    val topics = group.topics()
                    var oldFormatTopics: List<OffsetFetchRequestTopic>? = null
                    if (topics != null) oldFormatTopics = topics.map { topic ->
                        OffsetFetchRequestTopic()
                            .setName(topic.name())
                            .setPartitionIndexes(topic.partitionIndexes())
                    }

                    oldDataFormat = OffsetFetchRequestData()
                        .setGroupId(groupName)
                        .setTopics(oldFormatTopics)
                        .setRequireStable(data.requireStable())
                }
                return OffsetFetchRequest(oldDataFormat ?: data, version)
            } else if (data.groups().isEmpty()) {
                val groupName = data.groupId()
                val oldFormatTopics = data.topics()
                var topics: List<OffsetFetchRequestTopics>? = null
                if (oldFormatTopics != null) topics = oldFormatTopics.map { topic ->
                    OffsetFetchRequestTopics()
                        .setName(topic.name())
                        .setPartitionIndexes(topic.partitionIndexes())
                }

                val convertedDataFormat = OffsetFetchRequestData().setGroups(
                    listOf(
                        OffsetFetchRequestGroup()
                            .setGroupId(groupName)
                            .setTopics(topics)
                    )
                ).setRequireStable(data.requireStable())

                return OffsetFetchRequest(convertedDataFormat, version)
            }

            return OffsetFetchRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    /**
     * Indicates that it is not possible to fetch consumer groups in batches with FetchOffset.
     * Instead, consumer groups' offsets must be fetched one by one.
     */
    class NoBatchedOffsetFetchRequestException(
        message: String?,
    ) : UnsupportedVersionException(message) {

        companion object {
            private val serialVersionUID = 1L
        }
    }

    fun partitions(): List<TopicPartition>? {
        if (isAllPartitions) return null

        val partitions = mutableListOf<TopicPartition>()
        for (topic: OffsetFetchRequestTopic in data.topics())
            for (partitionIndex in topic.partitionIndexes())
                partitions.add(TopicPartition(topic.name(), partitionIndex))

        return partitions
    }

    companion object {

        private val log = LoggerFactory.getLogger(OffsetFetchRequest::class.java)

        private val ALL_TOPIC_PARTITIONS: List<OffsetFetchRequestTopic>? = null

        private val ALL_TOPIC_PARTITIONS_BATCH: List<OffsetFetchRequestTopics>? = null

        fun parse(buffer: ByteBuffer, version: Short): OffsetFetchRequest =
            OffsetFetchRequest(OffsetFetchRequestData(ByteBufferAccessor(buffer), version), version)
    }
}
