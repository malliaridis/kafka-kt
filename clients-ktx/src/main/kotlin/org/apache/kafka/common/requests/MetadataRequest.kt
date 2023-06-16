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
import java.util.stream.Collectors
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class MetadataRequest(
    private val data: MetadataRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.METADATA, version) {

    override fun data(): MetadataRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val error = Errors.forException(e)
        val responseData = MetadataResponseData()
        if (data.topics() != null) for (topic in data.topics()) {
            // the response does not allow null, so convert to empty string if necessary
            val topicName = if (topic.name() == null) "" else topic.name()
            responseData.topics().add(
                MetadataResponseTopic()
                    .setName(topicName)
                    .setTopicId(topic.topicId())
                    .setErrorCode(error.code)
                    .setIsInternal(false)
                    .setPartitions(emptyList())
            )
        }

        responseData.setThrottleTimeMs(throttleTimeMs)
        return MetadataResponse(responseData, true)
    }

    val isAllTopics: Boolean
        // In version 0, an empty topic list indicates
        // "request metadata for all topics."
        get() = data.topics() == null || data.topics().isEmpty() && version.toInt() == 0

    fun topics(): List<String>? {
        return if (isAllTopics) null // In version 0, we return null for empty topic list
        else data.topics().map { obj: MetadataRequestTopic -> obj.name() }
    }

    fun topicIds(): List<Uuid> {
        return if (isAllTopics) emptyList()
        else if (version < 10) emptyList()
        else data.topics().map { obj: MetadataRequestTopic -> obj.topicId() }
    }

    fun allowAutoTopicCreation(): Boolean = data.allowAutoTopicCreation()

    class Builder : AbstractRequest.Builder<MetadataRequest> {

        private val data: MetadataRequestData

        constructor(data: MetadataRequestData) : super(ApiKeys.METADATA) {
            this.data = data
        }

        constructor(
            topics: List<String?>?,
            allowAutoTopicCreation: Boolean,
            allowedVersion: Short,
        ) : this(topics, allowAutoTopicCreation, allowedVersion, allowedVersion)

        @JvmOverloads
        constructor(
            topics: List<String?>?,
            allowAutoTopicCreation: Boolean,
            minVersion: Short = ApiKeys.METADATA.oldestVersion(),
            maxVersion: Short = ApiKeys.METADATA.latestVersion(),
        ) : super(ApiKeys.METADATA, minVersion, maxVersion) {
            val data = MetadataRequestData()
            topics?.forEach { topic: String? ->
                data.topics().add(MetadataRequestTopic().setName(topic))
            } ?: data.setTopics(null)
            data.setAllowAutoTopicCreation(allowAutoTopicCreation)
            this.data = data
        }

        constructor(topicIds: List<Uuid?>?) : super(
            ApiKeys.METADATA,
            ApiKeys.METADATA.oldestVersion(),
            ApiKeys.METADATA.latestVersion()
        ) {
            val data = MetadataRequestData()
            topicIds?.forEach { topicId: Uuid? ->
                data.topics().add(MetadataRequestTopic().setTopicId(topicId))
            } ?: data.setTopics(null)

            // It's impossible to create topic with topicId
            data.setAllowAutoTopicCreation(false)
            this.data = data
        }

        fun emptyTopicList(): Boolean {
            return data.topics().isEmpty()
        }

        val isAllTopics: Boolean
            get() = data.topics() == null

        fun topics(): List<String> {
            return data.topics()
                .stream()
                .map { obj: MetadataRequestTopic -> obj.name() }
                .collect(Collectors.toList())
        }

        override fun build(version: Short): MetadataRequest {
            if (version < 1) throw UnsupportedVersionException(
                "MetadataRequest versions older than 1 are not supported."
            )

            if (!data.allowAutoTopicCreation() && version < 4) throw UnsupportedVersionException(
                "MetadataRequest versions older than 4 don't support the " +
                        "allowAutoTopicCreation field"
            )

            if (data.topics() != null) {
                data.topics().forEach { topic ->
                    if (topic.name() == null && version < 12) throw UnsupportedVersionException(
                        "MetadataRequest version $version does not support null topic names."
                    )
                    if (Uuid.ZERO_UUID != topic.topicId() && version < 12)
                        throw UnsupportedVersionException(
                        "MetadataRequest version $version does not support non-zero topic IDs."
                    )
                }
            }

            return MetadataRequest(data, version)
        }

        override fun toString(): String = data.toString()

        companion object {

            private val ALL_TOPICS_REQUEST_DATA =
                MetadataRequestData().setTopics(null).setAllowAutoTopicCreation(true)

            // This never causes auto-creation, but we set the boolean to true because that is
            // the default value when deserializing V2 and older. This way, the value is
            // consistent after serialization and deserialization.
            fun allTopics(): Builder = Builder(ALL_TOPICS_REQUEST_DATA)
        }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): MetadataRequest =
            MetadataRequest(
                MetadataRequestData(ByteBufferAccessor(buffer), version),
                version,
            )

        fun convertToMetadataRequestTopic(topics: Collection<String?>): List<MetadataRequestTopic> =
            topics.map { topic -> MetadataRequestTopic().setName(topic) }

        fun convertTopicIdsToMetadataRequestTopic(
            topicIds: Collection<Uuid?>,
        ): List<MetadataRequestTopic> = topicIds.map { topicId: Uuid? ->
            MetadataRequestTopic().setTopicId(topicId)
        }
    }
}
