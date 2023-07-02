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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.WriteTxnMarkersResponseData
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerResult
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * - [Errors.CORRUPT_MESSAGE]
 * - [Errors.INVALID_PRODUCER_EPOCH]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 * - [Errors.NOT_LEADER_OR_FOLLOWER]
 * - [Errors.MESSAGE_TOO_LARGE]
 * - [Errors.RECORD_LIST_TOO_LARGE]
 * - [Errors.NOT_ENOUGH_REPLICAS]
 * - [Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND]
 * - [Errors.INVALID_REQUIRED_ACKS]
 * - [Errors.TRANSACTION_COORDINATOR_FENCED]
 * - [Errors.REQUEST_TIMED_OUT]
 * - [Errors.CLUSTER_AUTHORIZATION_FAILED]
 */
class WriteTxnMarkersResponse : AbstractResponse {

    private val data: WriteTxnMarkersResponseData

    constructor(errors: Map<Long, Map<TopicPartition, Errors>>) : super(ApiKeys.WRITE_TXN_MARKERS) {
        val markers: List<WritableTxnMarkerResult> = errors.map { (key, value) ->
            val responseTopicDataMap: MutableMap<String, WritableTxnMarkerTopicResult> = HashMap()
            for ((topicPartition, value1) in value) {
                val topicName = topicPartition.topic
                val topic = responseTopicDataMap.getOrDefault(
                    topicName,
                    WritableTxnMarkerTopicResult().setName(topicName)
                )
                topic.partitions().add(
                    WritableTxnMarkerPartitionResult()
                        .setErrorCode(value1.code)
                        .setPartitionIndex(topicPartition.partition)
                )
                responseTopicDataMap[topicName] = topic
            }
            WritableTxnMarkerResult()
                .setProducerId(key)
                .setTopics(ArrayList(responseTopicDataMap.values))
        }

        data = WriteTxnMarkersResponseData().setMarkers(markers)
    }

    constructor(data: WriteTxnMarkersResponseData) : super(ApiKeys.WRITE_TXN_MARKERS) {
        this.data = data
    }

    override fun data(): WriteTxnMarkersResponseData = data

    fun errorsByProducerId(): Map<Long, Map<TopicPartition, Errors>> {
        val errors = mutableMapOf<Long, Map<TopicPartition, Errors>>()

        for (marker in data.markers()) {
            val topicPartitionErrorsMap: MutableMap<TopicPartition, Errors> = HashMap()
            for (topic in marker.topics()) {
                for (partitionResult in topic.partitions()) {
                    topicPartitionErrorsMap[
                        TopicPartition(
                            topic.name(),
                            partitionResult.partitionIndex()
                        )
                    ] = Errors.forCode(partitionResult.errorCode())
                }
            }
            errors[marker.producerId()] = topicPartitionErrorsMap
        }
        return errors
    }

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()

        for (marker in data.markers()) {
            for (topic in marker.topics()) {
                for (partitionResult in topic.partitions())
                    updateErrorCounts(errorCounts, Errors.forCode(partitionResult.errorCode()))
            }
        }

        return errorCounts
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): WriteTxnMarkersResponse {
            return WriteTxnMarkersResponse(
                WriteTxnMarkersResponseData(ByteBufferAccessor(buffer), version)
            )
        }
    }
}
