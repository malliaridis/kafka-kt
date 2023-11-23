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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

/**
 * Possible error codes:
 *
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.INVALID_TXN_STATE]
 * - [Errors.INVALID_PRODUCER_ID_MAPPING]
 * - [Errors.INVALID_PRODUCER_EPOCH] // for version <=1
 * - [Errors.PRODUCER_FENCED]
 * - [Errors.TOPIC_AUTHORIZATION_FAILED]
 * - [Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 */
class AddPartitionsToTxnResponse : AbstractResponse {
    private val data: AddPartitionsToTxnResponseData
    private lateinit var cachedErrorsMap: MutableMap<TopicPartition, Errors>

    constructor(data: AddPartitionsToTxnResponseData) : super(ApiKeys.ADD_PARTITIONS_TO_TXN) {
        this.data = data
    }

    constructor(
        throttleTimeMs: Int,
        errors: Map<TopicPartition, Errors>
    ) : super(ApiKeys.ADD_PARTITIONS_TO_TXN) {
        val resultMap: MutableMap<String, AddPartitionsToTxnPartitionResultCollection> = HashMap()

        for ((topicPartition, value) in errors) {
            val topicName = topicPartition.topic
            val partitionResult = AddPartitionsToTxnPartitionResult()
                .setErrorCode(value.code)
                .setPartitionIndex(topicPartition.partition)
            val partitionResultCollection = resultMap.getOrDefault(
                topicName, AddPartitionsToTxnPartitionResultCollection()
            )
            partitionResultCollection.add(partitionResult)
            resultMap[topicName] = partitionResultCollection
        }

        val topicCollection = AddPartitionsToTxnTopicResultCollection()
        topicCollection.addAll(
            resultMap.map { (key, value) ->
                AddPartitionsToTxnTopicResult()
                    .setName(key)
                    .setResults(value)
            }
        )

        data = AddPartitionsToTxnResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResults(topicCollection)
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun errors(): Map<TopicPartition, Errors> {
        if (::cachedErrorsMap.isInitialized) return cachedErrorsMap

        cachedErrorsMap = HashMap()
        for (topicResult in data.results) {
            for (partitionResult in topicResult.results) {
                cachedErrorsMap[TopicPartition(
                    topicResult.name, partitionResult.partitionIndex
                )] = Errors.forCode(partitionResult.errorCode)
            }
        }
        return cachedErrorsMap
    }

    override fun errorCounts(): Map<Errors, Int> = errorCounts(errors().values)

    override fun data(): AddPartitionsToTxnResponseData = data

    override fun toString(): String = data.toString()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AddPartitionsToTxnResponse =
            AddPartitionsToTxnResponse(
                AddPartitionsToTxnResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
