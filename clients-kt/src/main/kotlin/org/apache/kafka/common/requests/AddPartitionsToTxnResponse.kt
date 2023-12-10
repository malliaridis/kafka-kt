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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

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
class AddPartitionsToTxnResponse(
    private val data: AddPartitionsToTxnResponseData,
) : AbstractResponse(ApiKeys.ADD_PARTITIONS_TO_TXN) {

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun errors(): Map<String, Map<TopicPartition, Errors>> {
        val errorsMap = mutableMapOf<String, Map<TopicPartition, Errors>>()
        if (!data.resultsByTopicV3AndBelow.isEmpty()) {
            errorsMap[V3_AND_BELOW_TXN_ID] = errorsForTransaction(data.resultsByTopicV3AndBelow)
        }
        for (result in data.resultsByTransaction)
            errorsMap[result.transactionalId] = errorsForTransaction(result.topicResults)

        return errorsMap
    }

    override fun errorCounts(): Map<Errors, Int> {
        val allErrors: MutableList<Errors> = ArrayList()

        // If we are not using this field, we have request 4 or later
        if (data.resultsByTopicV3AndBelow.isEmpty()) allErrors.add(Errors.forCode(data.errorCode))

        errors().forEach { (_, errors) -> allErrors.addAll(errors.values) }
        return errorCounts(allErrors)
    }

    fun getTransactionTopicResults(transactionalId: String): AddPartitionsToTxnTopicResultCollection =
        data.resultsByTransaction.find(transactionalId)!!.topicResults

    override fun data(): AddPartitionsToTxnResponseData = data

    override fun toString(): String = data.toString()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        const val V3_AND_BELOW_TXN_ID = ""

        private fun topicCollectionForErrors(
            errors: Map<TopicPartition, Errors>,
        ): AddPartitionsToTxnTopicResultCollection {
            val resultMap = mutableMapOf<String, AddPartitionsToTxnPartitionResultCollection>()

            for ((topicPartition, error) in errors) {
                val topicName = topicPartition.topic
                val partitionResult = AddPartitionsToTxnPartitionResult()
                    .setPartitionErrorCode(error.code)
                    .setPartitionIndex(topicPartition.partition)
                val partitionResultCollection =
                    resultMap.getOrDefault(topicName, AddPartitionsToTxnPartitionResultCollection())
                partitionResultCollection.add(partitionResult)
                resultMap[topicName] = partitionResultCollection
            }

            val topicCollection = AddPartitionsToTxnTopicResultCollection()
            for ((key, value) in resultMap) {
                topicCollection.add(
                    AddPartitionsToTxnTopicResult()
                        .setName(key)
                        .setResultsByPartition(value)
                )
            }
            return topicCollection
        }

        fun resultForTransaction(
            transactionalId: String,
            errors: Map<TopicPartition, Errors>,
        ): AddPartitionsToTxnResult = AddPartitionsToTxnResult()
            .setTransactionalId(transactionalId)
            .setTopicResults(topicCollectionForErrors(errors))

        fun errorsForTransaction(topicCollection: AddPartitionsToTxnTopicResultCollection): Map<TopicPartition, Errors> {
            val topicResults: MutableMap<TopicPartition, Errors> = HashMap()
            for (topicResult in topicCollection) {
                for (partitionResult in topicResult.resultsByPartition) {
                    topicResults[TopicPartition(topicResult.name, partitionResult.partitionIndex)] =
                        Errors.forCode(partitionResult.partitionErrorCode)
                }
            }
            return topicResults
        }

        fun parse(buffer: ByteBuffer, version: Short): AddPartitionsToTxnResponse =
            AddPartitionsToTxnResponse(
                AddPartitionsToTxnResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
