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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class AddPartitionsToTxnRequest(
    private val data: AddPartitionsToTxnRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ADD_PARTITIONS_TO_TXN, version) {

    override fun data(): AddPartitionsToTxnRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AddPartitionsToTxnResponse {
        val error = Errors.forException(e)
        val response = AddPartitionsToTxnResponseData()
        if (version < EARLIEST_BROKER_VERSION) response.setResultsByTopicV3AndBelow(
            errorResponseForTopics(data.v3AndBelowTopics, error)
        )
        else response.setErrorCode(error.code)
        response.setThrottleTimeMs(throttleTimeMs)

        return AddPartitionsToTxnResponse(response)
    }

    fun partitionsByTransaction(): Map<String, List<TopicPartition>> {
        val partitionsByTransaction = mutableMapOf<String, List<TopicPartition>>()
        for (transaction in data.transactions) {
            val partitions = getPartitions(transaction.topics)
            partitionsByTransaction[transaction.transactionalId] = partitions
        }
        return partitionsByTransaction
    }

    // Takes a version 3 or below request (client request) and returns a v4+ singleton (one transaction ID) request.
    fun normalizeRequest(): AddPartitionsToTxnRequest {
        return AddPartitionsToTxnRequest(
            data = AddPartitionsToTxnRequestData().setTransactions(singletonTransaction()),
            version = version,
        )
    }

    // This method returns true if all the transactions in it are verify only. One reason to distinguish
    // is to separate requests that will need to write to log in the non error case (adding partitions)
    // from ones that will not (verify only).
    fun allVerifyOnlyRequest(): Boolean =
        version > LAST_CLIENT_VERSION && data.transactions.all(AddPartitionsToTxnTransaction::verifyOnly)

    private fun singletonTransaction(): AddPartitionsToTxnTransactionCollection {
        val singleTxn = AddPartitionsToTxnTransactionCollection()
        singleTxn.add(
            AddPartitionsToTxnTransaction()
                .setTransactionalId(data.v3AndBelowTransactionalId)
                .setProducerId(data.v3AndBelowProducerId)
                .setProducerEpoch(data.v3AndBelowProducerEpoch)
                .setTopics(data.v3AndBelowTopics)
        )
        return singleTxn
    }

    fun errorResponseForTransaction(transactionalId: String, e: Errors): AddPartitionsToTxnResult {
        val txnResult = AddPartitionsToTxnResult().setTransactionalId(transactionalId)
        val topicResults = errorResponseForTopics(data.transactions.find(transactionalId)!!.topics, e)
        txnResult.setTopicResults(topicResults)
        return txnResult
    }

    private fun errorResponseForTopics(
        topics: AddPartitionsToTxnTopicCollection,
        e: Errors,
    ): AddPartitionsToTxnTopicResultCollection {
        val topicResults = AddPartitionsToTxnTopicResultCollection()
        for (topic in topics) {
            val topicResult = AddPartitionsToTxnTopicResult().setName(topic.name)
            val partitionResult = AddPartitionsToTxnPartitionResultCollection()
            for (partition in topic.partitions) {
                partitionResult.add(
                    AddPartitionsToTxnPartitionResult()
                        .setPartitionIndex(partition)
                        .setPartitionErrorCode(e.code)
                )
            }
            topicResult.setResultsByPartition(partitionResult)
            topicResults.add(topicResult)
        }
        return topicResults
    }

    class Builder private constructor(
        minVersion: Short,
        maxVersion: Short,
        val data: AddPartitionsToTxnRequestData,
    ) : AbstractRequest.Builder<AddPartitionsToTxnRequest>(
        apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN,
        oldestAllowedVersion = minVersion,
        latestAllowedVersion = maxVersion,
    ) {

        override fun build(version: Short): AddPartitionsToTxnRequest =
            AddPartitionsToTxnRequest(data, version)

        override fun toString(): String = data.toString()

        companion object {

            fun forClient(
                transactionalId: String,
                producerId: Long,
                producerEpoch: Short,
                partitions: List<TopicPartition>,
            ): Builder {
                val topics = buildTxnTopicCollection(partitions)
                return Builder(
                    minVersion = ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(),
                    maxVersion = LAST_CLIENT_VERSION,
                    data = AddPartitionsToTxnRequestData()
                        .setV3AndBelowTransactionalId(transactionalId)
                        .setV3AndBelowProducerId(producerId)
                        .setV3AndBelowProducerEpoch(producerEpoch)
                        .setV3AndBelowTopics(topics)
                )
            }

            fun forBroker(transactions: AddPartitionsToTxnTransactionCollection): Builder = Builder(
                EARLIEST_BROKER_VERSION,
                ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(),
                AddPartitionsToTxnRequestData().setTransactions(transactions)
            )

            private fun buildTxnTopicCollection(partitions: List<TopicPartition>): AddPartitionsToTxnTopicCollection {
                val partitionMap = mutableMapOf<String, MutableList<Int>>()
                for ((topicName, partition) in partitions) {
                    partitionMap.compute(topicName) { _, subPartitions ->
                        val subPartitionList = subPartitions ?: mutableListOf()
                        subPartitionList.add(partition)
                        subPartitionList
                    }
                }
                val topics = AddPartitionsToTxnTopicCollection()
                for ((key, value) in partitionMap) {
                    topics.add(
                        AddPartitionsToTxnTopic()
                            .setName(key)
                            .setPartitions(value.toIntArray())
                    )
                }
                return topics
            }
        }
    }

    companion object {

        private const val LAST_CLIENT_VERSION: Short = 3

        // Note: earliest broker version is also the first version to support verification requests.
        private const val EARLIEST_BROKER_VERSION: Short = 4

        fun parse(
            buffer: ByteBuffer,
            version: Short,
        ): AddPartitionsToTxnRequest = AddPartitionsToTxnRequest(
            AddPartitionsToTxnRequestData(ByteBufferAccessor(buffer), version), version
        )

        fun getPartitions(topics: AddPartitionsToTxnTopicCollection): List<TopicPartition> {
            return topics.flatMap { topicCollection ->
                topicCollection.partitions.map { partition ->
                    TopicPartition(topicCollection.name, partition)
                }
            }
        }
    }
}
