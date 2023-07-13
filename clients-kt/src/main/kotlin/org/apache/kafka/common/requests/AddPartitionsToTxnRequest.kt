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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.BiFunction

class AddPartitionsToTxnRequest(
    private val data: AddPartitionsToTxnRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ADD_PARTITIONS_TO_TXN, version) {

    private lateinit var cachedPartitions: List<TopicPartition>

    class Builder : AbstractRequest.Builder<AddPartitionsToTxnRequest> {

        val data: AddPartitionsToTxnRequestData

        constructor(data: AddPartitionsToTxnRequestData) : super(ApiKeys.ADD_PARTITIONS_TO_TXN) {
            this.data = data
        }

        constructor(
            transactionalId: String?,
            producerId: Long,
            producerEpoch: Short,
            partitions: List<TopicPartition>
        ) : super(ApiKeys.ADD_PARTITIONS_TO_TXN) {
            val partitionMap: MutableMap<String, MutableList<Int>> = HashMap()
            for ((topicName, partition) in partitions) {
                partitionMap.compute(topicName) { _, subPartitions ->
                    val subPartitions = subPartitions ?: mutableListOf()

                    subPartitions.add(partition)
                    subPartitions
                }
            }

            val topics = AddPartitionsToTxnTopicCollection()

            topics.addAll(
                partitionMap.map { (key, value) ->
                    AddPartitionsToTxnTopic()
                        .setName(key)
                        .setPartitions(value.toIntArray())
                }
            )

            data = AddPartitionsToTxnRequestData()
                .setTransactionalId(transactionalId!!)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setTopics(topics)
        }

        override fun build(version: Short): AddPartitionsToTxnRequest =
            AddPartitionsToTxnRequest(data, version)

        override fun toString(): String = data.toString()

        companion object {

            fun getPartitions(data: AddPartitionsToTxnRequestData): List<TopicPartition> =
                data.topics().flatMap { topicCollection ->
                    topicCollection.partitions().map { partition ->
                        TopicPartition(topicCollection.name(), partition)
                    }
            }
        }
    }

    fun partitions(): List<TopicPartition> {
        if (::cachedPartitions.isInitialized) return cachedPartitions
        cachedPartitions = Builder.getPartitions(data)
        return cachedPartitions
    }

    override fun data(): AddPartitionsToTxnRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AddPartitionsToTxnResponse {
        val errors = partitions().associateWith { Errors.forException(e) }
        return AddPartitionsToTxnResponse(throttleTimeMs, errors)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AddPartitionsToTxnRequest =
            AddPartitionsToTxnRequest(
                AddPartitionsToTxnRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
