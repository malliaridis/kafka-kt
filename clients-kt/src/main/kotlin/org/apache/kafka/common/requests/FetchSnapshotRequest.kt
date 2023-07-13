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
import org.apache.kafka.common.message.FetchSnapshotRequestData
import org.apache.kafka.common.message.FetchSnapshotResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.UnaryOperator

class FetchSnapshotRequest(
    private val data: FetchSnapshotRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.FETCH_SNAPSHOT, version) {

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): FetchSnapshotResponse {
        return FetchSnapshotResponse(
            FetchSnapshotResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code)
        )
    }

    override fun data(): FetchSnapshotRequestData = data

    class Builder(
        private val data: FetchSnapshotRequestData,
    ) : AbstractRequest.Builder<FetchSnapshotRequest>(ApiKeys.FETCH_SNAPSHOT) {

        override fun build(version: Short): FetchSnapshotRequest =
            FetchSnapshotRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        /**
         * Creates a FetchSnapshotRequestData with a single PartitionSnapshot for the topic
         * partition.
         *
         * The partition index will already be populated when calling operator.
         *
         * @param topicPartition the topic partition to include
         * @param operator unary operator responsible for populating all the appropriate fields
         * @return the created fetch snapshot request data
         */
        fun singleton(
            clusterId: String?,
            topicPartition: TopicPartition,
            operator: UnaryOperator<FetchSnapshotRequestData.PartitionSnapshot>,
        ): FetchSnapshotRequestData {
            val partitionSnapshot = operator.apply(
                FetchSnapshotRequestData.PartitionSnapshot()
                    .setPartition(topicPartition.partition)
            )

            return FetchSnapshotRequestData()
                .setClusterId(clusterId)
                .setTopics(
                    listOf(
                        FetchSnapshotRequestData.TopicSnapshot()
                            .setName(topicPartition.topic)
                            .setPartitions(listOf(partitionSnapshot))
                    )
                )
        }

        /**
         * Finds the PartitionSnapshot for a given topic partition.
         *
         * @param data the fetch snapshot request data
         * @param topicPartition the topic partition to find
         * @return the request partition snapshot if found, otherwise an empty Optional
         */
        fun forTopicPartition(
            data: FetchSnapshotRequestData,
            topicPartition: TopicPartition,
        ): FetchSnapshotRequestData.PartitionSnapshot? {
            return data
                .topics()
                .filter { topic -> topic.name() == topicPartition.topic }
                .flatMap { topic -> topic.partitions() }
                .firstOrNull { partition -> partition.partition() == topicPartition.partition }
        }

        fun parse(buffer: ByteBuffer, version: Short): FetchSnapshotRequest =
            FetchSnapshotRequest(
                FetchSnapshotRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
