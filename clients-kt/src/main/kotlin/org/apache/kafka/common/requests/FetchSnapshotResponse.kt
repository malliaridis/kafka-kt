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
import java.util.function.UnaryOperator
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchSnapshotResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class FetchSnapshotResponse(private val data: FetchSnapshotResponseData) :
    AbstractResponse(ApiKeys.FETCH_SNAPSHOT) {
    override fun errorCounts(): Map<Errors, Int> {
        val errors = mutableMapOf<Errors, Int>()
        val topLevelError = Errors.forCode(data.errorCode)
        if (topLevelError !== Errors.NONE) errors[topLevelError] = 1

        for (topicResponse in data.topics)
            for (partitionResponse in topicResponse.partitions)
                errors.compute(Errors.forCode(partitionResponse.errorCode)) { _, count ->
                    if (count == null) 1 else count + 1
                }

        return errors
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun data(): FetchSnapshotResponseData = data

    companion object {

        /**
         * Creates a FetchSnapshotResponseData with a top level error.
         *
         * @param error the top level error
         * @return the created fetch snapshot response data
         */
        fun withTopLevelError(error: Errors): FetchSnapshotResponseData =
            FetchSnapshotResponseData().setErrorCode(error.code)

        /**
         * Creates a FetchSnapshotResponseData with a single PartitionSnapshot for the topic
         * partition.
         *
         * The partition index will already by populated when calling operator.
         *
         * @param topicPartition the topic partition to include
         * @param operator unary operator responsible for populating all of the appropriate fields
         * @return the created fetch snapshot response data
         */
        fun singleton(
            topicPartition: TopicPartition,
            operator: UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot>,
        ): FetchSnapshotResponseData {
            val partitionSnapshot = operator.apply(
                FetchSnapshotResponseData.PartitionSnapshot().setIndex(topicPartition.partition)
            )

            return FetchSnapshotResponseData()
                .setTopics(
                    listOf(
                        FetchSnapshotResponseData.TopicSnapshot()
                            .setName(topicPartition.topic)
                            .setPartitions(listOf(partitionSnapshot))
                    )
                )
        }

        /**
         * Finds the PartitionSnapshot for a given topic partition.
         *
         * @param data the fetch snapshot response data
         * @param topicPartition the topic partition to find
         * @return the response partition snapshot if found, otherwise an empty Optional
         */
        fun forTopicPartition(
            data: FetchSnapshotResponseData,
            topicPartition: TopicPartition,
        ): FetchSnapshotResponseData.PartitionSnapshot? {
            return data
                .topics
                .filter { topic -> topic.name == topicPartition.topic }
                .flatMap { topic -> topic.partitions }
                .find { partition -> partition.index == topicPartition.partition }
        }

        fun parse(buffer: ByteBuffer, version: Short): FetchSnapshotResponse =
            FetchSnapshotResponse(
                FetchSnapshotResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
