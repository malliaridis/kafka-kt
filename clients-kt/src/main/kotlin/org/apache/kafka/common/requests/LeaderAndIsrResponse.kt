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
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicErrorCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

/**
 * Possible error code:
 *
 * - STALE_CONTROLLER_EPOCH (11)
 * - STALE_BROKER_EPOCH (77)
 */
class LeaderAndIsrResponse(
    private val data: LeaderAndIsrResponseData,
    private val version: Short,
) : AbstractResponse(ApiKeys.LEADER_AND_ISR) {

    fun topics(): LeaderAndIsrTopicErrorCollection = data.topics

    fun error(): Errors = Errors.forCode(data.errorCode)

    override fun errorCounts(): Map<Errors, Int> {
        val error = error()
        // Minor optimization since the top-level error applies to all partitions
        if (error != Errors.NONE) mapOf(
            if (version < 5) error to data.partitionErrors.size + 1
            else error to data.topics.sumOf { it.partitionErrors.size } + 1
        )

        val errors = errorCounts(
            if (version < 5) data.partitionErrors
                .map { Errors.forCode(it.errorCode) }
            else data.topics
                .flatMap { it.partitionErrors }
                .map { Errors.forCode(it.errorCode) }
        ).toMutableMap()
        updateErrorCounts(errors, Errors.NONE)
        return errors
    }

    fun partitionErrors(topicNames: Map<Uuid, String>): Map<TopicPartition, Errors> {
        val errors = mutableMapOf<TopicPartition, Errors>()
        if (version < 5) data.partitionErrors.forEach { partition ->
            errors[TopicPartition(partition.topicName, partition.partitionIndex)] =
                Errors.forCode(partition.errorCode)

        } else data.topics.forEach { topic ->
            val topicName = topicNames[topic.topicId]
            if (topicName != null) topic.partitionErrors.forEach { partition ->
                errors[TopicPartition(topicName, partition.partitionIndex)] =
                    Errors.forCode(partition.errorCode)
            }
        }

        return errors
    }

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    override fun data(): LeaderAndIsrResponseData = data

    override fun toString(): String = data.toString()

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): LeaderAndIsrResponse =
            LeaderAndIsrResponse(
                LeaderAndIsrResponseData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
