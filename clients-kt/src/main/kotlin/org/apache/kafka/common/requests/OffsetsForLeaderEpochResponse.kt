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
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch

/**
 * Possible error codes:
 * - [Errors.TOPIC_AUTHORIZATION_FAILED] If the user does not have DESCRIBE access to a requested
 *   topic
 * - [Errors.REPLICA_NOT_AVAILABLE] If the request is received by a broker with version < 2.6 which
 *   is not a replica
 * - [Errors.NOT_LEADER_OR_FOLLOWER] If the broker is not a leader or follower and either the
 *   provided leader epoch matches the known leader epoch on the broker or is empty
 * - [Errors.FENCED_LEADER_EPOCH] If the epoch is lower than the broker's epoch
 * - [Errors.UNKNOWN_LEADER_EPOCH] If the epoch is larger than the broker's epoch
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION] If the broker does not have metadata for a topic or
 *   partition
 * - [Errors.KAFKA_STORAGE_ERROR] If the log directory for one of the requested partitions is
 *   offline
 * - [Errors.UNKNOWN_SERVER_ERROR] For any unexpected errors
 */
class OffsetsForLeaderEpochResponse(
    private val data: OffsetForLeaderEpochResponseData,
) : AbstractResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH) {

    override fun data(): OffsetForLeaderEpochResponseData = data

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()
        data.topics().forEach { topic: OffsetForLeaderTopicResult ->
            topic.partitions().forEach { partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode()))
            }
        }
        return errorCounts
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun toString(): String = data.toString()

    companion object {

        const val UNDEFINED_EPOCH_OFFSET = RecordBatch.NO_PARTITION_LEADER_EPOCH.toLong()

        const val UNDEFINED_EPOCH = RecordBatch.NO_PARTITION_LEADER_EPOCH

        fun parse(buffer: ByteBuffer, version: Short): OffsetsForLeaderEpochResponse =
            OffsetsForLeaderEpochResponse(
                OffsetForLeaderEpochResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
