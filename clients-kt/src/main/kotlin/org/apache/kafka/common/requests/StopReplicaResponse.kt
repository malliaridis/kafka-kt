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
import java.util.*
import org.apache.kafka.common.message.StopReplicaResponseData
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error code:
 * - [Errors.STALE_CONTROLLER_EPOCH]
 * - [Errors.STALE_BROKER_EPOCH]
 * - [Errors.FENCED_LEADER_EPOCH]
 * - [Errors.KAFKA_STORAGE_ERROR]
 */
class StopReplicaResponse(
    private val data: StopReplicaResponseData,
) : AbstractResponse(ApiKeys.STOP_REPLICA) {

    fun partitionErrors(): List<StopReplicaPartitionError> = data.partitionErrors

    fun error(): Errors = Errors.forCode(data.errorCode)

    override fun errorCounts(): Map<Errors, Int> {
        if (data.errorCode != Errors.NONE.code)
        // Minor optimization since the top-level error applies to all partitions
            return mapOf(error() to data.partitionErrors.size + 1)

        val errors = errorCounts(data.partitionErrors.map { Errors.forCode(it.errorCode) })

        updateErrorCounts(
            errorCounts = errors.toMutableMap(),
            error = Errors.forCode(data.errorCode) // top level error
        )

        return errors
    }

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    override fun data(): StopReplicaResponseData = data

    override fun toString(): String = data.toString()

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): StopReplicaResponse =
            StopReplicaResponse(
                StopReplicaResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
