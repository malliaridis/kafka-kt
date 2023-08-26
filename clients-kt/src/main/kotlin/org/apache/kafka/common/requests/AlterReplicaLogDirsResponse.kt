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

import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

/**
 * Possible error codes:
 *
 * - [Errors.LOG_DIR_NOT_FOUND]
 * - [Errors.KAFKA_STORAGE_ERROR]
 * - [Errors.REPLICA_NOT_AVAILABLE]
 * - [Errors.UNKNOWN_SERVER_ERROR]
 */
class AlterReplicaLogDirsResponse(
    private val data: AlterReplicaLogDirsResponseData,
) : AbstractResponse(ApiKeys.ALTER_REPLICA_LOG_DIRS) {

    override fun data(): AlterReplicaLogDirsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()
        data.results.forEach { topicResult ->
            topicResult.partitions.forEach { partitionResult ->
                updateErrorCounts(errorCounts, Errors.forCode(partitionResult.errorCode))
            }
        }

        return errorCounts
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterReplicaLogDirsResponse =
            AlterReplicaLogDirsResponse(
                AlterReplicaLogDirsResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
