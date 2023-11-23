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
import org.apache.kafka.common.message.DescribeLogDirsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

class DescribeLogDirsResponse(
    private val data: DescribeLogDirsResponseData,
) : AbstractResponse(ApiKeys.DESCRIBE_LOG_DIRS) {

    override fun data(): DescribeLogDirsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()
        errorCounts[Errors.forCode(data.errorCode)] = 1
        data.results.forEach { result: DescribeLogDirsResponseData.DescribeLogDirsResult ->
            updateErrorCounts(errorCounts, Errors.forCode(result.errorCode))
        }

        return errorCounts
    }

    // Note this class is part of the public API, reachable from Admin.describeLogDirs()
    /**
     * Possible error code:
     *
     * - KAFKA_STORAGE_ERROR (56)
     * - UNKNOWN (-1)
     */
    @Deprecated(
        """Deprecated Since Kafka 2.7.
      Use {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#descriptions()}
      and {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#allDescriptions()} to access 
      the replacement class {@link org.apache.kafka.clients.admin.LogDirDescription}."""
    )
    class LogDirInfo(val error: Errors, val replicaInfos: Map<TopicPartition, ReplicaInfo>) {

        override fun toString(): String = "(error=$error, replicas=$replicaInfos)"
    }

    // Note this class is part of the public API, reachable from Admin.describeLogDirs()
    @Deprecated(
        """Deprecated Since Kafka 2.7.
      Use {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#descriptions()}
      and {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#allDescriptions()} to access 
      the replacement class {@link org.apache.kafka.clients.admin.ReplicaInfo}."""
    )
    class ReplicaInfo(val size: Long, val offsetLag: Long, val isFuture: Boolean) {
        override fun toString(): String {
            return "(size=$size" +
                    ", offsetLag=$offsetLag" +
                    ", isFuture=$isFuture" +
                    ")"
        }
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        const val INVALID_OFFSET_LAG = -1L

        const val UNKNOWN_VOLUME_BYTES = -1L

        fun parse(buffer: ByteBuffer, version: Short): DescribeLogDirsResponse =
            DescribeLogDirsResponse(
                DescribeLogDirsResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
