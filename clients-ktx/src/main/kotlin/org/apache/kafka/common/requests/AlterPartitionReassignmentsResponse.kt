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

import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

class AlterPartitionReassignmentsResponse(
    private val data: AlterPartitionReassignmentsResponseData,
) : AbstractResponse(ApiKeys.ALTER_PARTITION_REASSIGNMENTS) {

    override fun data(): AlterPartitionReassignmentsResponseData = data

    override fun shouldClientThrottle(version: Short): Boolean = true

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val counts = mutableMapOf<Errors, Int>()
        updateErrorCounts(counts, Errors.forCode(data.errorCode()))
        data.responses().forEach { topicResponse ->
            topicResponse.partitions().forEach { partitionResponse ->
                updateErrorCounts(counts, Errors.forCode(partitionResponse.errorCode()))
            }
        }

        return counts
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterPartitionReassignmentsResponse =
            AlterPartitionReassignmentsResponse(
                AlterPartitionReassignmentsResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
