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
import java.util.stream.Collectors
import org.apache.kafka.common.message.AlterPartitionRequestData
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.common.message.AlterPartitionResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class AlterPartitionRequest(
    private val data: AlterPartitionRequestData,
    apiVersion: Short,
) : AbstractRequest(ApiKeys.ALTER_PARTITION, apiVersion) {

    override fun data(): AlterPartitionRequestData = data

    /**
     * Get an error response for a request with specified throttle time in the response if
     * applicable.
     */
    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse =
        AlterPartitionResponse(
            AlterPartitionResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code)
        )

    /**
     * @constructor Constructs a builder for AlterPartitionRequest.
     *
     * @property data The data to be sent. Note that because the version of the request is not known
     * at this time, it is expected that all topics have a topic id and a topic name set.
     * @param canUseTopicIds True if version 2 and above can be used.
     */
    class Builder(
        private val data: AlterPartitionRequestData,
        canUseTopicIds: Boolean,
    ) : AbstractRequest.Builder<AlterPartitionRequest>(
        ApiKeys.ALTER_PARTITION,
        // Version 1 is the maximum version that can be used without topic ids.
        ApiKeys.ALTER_PARTITION.oldestVersion(),
        if (canUseTopicIds) ApiKeys.ALTER_PARTITION.latestVersion() else 1
    ) {

        override fun build(version: Short): AlterPartitionRequest {
            if (version < 3) {
                data.topics.forEach { topicData ->
                    topicData.partitions.forEach { partitionData ->
                        // The newIsrWithEpochs will be empty after build. Then we can skip the conversion if the build
                        // is called again.
                        if (partitionData.newIsrWithEpochs.isNotEmpty()) {
                            val newIsr = partitionData.newIsrWithEpochs.map {it.brokerId }
                            partitionData.setNewIsr(newIsr.toIntArray())
                            partitionData.setNewIsrWithEpochs(emptyList())
                        }
                    }
                }
            }
            return AlterPartitionRequest(data, version)
        }

        override fun toString(): String = data.toString()
    }

    companion object {

        fun newIsrToSimpleNewIsrWithBrokerEpochs(newIsr: List<Int>): List<BrokerState> =
            newIsr.map { brokerId -> BrokerState().setBrokerId(brokerId) }

        fun parse(buffer: ByteBuffer, version: Short): AlterPartitionRequest =
            AlterPartitionRequest(
                AlterPartitionRequestData(ByteBufferAccessor(buffer),version),
                version,
            )
    }
}
