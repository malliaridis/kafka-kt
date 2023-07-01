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
import org.apache.kafka.common.message.ControlledShutdownResponseData
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartition
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartitionCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Consumer

/**
 * Possible error codes:
 *
 * - UNKNOWN(-1) (this is because IllegalStateException may be thrown in
 *   `KafkaController.shutdownBroker`, it would be good to improve this)
 * - BROKER_NOT_AVAILABLE(8)
 * - STALE_CONTROLLER_EPOCH(11)
 */
class ControlledShutdownResponse(
    private val data: ControlledShutdownResponseData,
) : AbstractResponse(ApiKeys.CONTROLLED_SHUTDOWN) {

    fun error(): Errors = Errors.forCode(data.errorCode())

    override fun errorCounts(): Map<Errors, Int> = errorCounts(error())

    override fun throttleTimeMs(): Int = DEFAULT_THROTTLE_TIME

    // Not supported by the response schema
    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) = Unit

    override fun data(): ControlledShutdownResponseData = data

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ControlledShutdownResponse =
            ControlledShutdownResponse(
                ControlledShutdownResponseData(ByteBufferAccessor(buffer), version)
            )

        fun prepareResponse(error: Errors, tps: Set<TopicPartition>): ControlledShutdownResponse {
            val data = ControlledShutdownResponseData()
            data.setErrorCode(error.code)
            val pSet = RemainingPartitionCollection()

            pSet.addAll(tps.map { (topic, partition) ->
                RemainingPartition()
                    .setTopicName(topic)
                    .setPartitionIndex(partition)
            })
            data.setRemainingPartitions(pSet)

            return ControlledShutdownResponse(data)
        }
    }
}
