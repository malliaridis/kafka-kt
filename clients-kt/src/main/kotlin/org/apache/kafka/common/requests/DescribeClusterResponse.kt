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

import org.apache.kafka.common.Node
import org.apache.kafka.common.message.DescribeClusterResponseData
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer
import java.util.function.Function
import java.util.stream.Collectors

class DescribeClusterResponse(
    private val data: DescribeClusterResponseData,
) : AbstractResponse(ApiKeys.DESCRIBE_CLUSTER) {

    fun nodes(): Map<Int, Node> = data.brokers().associateBy(
        keySelector = { it.brokerId },
        valueTransform = { broker ->
            Node(
                id = broker.brokerId,
                host = broker.host,
                port = broker.port,
                rack = broker.rack
            )
        }
    )

    override fun errorCounts(): Map<Errors, Int> = errorCounts(Errors.forCode(data.errorCode()))

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun data(): DescribeClusterResponseData = data

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeClusterResponse =
            DescribeClusterResponse(
                DescribeClusterResponseData(ByteBufferAccessor(buffer), version)
            )
    }
}
