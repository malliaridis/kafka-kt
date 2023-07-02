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

import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.message.DescribeClusterResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer

class DescribeClusterRequest(
    private val data: DescribeClusterRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_CLUSTER, version) {

    override fun data(): DescribeClusterRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, message) = ApiError.fromThrowable(e)
        return DescribeClusterResponse(
            DescribeClusterResponseData()
                .setErrorCode(error.code)
                .setErrorMessage(message)
        )
    }

    override fun toString(verbose: Boolean): String = data.toString()

    class Builder(
        private val data: DescribeClusterRequestData,
    ) : AbstractRequest.Builder<DescribeClusterRequest>(ApiKeys.DESCRIBE_CLUSTER) {

        override fun build(version: Short): DescribeClusterRequest =
            DescribeClusterRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeClusterRequest =
            DescribeClusterRequest(
                DescribeClusterRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
