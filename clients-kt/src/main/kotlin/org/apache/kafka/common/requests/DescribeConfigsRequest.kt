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

import org.apache.kafka.common.message.DescribeConfigsRequestData
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DescribeConfigsRequest(
    private val data: DescribeConfigsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_CONFIGS, version) {

    override fun data(): DescribeConfigsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): DescribeConfigsResponse {
        val error = Errors.forException(e)
        return DescribeConfigsResponse(
            DescribeConfigsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setResults(
                    data.resources.map { result ->
                        DescribeConfigsResponseData.DescribeConfigsResult()
                            .setErrorCode(error.code)
                            .setErrorMessage(error.message)
                            .setResourceName(result.resourceName)
                            .setResourceType(result.resourceType)
                    }
                )
        )
    }

    class Builder(
        private val data: DescribeConfigsRequestData,
    ) : AbstractRequest.Builder<DescribeConfigsRequest>(ApiKeys.DESCRIBE_CONFIGS) {

        override fun build(version: Short): DescribeConfigsRequest =
            DescribeConfigsRequest(data, version)
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeConfigsRequest =
            DescribeConfigsRequest(
                DescribeConfigsRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
