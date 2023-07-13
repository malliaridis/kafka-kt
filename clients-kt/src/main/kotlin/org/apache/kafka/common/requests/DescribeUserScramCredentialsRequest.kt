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

import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer

class DescribeUserScramCredentialsRequest private constructor(
    private val data: DescribeUserScramCredentialsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, version) {

    override fun data(): DescribeUserScramCredentialsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, message) = ApiError.fromThrowable(e)
        val response = DescribeUserScramCredentialsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code)
            .setErrorMessage(message)

        response.results += data.users.map {
            DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                .setErrorCode(error.code)
                .setErrorMessage(message)
        }

        return DescribeUserScramCredentialsResponse(response)
    }

    class Builder(
        private val data: DescribeUserScramCredentialsRequestData,
    ) : AbstractRequest.Builder<DescribeUserScramCredentialsRequest>(
        ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
    ) {

        override fun build(version: Short): DescribeUserScramCredentialsRequest =
            DescribeUserScramCredentialsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeUserScramCredentialsRequest =
            DescribeUserScramCredentialsRequest(
                DescribeUserScramCredentialsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
