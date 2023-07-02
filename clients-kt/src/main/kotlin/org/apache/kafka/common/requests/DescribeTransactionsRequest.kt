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

import org.apache.kafka.common.message.DescribeTransactionsRequestData
import org.apache.kafka.common.message.DescribeTransactionsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DescribeTransactionsRequest private constructor(
    private val data: DescribeTransactionsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_TRANSACTIONS, version) {

    override fun data(): DescribeTransactionsRequestData = data

    override fun getErrorResponse(
        throttleTimeMs: Int,
        e: Throwable,
    ): DescribeTransactionsResponse {
        val error = Errors.forException(e)
        val response = DescribeTransactionsResponseData().setThrottleTimeMs(throttleTimeMs)

        response.transactionStates().addAll(
            data.transactionalIds().map { transactionalId ->
                DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(error.code)
            }
        )

        return DescribeTransactionsResponse(response)
    }

    override fun toString(verbose: Boolean): String = data.toString()

    class Builder(
        val data: DescribeTransactionsRequestData,
    ) : AbstractRequest.Builder<DescribeTransactionsRequest>(ApiKeys.DESCRIBE_TRANSACTIONS) {

        override fun build(version: Short): DescribeTransactionsRequest =
            DescribeTransactionsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeTransactionsRequest =
            DescribeTransactionsRequest(
                DescribeTransactionsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
