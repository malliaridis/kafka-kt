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

import org.apache.kafka.common.message.ListTransactionsRequestData
import org.apache.kafka.common.message.ListTransactionsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class ListTransactionsRequest private constructor(
    private val data: ListTransactionsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.LIST_TRANSACTIONS, version) {


    override fun data(): ListTransactionsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): ListTransactionsResponse {
        val error = Errors.forException(e)
        val response = ListTransactionsResponseData()
            .setErrorCode(error.code)
            .setThrottleTimeMs(throttleTimeMs)

        return ListTransactionsResponse(response)
    }

    override fun toString(verbose: Boolean): String = data.toString()

    class Builder(
        val data: ListTransactionsRequestData,
    ) : AbstractRequest.Builder<ListTransactionsRequest>(ApiKeys.LIST_TRANSACTIONS) {

        override fun build(version: Short): ListTransactionsRequest =
            ListTransactionsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ListTransactionsRequest =
            ListTransactionsRequest(
                ListTransactionsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
