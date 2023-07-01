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

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class AddOffsetsToTxnRequest(
    private val data: AddOffsetsToTxnRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ADD_OFFSETS_TO_TXN, version) {

    override fun data(): AddOffsetsToTxnRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AddOffsetsToTxnResponse =
        AddOffsetsToTxnResponse(
            AddOffsetsToTxnResponseData()
                .setErrorCode(Errors.forException(e).code)
                .setThrottleTimeMs(throttleTimeMs)
        )

    class Builder(
        var data: AddOffsetsToTxnRequestData,
    ) : AbstractRequest.Builder<AddOffsetsToTxnRequest>(ApiKeys.ADD_OFFSETS_TO_TXN) {

        override fun build(version: Short): AddOffsetsToTxnRequest=
            AddOffsetsToTxnRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AddOffsetsToTxnRequest =
            AddOffsetsToTxnRequest(
                AddOffsetsToTxnRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
