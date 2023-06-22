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

import org.apache.kafka.common.message.EndTxnResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

/**
 * Possible error codes:
 *
 * - [Errors.NOT_COORDINATOR]
 * - [Errors.COORDINATOR_NOT_AVAILABLE]
 * - [Errors.COORDINATOR_LOAD_IN_PROGRESS]
 * - [Errors.INVALID_TXN_STATE]
 * - [Errors.INVALID_PRODUCER_ID_MAPPING]
 * - [Errors.INVALID_PRODUCER_EPOCH] // for version <=1
 * - [Errors.PRODUCER_FENCED]
 * - [Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED]
 */
class EndTxnResponse(private val data: EndTxnResponseData) : AbstractResponse(ApiKeys.END_TXN) {

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun error(): Errors = Errors.forCode(data.errorCode())

    override fun errorCounts(): Map<Errors, Int> = errorCounts(error())

    override fun data(): EndTxnResponseData = data

    override fun toString(): String = data.toString()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): EndTxnResponse =
            EndTxnResponse(EndTxnResponseData(ByteBufferAccessor(buffer), version))
    }
}
