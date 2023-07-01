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

import org.apache.kafka.common.message.BrokerRegistrationRequestData
import org.apache.kafka.common.message.BrokerRegistrationResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class BrokerRegistrationRequest(
    private val data: BrokerRegistrationRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.BROKER_REGISTRATION, version) {

    override fun data(): BrokerRegistrationRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): BrokerRegistrationResponse =
        BrokerRegistrationResponse(
            BrokerRegistrationResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code)
        )

    class Builder(
        private val data: BrokerRegistrationRequestData,
    ) : AbstractRequest.Builder<BrokerRegistrationRequest>(ApiKeys.BROKER_REGISTRATION) {

        override fun oldestAllowedVersion(): Short {
            return if (data.isMigratingZkBroker) 1.toShort()
            else 0.toShort()
        }

        override val oldestAllowedVersion : Short
            get() = if (data.isMigratingZkBroker) 1.toShort()
            else 0.toShort()

        override fun build(version: Short): BrokerRegistrationRequest =
            BrokerRegistrationRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): BrokerRegistrationRequest =
            BrokerRegistrationRequest(
                BrokerRegistrationRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
