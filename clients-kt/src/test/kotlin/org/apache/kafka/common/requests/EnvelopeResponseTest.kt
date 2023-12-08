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

import java.nio.ByteBuffer
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.test.TestUtils.toBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class EnvelopeResponseTest {

    @Test
    fun testToSend() {
        for (version in ApiKeys.ENVELOPE.allVersions()) {
            val responseData = ByteBuffer.wrap("foobar".toByteArray())
            val response = EnvelopeResponse(responseData, Errors.NONE)
            val headerVersion = ApiKeys.ENVELOPE.responseHeaderVersion(version)
            val header = ResponseHeader(15, headerVersion)

            val send = response.toSend(header, version)
            val buffer = toBuffer(send)
            assertEquals(send.size() - 4, buffer.getInt().toLong())
            val parsedHeader = ResponseHeader.parse(buffer, headerVersion)
            assertEquals(header.size(), parsedHeader.size())
            assertEquals(header, parsedHeader)

            val parsedResponseData = EnvelopeResponseData()
            parsedResponseData.read(ByteBufferAccessor(buffer), version)
            assertEquals(response.data(), parsedResponseData)
        }
    }
}
