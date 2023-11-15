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

import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.JoinGroupRequestData
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.apache.kafka.test.TestUtils.randomString
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class JoinGroupRequestTest {

    @Test
    fun shouldAcceptValidGroupInstanceIds() {
        val maxLengthString = randomString(249)
        val validGroupInstanceIds = arrayOf(
            "valid",
            "INSTANCE",
            "gRoUp",
            "ar6",
            "VaL1d",
            "_0-9_.",
            "...",
            maxLengthString,
        )
        for (instanceId in validGroupInstanceIds) JoinGroupRequest.validateGroupInstanceId(instanceId)
    }

    @Test
    fun shouldThrowOnInvalidGroupInstanceIds() {
        val longString = CharArray(250) { 'a' }
        val invalidGroupInstanceIds = arrayOf("", "foo bar", "..", "foo:bar", "foo=bar", ".", String(longString))
        for (instanceId in invalidGroupInstanceIds) {
            assertFailsWith<InvalidConfigurationException>(
                message = "No exception was thrown for invalid instance id: $instanceId",
            ) { JoinGroupRequest.validateGroupInstanceId(instanceId) }
        }
    }

    @Test
    fun testRequestVersionCompatibilityFailBuild() {
        assertFailsWith<UnsupportedVersionException> {
            JoinGroupRequest.Builder(
                JoinGroupRequestData()
                    .setGroupId("groupId")
                    .setMemberId("consumerId")
                    .setGroupInstanceId("groupInstanceId")
                    .setProtocolType("consumer")
            ).build(version = 4)
        }
    }

    @Test
    fun testRebalanceTimeoutDefaultsToSessionTimeoutV0() {
        val sessionTimeoutMs = 30000
        val version: Short = 0
        val buffer = toByteBuffer(
            message = JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId("consumerId")
                .setProtocolType("consumer")
                .setSessionTimeoutMs(sessionTimeoutMs),
            version = version,
        )
        val request = JoinGroupRequest.parse(buffer = buffer, version = version)
        assertEquals(sessionTimeoutMs, request.data().sessionTimeoutMs)
        assertEquals(sessionTimeoutMs, request.data().rebalanceTimeoutMs)
    }
}
