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

import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LeaveGroupResponseTest {

    private val memberIdOne = "member_1"

    private val instanceIdOne = "instance_1"

    private val memberIdTwo = "member_2"

    private val instanceIdTwo = "instance_2"

    private val throttleTimeMs = 10

    private lateinit var memberResponses: List<MemberResponse>

    @BeforeEach
    fun setUp() {
        memberResponses = listOf(
            MemberResponse()
                .setMemberId(memberIdOne)
                .setGroupInstanceId(instanceIdOne)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code),
            MemberResponse()
                .setMemberId(memberIdTwo)
                .setGroupInstanceId(instanceIdTwo)
                .setErrorCode(Errors.FENCED_INSTANCE_ID.code),
        )
    }

    @Test
    fun testConstructorWithMemberResponses() {
        val expectedErrorCounts = mapOf(
            Errors.NONE to 1, // top level
            Errors.UNKNOWN_MEMBER_ID to 1,
            Errors.FENCED_INSTANCE_ID to 1,
        )
        for (version in ApiKeys.LEAVE_GROUP.allVersions()) {
            val leaveGroupResponse = LeaveGroupResponse(
                memberResponses = memberResponses,
                topLevelError = Errors.NONE,
                throttleTimeMs = throttleTimeMs,
                version = version,
            )
            if (version >= 3) {
                assertEquals(expectedErrorCounts, leaveGroupResponse.errorCounts())
                assertEquals(memberResponses, leaveGroupResponse.memberResponses())
            } else {
                assertEquals(
                    expected = mapOf(Errors.UNKNOWN_MEMBER_ID to 1),
                    actual = leaveGroupResponse.errorCounts(),
                )
                assertEquals(emptyList(), leaveGroupResponse.memberResponses())
            }
            if (version >= 1) assertEquals(throttleTimeMs, leaveGroupResponse.throttleTimeMs())
            else assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, leaveGroupResponse.throttleTimeMs())
            assertEquals(Errors.UNKNOWN_MEMBER_ID, leaveGroupResponse.error())
        }
    }

    @Test
    fun testShouldThrottle() {
        val response = LeaveGroupResponse(LeaveGroupResponseData())
        for (version in ApiKeys.LEAVE_GROUP.allVersions()) {
            if (version >= 2) assertTrue(response.shouldClientThrottle(version))
            else assertFalse(response.shouldClientThrottle(version))
        }
    }

    @Test
    fun testEqualityWithSerialization() {
        val responseData = LeaveGroupResponseData()
            .setErrorCode(Errors.NONE.code)
            .setThrottleTimeMs(throttleTimeMs)
        for (version in ApiKeys.LEAVE_GROUP.allVersions()) {
            val primaryResponse = LeaveGroupResponse.parse(
                buffer = toByteBuffer(responseData, version),
                version = version,
            )
            val secondaryResponse = LeaveGroupResponse.parse(
                buffer = toByteBuffer(responseData, version),
                version = version,
            )
            assertEquals(primaryResponse, primaryResponse)
            assertEquals(primaryResponse, secondaryResponse)
            assertEquals(primaryResponse.hashCode(), secondaryResponse.hashCode())
        }
    }

    @Test
    fun testParse() {
        val expectedErrorCounts = mapOf(Errors.NOT_COORDINATOR to 1)
        val data = LeaveGroupResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code)
            .setThrottleTimeMs(throttleTimeMs)
        for (version in ApiKeys.LEAVE_GROUP.allVersions()) {
            val buffer = toByteBuffer(data, version)
            val leaveGroupResponse = LeaveGroupResponse.parse(buffer, version)
            assertEquals(expectedErrorCounts, leaveGroupResponse.errorCounts())
            if (version >= 1) assertEquals(throttleTimeMs, leaveGroupResponse.throttleTimeMs())
            else assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, leaveGroupResponse.throttleTimeMs())
            assertEquals(Errors.NOT_COORDINATOR, leaveGroupResponse.error())
        }
    }

    @Test
    fun testEqualityWithMemberResponses() {
        for (version in ApiKeys.LEAVE_GROUP.allVersions()) {
            val localResponses =
                (if (version > 2) memberResponses
                else memberResponses.subList(0, 1)).toMutableList()
            val primaryResponse = LeaveGroupResponse(
                memberResponses = localResponses,
                topLevelError = Errors.NONE,
                throttleTimeMs = throttleTimeMs,
                version = version
            )

            // The order of members should not alter result data.
            localResponses.reverse()
            val reversedResponse = LeaveGroupResponse(
                memberResponses = localResponses,
                topLevelError = Errors.NONE,
                throttleTimeMs = throttleTimeMs,
                version = version
            )
            assertEquals(primaryResponse, primaryResponse)
            assertEquals(primaryResponse, reversedResponse)
            assertEquals(primaryResponse.hashCode(), reversedResponse.hashCode())
        }
    }
}
