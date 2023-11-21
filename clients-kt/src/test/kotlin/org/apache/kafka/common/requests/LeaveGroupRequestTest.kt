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

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.LeaveGroupRequestData
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.fail

class LeaveGroupRequestTest {
    
    private val groupId = "group_id"
    
    private val memberIdOne = "member_1"
    
    private val instanceIdOne = "instance_1"
    
    private val memberIdTwo = "member_2"
    
    private val instanceIdTwo = "instance_2"
    
    private val throttleTimeMs = 10
    
    private lateinit var builder: LeaveGroupRequest.Builder
    
    private lateinit var members: List<MemberIdentity>
    
    @BeforeEach
    fun setUp() {
        members = listOf(
            MemberIdentity()
                .setMemberId(memberIdOne)
                .setGroupInstanceId(instanceIdOne),
            MemberIdentity()
                .setMemberId(memberIdTwo)
                .setGroupInstanceId(instanceIdTwo),
        )
        builder = LeaveGroupRequest.Builder(groupId, members)
    }

    @Test
    fun testMultiLeaveConstructor() {
        val expectedData = LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMembers(members)
        for (version in ApiKeys.LEAVE_GROUP.allVersions()) {
            try {
                val request = builder.build(version)
                if (version <= 2)
                    fail("Older version $version request data should not be created due to non-single members")
                assertEquals(expectedData, request.data())
                assertEquals(members, request.members())
                val expectedResponse = LeaveGroupResponse(
                    memberResponses = emptyList(),
                    topLevelError = Errors.COORDINATOR_LOAD_IN_PROGRESS,
                    throttleTimeMs = throttleTimeMs,
                    version = version,
                )
                assertEquals(
                    expected = expectedResponse,
                    actual = request.getErrorResponse(
                        throttleTimeMs = throttleTimeMs,
                        e = (Errors.COORDINATOR_LOAD_IN_PROGRESS.exception)!!,
                    )
                )
            } catch (e: UnsupportedVersionException) {
                assertContains(e.message!!, "leave group request only supports single member instance")
            }
        }
    }

    @Test
    fun testSingleLeaveConstructor() {
        val expectedData = LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberIdOne)
        val singleMember = listOf(MemberIdentity().setMemberId(memberIdOne))
        builder = LeaveGroupRequest.Builder(groupId, singleMember)
        for (version in 0..2) {
            val request = builder.build(version.toShort())
            assertEquals(expectedData, request.data())
            assertEquals(singleMember, request.members())
            val expectedThrottleTime = if (version >= 1) throttleTimeMs else AbstractResponse.DEFAULT_THROTTLE_TIME
            val expectedResponse = LeaveGroupResponse(
                LeaveGroupResponseData()
                    .setErrorCode(Errors.NOT_CONTROLLER.code)
                    .setThrottleTimeMs(expectedThrottleTime)
            )
            assertEquals(
                expected = expectedResponse,
                actual = request.getErrorResponse(
                    throttleTimeMs = throttleTimeMs,
                    e = Errors.NOT_CONTROLLER.exception!!,
                ),
            )
        }
    }

    @Test
    fun testBuildEmptyMembers() {
        assertFailsWith<IllegalArgumentException> { LeaveGroupRequest.Builder(groupId, emptyList()) }
    }
}
