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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.test.TestUtils.assertFutureError
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import org.apache.kafka.test.TestUtils.assertNotFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class RemoveMembersFromConsumerGroupResultTest {
    
    private val instanceOne = MemberToRemove("instance-1")
    
    private val instanceTwo = MemberToRemove("instance-2")
    
    private lateinit var membersToRemove: MutableSet<MemberToRemove>
    
    private lateinit var errorsMap: MutableMap<MemberIdentity, Errors>
    
    private lateinit var memberFutures: KafkaFutureImpl<Map<MemberIdentity, Errors>>
    
    @BeforeEach
    fun setUp() {
        memberFutures = KafkaFutureImpl()
        membersToRemove = hashSetOf()
        membersToRemove.add(instanceOne)
        membersToRemove.add(instanceTwo)
        errorsMap = HashMap()
        errorsMap[instanceOne.toMemberIdentity()] = Errors.NONE
        errorsMap[instanceTwo.toMemberIdentity()] = Errors.FENCED_INSTANCE_ID
    }

    @Test
    @Throws(InterruptedException::class)
    fun testTopLevelErrorConstructor() {
        memberFutures.completeExceptionally(Errors.GROUP_AUTHORIZATION_FAILED.exception!!)
        val topLevelErrorResult = RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove)
        assertFutureError(topLevelErrorResult.all(), GroupAuthorizationException::class.java)
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testMemberLevelErrorConstructor() {
        createAndVerifyMemberLevelError()
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testMemberMissingErrorInRequestConstructor() {
        errorsMap.remove(instanceTwo.toMemberIdentity())
        memberFutures.complete(errorsMap)
        assertFalse(memberFutures.isCompletedExceptionally)
        val missingMemberResult = RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove)
        assertFutureError(missingMemberResult.all(), IllegalArgumentException::class.java)
        assertNotFails { (missingMemberResult.memberResult(instanceOne).get()) }
        assertFutureError(missingMemberResult.memberResult(instanceTwo), IllegalArgumentException::class.java)
    }

    @Test
    @Throws(InterruptedException::class, ExecutionException::class)
    fun testMemberLevelErrorInResponseConstructor() {
        val memberLevelErrorResult = createAndVerifyMemberLevelError()
        assertFailsWith<IllegalArgumentException> {
            memberLevelErrorResult.memberResult(MemberToRemove("invalid-instance-id"))
        }
    }

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testNoErrorConstructor() {
        val errorsMap: MutableMap<MemberIdentity, Errors> = HashMap()
        errorsMap[instanceOne.toMemberIdentity()] = Errors.NONE
        errorsMap[instanceTwo.toMemberIdentity()] = Errors.NONE
        val noErrorResult = RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove)
        memberFutures.complete(errorsMap)
        assertNotFails { noErrorResult.all().get() }
        assertNotFails { noErrorResult.memberResult(instanceOne).get() }
        assertNotFails { noErrorResult.memberResult(instanceTwo).get() }
    }

    @Throws(InterruptedException::class, ExecutionException::class)
    private fun createAndVerifyMemberLevelError(): RemoveMembersFromConsumerGroupResult {
        memberFutures.complete(errorsMap)
        assertFalse(memberFutures.isCompletedExceptionally)
        val memberLevelErrorResult = RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove)
        assertFutureError(memberLevelErrorResult.all(), FencedInstanceIdException::class.java)
        assertNotFails { (memberLevelErrorResult.memberResult(instanceOne).get()) }
        assertFutureError(
            future = memberLevelErrorResult.memberResult(instanceTwo),
            exceptionClass = FencedInstanceIdException::class.java,
        )
        return memberLevelErrorResult
    }
}
