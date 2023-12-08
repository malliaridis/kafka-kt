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

import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.fail

class DescribeUserScramCredentialsResultTest {
    
    @Test
    fun testTopLevelError() {
        val dataFuture = KafkaFutureImpl<DescribeUserScramCredentialsResponseData>()
        dataFuture.completeExceptionally(RuntimeException())
        val results = DescribeUserScramCredentialsResult(dataFuture)
        try {
            results.all().get()
            fail("expected all() to fail when there is a top-level error")
        } catch (expected: Exception) {
            // ignore, expected
        }
        try {
            results.users().get()
            fail("expected users() to fail when there is a top-level error")
        } catch (expected: Exception) {
            // ignore, expected
        }
        try {
            results.description("whatever").get()
            fail("expected description() to fail when there is a top-level error")
        } catch (expected: Exception) {
            // ignore, expected
        }
    }

    @Test
    @Throws(Exception::class)
    fun testUserLevelErrors() {
        val goodUser = "goodUser"
        val unknownUser = "unknownUser"
        val failedUser = "failedUser"
        val dataFuture = KafkaFutureImpl<DescribeUserScramCredentialsResponseData>()
        val scramSha256 = ScramMechanism.SCRAM_SHA_256
        val iterations = 4096
        dataFuture.complete(
            DescribeUserScramCredentialsResponseData().setErrorCode(Errors.NONE.code).setResults(
                listOf(
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                        .setUser(goodUser)
                        .setCredentialInfos(
                            listOf(CredentialInfo().setMechanism(scramSha256.type).setIterations(iterations))
                        ),
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser(unknownUser)
                        .setErrorCode(Errors.RESOURCE_NOT_FOUND.code),
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser(failedUser)
                        .setErrorCode(Errors.DUPLICATE_RESOURCE.code)
                )
            )
        )
        val results = DescribeUserScramCredentialsResult(dataFuture)
        try {
            results.all().get()
            fail("expected all() to fail when there is a user-level error")
        } catch (expected: Exception) {
            // ignore, expected
        }
        assertEquals(
            expected = listOf(goodUser, failedUser),
            actual = results.users().get(),
            message = "Expected 2 users with credentials",
        )
        val goodUserDescription = results.description(goodUser).get()
        assertEquals(
            expected = UserScramCredentialsDescription(
                name = goodUser,
                credentialInfos = listOf(ScramCredentialInfo(scramSha256, iterations)),
            ),
            actual = goodUserDescription,
        )
        try {
            results.description(failedUser).get()
            fail("expected description(failedUser) to fail when there is a user-level error")
        } catch (expected: Exception) {
            // ignore, expected
        }
        try {
            results.description(unknownUser).get()
            fail("expected description(unknownUser) to fail when there is no such user")
        } catch (expected: Exception) {
            // ignore, expected
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSuccessfulDescription() {
        val goodUser = "goodUser"
        val unknownUser = "unknownUser"
        val dataFuture = KafkaFutureImpl<DescribeUserScramCredentialsResponseData>()
        val scramSha256 = ScramMechanism.SCRAM_SHA_256
        val iterations = 4096
        dataFuture.complete(
            DescribeUserScramCredentialsResponseData()
                .setErrorCode(Errors.NONE.code)
                .setResults(
                    listOf(
                        DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                            .setUser(goodUser)
                            .setCredentialInfos(
                                listOf(CredentialInfo().setMechanism(scramSha256.type).setIterations(iterations))
                            ),
                    )
                )
        )
        val results = DescribeUserScramCredentialsResult(dataFuture)
        assertEquals(
            expected = listOf(goodUser),
            actual = results.users().get(),
            message = "Expected 1 user with credentials",
        )
        val allResults = results.all().get()
        assertEquals(expected = 1, actual = allResults.size)
        val goodUserDescriptionViaAll = allResults[goodUser]
        assertEquals(
            expected = UserScramCredentialsDescription(
                name = goodUser,
                credentialInfos = listOf(ScramCredentialInfo(scramSha256, iterations)),
            ),
            actual = goodUserDescriptionViaAll,
        )
        assertEquals(
            expected = goodUserDescriptionViaAll,
            actual = results.description(goodUser).get(),
            message = "Expected same thing via all() and description()",
        )
        try {
            results.description(unknownUser).get()
            fail("expected description(unknownUser) to fail when there is no such user even when all() succeeds")
        } catch (expected: Exception) {
            // ignore, expected
        }
    }
}
