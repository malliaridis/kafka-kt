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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.message.FindCoordinatorResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class CoordinatorStrategyTest {

    @Test
    fun testBuildOldLookupRequest() {
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        strategy.disableBatch()
        val request = strategy.buildRequest(setOf(CoordinatorKey.byGroupId("foo")))
        assertEquals("foo", request.data().key)
        assertEquals(CoordinatorType.GROUP, CoordinatorType.forId(request.data().keyType))
    }

    @Test
    fun testBuildLookupRequest() {
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        val request = strategy.buildRequest(
            setOf(
                CoordinatorKey.byGroupId("foo"),
                CoordinatorKey.byGroupId("bar"),
            )
        )
        assertEquals("", request.data().key)
        assertEquals(2, request.data().coordinatorKeys.size)
        assertEquals(CoordinatorType.GROUP, CoordinatorType.forId(request.data().keyType))
    }

    @Test
    @Disabled("Kotlin Migration: A strategy request cannot be built with nullable values (probably).")
    fun testBuildLookupRequestNonRepresentable() {
//        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
//        val request = strategy.buildRequest(
//            setOf(CoordinatorKey.byGroupId("foo"), null)
//        )
//        assertEquals("", request.data().key)
//        assertEquals(1, request.data().coordinatorKeys.size)
    }

    @Test
    fun testBuildOldLookupRequestRequiresOneKey() {
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        strategy.disableBatch()
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(emptySet()) }
        val group1 = CoordinatorKey.byGroupId("foo")
        val group2 = CoordinatorKey.byGroupId("bar")
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(setOf(group1, group2)) }
    }

    @Test
    fun testBuildOldLookupRequestRequiresAtLeastOneKey() {
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        strategy.disableBatch()
        assertFailsWith<IllegalArgumentException> {
            strategy.buildRequest(setOf(CoordinatorKey.byTransactionalId("txnid")))
        }
    }

    @Test
    fun testBuildLookupRequestRequiresAtLeastOneKey() {
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        assertFailsWith<IllegalArgumentException> { strategy.buildRequest(emptySet()) }
    }

    @Test
    fun testBuildLookupRequestRequiresKeySameType() {
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        assertFailsWith<IllegalArgumentException> {
            strategy.buildRequest(
                setOf(
                    CoordinatorKey.byGroupId("group"),
                    CoordinatorKey.byTransactionalId("txnid"),
                )
            )
        }
    }

    @Test
    fun testHandleOldResponseRequiresOneKey() {
        val responseData = FindCoordinatorResponseData().setErrorCode(Errors.NONE.code)
        val response = FindCoordinatorResponse(responseData)
        val strategy = CoordinatorStrategy(CoordinatorType.GROUP, LogContext())
        strategy.disableBatch()
        assertFailsWith<IllegalArgumentException> { strategy.handleResponse(emptySet(), response) }
        val group1 = CoordinatorKey.byGroupId("foo")
        val group2 = CoordinatorKey.byGroupId("bar")
        assertFailsWith<IllegalArgumentException> { strategy.handleResponse(setOf(group1, group2), response) }
    }

    @Test
    fun testSuccessfulOldCoordinatorLookup() {
        val group = CoordinatorKey.byGroupId("foo")
        val responseData = FindCoordinatorResponseData()
            .setErrorCode(Errors.NONE.code)
            .setHost("localhost")
            .setPort(9092)
            .setNodeId(1)
        val result = runOldLookup(group, responseData)
        assertEquals(mapOf(group to 1), result.mappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
    }

    @Test
    fun testSuccessfulCoordinatorLookup() {
        val group1 = CoordinatorKey.byGroupId("foo")
        val group2 = CoordinatorKey.byGroupId("bar")
        val responseData = FindCoordinatorResponseData()
            .setCoordinators(
                listOf(
                    FindCoordinatorResponseData.Coordinator()
                        .setKey("foo")
                        .setErrorCode(Errors.NONE.code)
                        .setHost("localhost")
                        .setPort(9092)
                        .setNodeId(1),
                    FindCoordinatorResponseData.Coordinator()
                        .setKey("bar")
                        .setErrorCode(Errors.NONE.code)
                        .setHost("localhost")
                        .setPort(9092)
                        .setNodeId(2),
                )
            )
        val result = runLookup(setOf(group1, group2), responseData)
        val expectedResult = mutableMapOf<CoordinatorKey, Int>()
        expectedResult[group1] = 1
        expectedResult[group2] = 2
        assertEquals(expectedResult, result.mappedKeys)
        assertEquals(emptyMap(), result.failedKeys)
    }

    @Test
    fun testRetriableOldCoordinatorLookup() {
        testRetriableOldCoordinatorLookup(Errors.COORDINATOR_LOAD_IN_PROGRESS)
        testRetriableOldCoordinatorLookup(Errors.COORDINATOR_NOT_AVAILABLE)
    }

    private fun testRetriableOldCoordinatorLookup(error: Errors) {
        val group = CoordinatorKey.byGroupId("foo")
        val responseData = FindCoordinatorResponseData().setErrorCode(error.code)
        val result = runOldLookup(group, responseData)
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(emptyMap(), result.mappedKeys)
    }

    @Test
    fun testRetriableCoordinatorLookup() {
        testRetriableCoordinatorLookup(Errors.COORDINATOR_LOAD_IN_PROGRESS)
        testRetriableCoordinatorLookup(Errors.COORDINATOR_NOT_AVAILABLE)
    }

    private fun testRetriableCoordinatorLookup(error: Errors) {
        val group1 = CoordinatorKey.byGroupId("foo")
        val group2 = CoordinatorKey.byGroupId("bar")
        val responseData = FindCoordinatorResponseData()
            .setCoordinators(
                listOf(
                    FindCoordinatorResponseData.Coordinator()
                        .setKey("foo")
                        .setErrorCode(error.code),
                    FindCoordinatorResponseData.Coordinator()
                        .setKey("bar")
                        .setErrorCode(Errors.NONE.code)
                        .setHost("localhost")
                        .setPort(9092)
                        .setNodeId(2),
                )
            )
        val result = runLookup(setOf(group1, group2), responseData)
        assertEquals(emptyMap(), result.failedKeys)
        assertEquals(mapOf(group2 to 2), result.mappedKeys)
    }

    @Test
    fun testFatalErrorOldLookupResponses() {
        val group = CoordinatorKey.byTransactionalId("foo")
        assertFatalOldLookup(group, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        assertFatalOldLookup(group, Errors.UNKNOWN_SERVER_ERROR)
        val throwable = assertFatalOldLookup(group, Errors.GROUP_AUTHORIZATION_FAILED)
        assertTrue(throwable is GroupAuthorizationException)
        val exception = throwable as GroupAuthorizationException?
        assertEquals("foo", exception!!.groupId)
    }

    fun assertFatalOldLookup(
        key: CoordinatorKey,
        error: Errors,
    ): Throwable? {
        val responseData = FindCoordinatorResponseData().setErrorCode(error.code)
        val result = runOldLookup(key, responseData)
        assertEquals(emptyMap(), result.mappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        val throwable = result.failedKeys[key]
        assertTrue(error.exception!!.javaClass.isInstance(throwable))
        return throwable
    }

    @Test
    fun testFatalErrorLookupResponses() {
        val group = CoordinatorKey.byTransactionalId("foo")
        assertFatalLookup(group, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        assertFatalLookup(group, Errors.UNKNOWN_SERVER_ERROR)
        val throwable = assertFatalLookup(group, Errors.GROUP_AUTHORIZATION_FAILED)
        assertIs<GroupAuthorizationException>(throwable)
        val exception = throwable as GroupAuthorizationException?
        assertEquals("foo", exception!!.groupId)
    }

    fun assertFatalLookup(
        key: CoordinatorKey,
        error: Errors,
    ): Throwable? {
        val responseData = FindCoordinatorResponseData()
            .setCoordinators(
                listOf(
                    FindCoordinatorResponseData.Coordinator()
                        .setKey(key.idValue)
                        .setErrorCode(error.code)
                )
            )
        val result = runLookup(setOf(key), responseData)
        assertEquals(emptyMap(), result.mappedKeys)
        assertEquals(setOf(key), result.failedKeys.keys)
        val throwable = result.failedKeys[key]
        assertTrue(error.exception!!.javaClass.isInstance(throwable))
        return throwable
    }

    private fun runOldLookup(
        key: CoordinatorKey,
        responseData: FindCoordinatorResponseData,
    ): AdminApiLookupStrategy.LookupResult<CoordinatorKey> {
        val strategy = CoordinatorStrategy(key.type, LogContext())
        strategy.disableBatch()
        val response = FindCoordinatorResponse(responseData)
        return strategy.handleResponse(setOf(key), response)
    }

    private fun runLookup(
        keys: Set<CoordinatorKey>,
        responseData: FindCoordinatorResponseData,
    ): AdminApiLookupStrategy.LookupResult<CoordinatorKey> {
        val strategy = CoordinatorStrategy(keys.first().type, LogContext())
        strategy.buildRequest(keys)
        val response = FindCoordinatorResponse(responseData)
        return strategy.handleResponse(keys, response)
    }
}
