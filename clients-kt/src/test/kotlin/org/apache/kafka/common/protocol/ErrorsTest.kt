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

package org.apache.kafka.common.protocol

import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.TimeoutException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

class ErrorsTest {
    
    @Test
    fun testUniqueErrorCodes() {
        val codeSet = Errors.values().map { error -> error.code }
        assertEquals(codeSet.size, Errors.values().size, "Error codes must be unique")
    }

    @Test
    fun testUniqueExceptions() {
        val exceptionSet = Errors.values()
            .filter { error -> error != Errors.NONE }
            .map { error -> error.exception!!.javaClass }
            .toSet()
        assertEquals(
            expected = exceptionSet.size,
            actual = Errors.values().size - 1,
            message = "Exceptions must be unique",
        ) // Ignore NONE
    }

    @Test
    fun testExceptionsAreNotGeneric() {
        for (error in Errors.values()) {
            if (error != Errors.NONE) assertNotEquals(
                illegal = error.exception!!.javaClass,
                actual = ApiException::class.java,
                message = "Generic ApiException should not be used",
            )
        }
    }

    @Test
    fun testNoneException() {
        assertNull(Errors.NONE.exception, "The NONE error should not have an exception")
    }

    @Test
    fun testForExceptionInheritance() {
        class ExtendedTimeoutException : TimeoutException()

        val expectedError = Errors.forException(TimeoutException())
        val actualError = Errors.forException(ExtendedTimeoutException())
        assertEquals(expectedError, actualError, "forException should match super classes")
    }

    @Test
    fun testForExceptionDefault() {
        val error = Errors.forException(ApiException())
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, error, "forException should default to unknown")
    }

    @Test
    fun testExceptionName() {
        var exceptionName = Errors.UNKNOWN_SERVER_ERROR.exceptionName
        assertEquals("org.apache.kafka.common.errors.UnknownServerException", exceptionName)
        exceptionName = Errors.NONE.exceptionName
        assertNull(exceptionName)
        exceptionName = Errors.INVALID_TOPIC_EXCEPTION.exceptionName
        assertEquals("org.apache.kafka.common.errors.InvalidTopicException", exceptionName)
    }
}
