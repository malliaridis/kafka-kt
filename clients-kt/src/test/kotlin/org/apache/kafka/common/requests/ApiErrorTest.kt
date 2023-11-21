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

import java.io.IOException
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.errors.NotControllerException
import org.apache.kafka.common.errors.NotCoordinatorException
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertEquals

class ApiErrorTest {

    @ParameterizedTest
    @MethodSource("parameters")
    fun fromThrowableShouldReturnCorrectError(t: Throwable?, expectedError: Errors?, expectedMsg: String?) {
        val (error, message) = ApiError.fromThrowable(t!!)
        assertEquals(expectedError, error)
        assertEquals(expectedMsg, message)
    }

    companion object {

        private fun parameters(): Collection<Arguments> {
            val notCoordinatorErrorMsg = "Not coordinator"
            val notControllerErrorMsg = "Not controller"
            val requestTimeoutErrorMsg = "request time out"

            return listOf(
                Arguments.of(
                    UnknownServerException("Don't leak sensitive information "),
                    Errors.UNKNOWN_SERVER_ERROR,
                    null,
                ),
                Arguments.of(
                    NotEnoughReplicasException(),
                    Errors.NOT_ENOUGH_REPLICAS,
                    null,
                ),
                // avoid populating the error message if it's a generic one
                Arguments.of(
                    UnknownTopicOrPartitionException(Errors.UNKNOWN_TOPIC_OR_PARTITION.message),
                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                    null,
                ),
                Arguments.of(
                    NotCoordinatorException(notCoordinatorErrorMsg),
                    Errors.NOT_COORDINATOR,
                    notCoordinatorErrorMsg,
                ),
                // test the NotControllerException is wrapped in the CompletionException, should return correct error
                Arguments.of(
                    CompletionException(NotControllerException(notControllerErrorMsg)),
                    Errors.NOT_CONTROLLER,
                    notControllerErrorMsg,
                ),
                // test the TimeoutException is wrapped in the ExecutionException, should return correct error
                Arguments.of(
                    ExecutionException(TimeoutException(requestTimeoutErrorMsg)),
                    Errors.REQUEST_TIMED_OUT,
                    requestTimeoutErrorMsg,
                ),
                // test the exception not in the Errors list, should return UNKNOWN_SERVER_ERROR
                Arguments.of(IOException(), Errors.UNKNOWN_SERVER_ERROR, null)
            )
        }
    }
}
