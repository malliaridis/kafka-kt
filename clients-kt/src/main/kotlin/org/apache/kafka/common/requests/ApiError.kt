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

import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.protocol.Errors

/**
 * Encapsulates an error code (via the Errors enum) and an optional message. Generally, the optional
 * message is only defined if it adds information over the default message associated with the error
 * code.
 *
 * This is an internal class (like every class in the requests package).
 */
data class ApiError(
    val error: Errors,
    val message: String? = error.message,
) {

    constructor(code: Short, message: String?) : this(
        error = Errors.forCode(code),
        message = message,
    )

    fun `is`(error: Errors): Boolean {
        return this.error === error
    }

    val isFailure: Boolean
        get() = !isSuccess

    val isSuccess: Boolean
        get() = `is`(Errors.NONE)

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("error")
    )
    fun error(): Errors = error

    /**
     * Return the optional error message or null. Consider using [messageWithFallback] instead.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("message")
    )
    fun message(): String? = message

    /**
     * If `message` is defined, return it. Otherwise, fallback to the default error message
     * associated with the error code.
     */
    fun messageWithFallback(): String? = message ?: error.message

    fun exception(): ApiException? = error.exception(message)

    override fun toString(): String = "ApiError(error=$error, message=$message)"

    companion object {

        val NONE = ApiError(Errors.NONE, null)

        fun fromThrowable(t: Throwable): ApiError {
            // Avoid populating the error message if it's a generic one. Also don't populate error
            // message for UNKNOWN_SERVER_ERROR to ensure we don't leak sensitive information.
            val throwableToBeEncoded = Errors.maybeUnwrapException(t)
            val error = Errors.forException(throwableToBeEncoded)
            val message =
                if (error === Errors.UNKNOWN_SERVER_ERROR || error.message == throwableToBeEncoded?.message) null
                else throwableToBeEncoded?.message
            return ApiError(error, message)
        }
    }
}
