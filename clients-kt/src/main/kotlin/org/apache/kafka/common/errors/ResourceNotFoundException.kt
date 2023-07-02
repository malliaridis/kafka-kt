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

package org.apache.kafka.common.errors

/**
 * Exception thrown due to a request for a resource that does not exist.
 *
 * @property resource the (potentially null) resource that was not found
 * @property message the exception's message
 * @property cause the exception's cause
 */
class ResourceNotFoundException(
    val resource: String? = null,
    message: String? = null,
    cause: Throwable? = null,
) : ApiException(message = message, cause = cause) {

    /**
     *
     * @return the (potentially null) resource that was not found
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("resource"),
    )
    fun resource(): String? {
        return resource
    }

    companion object {
        private const val serialVersionUID = 1L
    }
}