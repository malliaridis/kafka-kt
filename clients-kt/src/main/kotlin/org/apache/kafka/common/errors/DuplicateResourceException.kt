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
 * Exception thrown due to a request that illegally refers to the same resource twice
 * (for example, trying to both create and delete the same SCRAM credential for a particular user
 * in a single request).
 *
 * @property message The exception's message
 * @property cause The exception's cause
 * @property resource The (potentially null) resource that was referred to twice
 */
class DuplicateResourceException(
    message: String? = null,
    cause: Throwable? = null,
    val resource: String? = null,
) : ApiException(message = message, cause = cause) {

    /**
     *
     * @return the (potentially null) resource that was referred to twice
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("resource"),
    )
    fun resource(): String? {
        return resource
    }

    companion object {
        private const val serialVersionUID = 1L
    }
}
