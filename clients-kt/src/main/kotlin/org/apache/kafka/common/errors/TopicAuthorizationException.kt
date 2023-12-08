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


class TopicAuthorizationException(
    message: String? = null,
    val unauthorizedTopics: Set<String> = emptySet()
) : AuthorizationException(message) {

    constructor(unauthorizedTopics: Set<String>) : this(
        message = "Not authorized to access topics: $unauthorizedTopics",
        unauthorizedTopics = unauthorizedTopics,
    )

    /**
     * Get the set of topics which failed authorization. May be empty if the set is not known
     * in the context the exception was raised in.
     *
     * @return possibly empty set of unauthorized topics
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("unauthorizedTopics")
    )
    fun unauthorizedTopics(): Set<String> {
        return unauthorizedTopics
    }
}
