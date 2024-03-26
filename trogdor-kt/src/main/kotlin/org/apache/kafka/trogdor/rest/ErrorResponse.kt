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

package org.apache.kafka.trogdor.rest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.Objects
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString

/**
 * An error response.
 */
class ErrorResponse @JsonCreator constructor(
    @param:JsonProperty("code") private val code: Int,
    @param:JsonProperty("message") private val message: String?,
) {
    @JsonProperty
    fun code(): Int = code

    @JsonProperty
    fun message(): String? = message

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as ErrorResponse
        return code == that.code && message == that.message
    }

    override fun hashCode(): Int = Objects.hash(code, message)

    override fun toString(): String = toJsonString(this)
}
