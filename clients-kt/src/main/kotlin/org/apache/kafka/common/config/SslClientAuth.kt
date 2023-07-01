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

package org.apache.kafka.common.config

/**
 * Describes whether the server should require or request client authentication.
 */
enum class SslClientAuth {

    REQUIRED,

    REQUESTED,

    NONE;

    override fun toString(): String = super.toString().lowercase()

    companion object {

        @Deprecated(
            message = "Use values() instead",
            replaceWith = ReplaceWith("values()"),
        )
        val VALUES = values()

        fun forConfig(key: String?): SslClientAuth? {
            if (key == null) return NONE

            val upperCaseKey = key.uppercase()
            return values().firstOrNull { it.name == upperCaseKey }
        }
    }
}