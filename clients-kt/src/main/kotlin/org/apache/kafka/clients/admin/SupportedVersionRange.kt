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

/**
 * Represents a range of versions that a particular broker supports for some feature.
 *
 * @constructor Raises an exception unless the following conditions are met:
 * `1 <= minVersion <= maxVersion`.
 *
 * @property minVersion The minimum version value.
 * @property maxVersion The maximum version value.
 *
 * @throws IllegalArgumentException Raised when the condition described above is not met.
 */
data class SupportedVersionRange(
    val minVersion: Short,
    val maxVersion: Short,
) {

    init {
        require(!(minVersion < 0 || maxVersion < 0 || maxVersion < minVersion)) {
            String.format(
                "Expected 0 <= minVersion <= maxVersion but received minVersion:%d, maxVersion:%d.",
                minVersion,
                maxVersion
            )
        }
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("minVersion"),
    )
    fun minVersion(): Short = minVersion

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("maxVersion"),
    )
    fun maxVersion(): Short = maxVersion

    override fun toString(): String {
        return String.format(
            "SupportedVersionRange[min_version:%d, max_version:%d]",
            minVersion,
            maxVersion
        )
    }
}
