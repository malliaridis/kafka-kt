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
 * Represents a range of version levels supported by every broker in a cluster for some feature.
 *
 * @constructor Raises an exception unless the following condition is met:
 * ```
 * minVersionLevel >= 1 and maxVersionLevel >= 1 and maxVersionLevel >= minVersionLevel.
 * ```
 *
 * @property minVersionLevel The minimum version level value.
 * @property maxVersionLevel The maximum version level value.
 * @throws IllegalArgumentException Raised when the condition described above is not met.
 */
data class FinalizedVersionRange internal constructor(
    private val minVersionLevel: Short,
    private val maxVersionLevel: Short,
) {

    init {
        require(minVersionLevel >= 0 && maxVersionLevel >= 0 && maxVersionLevel >= minVersionLevel) {
            String.format(
                "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and" +
                        " maxVersionLevel >= minVersionLevel, but received" +
                        " minVersionLevel: %d, maxVersionLevel: %d",
                minVersionLevel,
                maxVersionLevel,
            )
        }
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("minVersionLevel"),
    )
    fun minVersionLevel(): Short = minVersionLevel

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("maxVersionLevel"),
    )
    fun maxVersionLevel(): Short = maxVersionLevel

    override fun toString(): String =
        "FinalizedVersionRange[min_version_level:$minVersionLevel, " +
                "max_version_level:$maxVersionLevel]"
}
