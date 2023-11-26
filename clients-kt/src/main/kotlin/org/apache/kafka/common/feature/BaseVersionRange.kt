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

package org.apache.kafka.common.feature

import java.lang.IllegalArgumentException
import org.apache.kafka.common.utils.Utils.mkEntry
import org.apache.kafka.common.utils.Utils.mkMap

/**
 * Represents an immutable basic version range using 2 attributes: min and max, each of type short.
 * The min and max attributes need to satisfy 2 rules:
 * - they are each expected to be >= 1, as we only consider positive version values to be valid.
 * - max should be >= min.
 *
 * The class also provides API to convert the version range to a map.
 * The class allows for configurable labels for the min/max attributes, which can be specialized by
 * sub-classes (if needed).
 *
 * @param minKeyLabel Label for the min version key, that's used only to convert to/from a map.
 * @param minValue The minimum version value.
 * @param maxKeyLabel Label for the max version key, that's used only to convert to/from a map.
 * @param maxValue The maximum version value.
 * @throws IllegalArgumentException If any of the following conditions are true:
 * - (minValue < 1) OR (maxValue < 1) OR (maxValue < minValue).
 * - minKeyLabel is empty, OR, minKeyLabel is empty.
 */
open class BaseVersionRange protected constructor(
    private val minKeyLabel: String,
    private val minValue: Short,
    private val maxKeyLabel: String,
    private val maxValue: Short
) {

    /**
     * Raises an exception unless the following condition is met:
     * minValue >= 1 and maxValue >= 1 and maxValue >= minValue.
     */
    init {
        require(!(minValue < 1 || maxValue < 1 || maxValue < minValue)) {
            String.format(
                "Expected minValue >= 1, maxValue >= 1 and maxValue >= minValue, but received" +
                        " minValue: %d, maxValue: %d", minValue, maxValue
            )
        }
        require(minKeyLabel.isNotEmpty()) { "Expected minKeyLabel to be non-empty." }
        require(maxKeyLabel.isNotEmpty()) { "Expected maxKeyLabel to be non-empty." }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("min")
    )
    fun min(): Short = minValue

    val min: Short
        get() = minValue

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("max")
    )
    fun max(): Short = maxValue

    val max: Short
        get() = maxValue

    override fun toString(): String {
        return String.format("%s[%s]", this.javaClass.simpleName, mapToString(toMap()))
    }

    fun toMap(): Map<String, Short> {
        return mkMap(mkEntry(minKeyLabel, min()), mkEntry(maxKeyLabel, max()))
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as BaseVersionRange
        return minKeyLabel == that.minKeyLabel
                && minValue == that.minValue
                && maxKeyLabel == that.maxKeyLabel
                && maxValue == that.maxValue
    }

    override fun hashCode(): Int {
        var result = minKeyLabel.hashCode()
        result = 31 * result + minValue
        result = 31 * result + maxKeyLabel.hashCode()
        result = 31 * result + maxValue
        return result
    }

    companion object {

        private fun mapToString(map: Map<String, Short>): String {
            return map.map { (key, value): Map.Entry<String, Short> -> "$key:$value" }
                .joinToString(", ")
        }

        fun valueOrThrow(
            key: String,
            versionRangeMap: Map<String, Short>
        ): Short {
            return requireNotNull(versionRangeMap[key]) {
                "$key absent in [${mapToString(versionRangeMap)}]"
            }
        }
    }
}
