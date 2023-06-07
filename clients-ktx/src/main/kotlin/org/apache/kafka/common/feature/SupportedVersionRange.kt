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

/**
 * An extended [BaseVersionRange] representing the min/max versions for a supported feature.
 */
class SupportedVersionRange(
    minVersion: Short = 1.toShort(),
    maxVersion: Short,
) : BaseVersionRange(
    minKeyLabel = MIN_VERSION_KEY_LABEL,
    minValue = minVersion,
    maxKeyLabel = MAX_VERSION_KEY_LABEL,
    maxValue = maxVersion,
) {

    /**
     * Checks if the version level does *NOT* fall within the [min, max] range of this SupportedVersionRange.
     *
     * @param version   the version to be checked
     *
     * @return  - true, if the version levels are incompatible
     * - false otherwise
     */
    fun isIncompatibleWith(version: Short): Boolean {
        return min > version || max < version
    }

    companion object {

        // Label for the min version key, that's used only to convert to/from a map.
        private const val MIN_VERSION_KEY_LABEL = "min_version"

        // Label for the max version key, that's used only to convert to/from a map.
        private const val MAX_VERSION_KEY_LABEL = "max_version"

        fun fromMap(versionRangeMap: Map<String, Short>): SupportedVersionRange {
            return SupportedVersionRange(
                valueOrThrow(MIN_VERSION_KEY_LABEL, versionRangeMap),
                valueOrThrow(MAX_VERSION_KEY_LABEL, versionRangeMap)
            )
        }
    }
}
