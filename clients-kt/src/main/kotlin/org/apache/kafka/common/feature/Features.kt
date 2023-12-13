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
 * Represents an immutable dictionary with key being feature name, and value being [VersionRangeType].
 * Also provides API to convert the features and their version ranges to/from a map.
 *
 * This class can be instantiated only using its factory functions, with the important ones being:
 * `Features.supportedFeatures(...)` and `Features.finalizedFeatures(...)`.
 *
 * Constructor is made private, as for readability it is preferred the caller uses one of the
 * static factory functions for instantiation (see below).
 *
 * @property features Map of feature name to a type of VersionRange.
 * @property VersionRangeType is the type of version range.
 * @see SupportedVersionRange
 */
class Features<VersionRangeType : BaseVersionRange> private constructor(
    val features: Map<String, VersionRangeType>,
) {

    @Deprecated(
        message = "Use property insteac.",
        replaceWith = ReplaceWith("features"),
    )
    fun features(): Map<String, VersionRangeType> = features

    fun empty(): Boolean {
        return features.isEmpty()
    }

    /**
     * @param  feature name of the feature
     * @return the VersionRangeType corresponding to the feature name, or null if the feature is
     * absent
     */
    operator fun get(feature: String): VersionRangeType? {
        return features[feature]
    }

    override fun toString(): String {
        return String.format(
            "Features{%s}",
            features.map { (key, value) -> String.format("(%s -> %s)", key, value) }
                .joinToString(", ")
        )
    }

    /**
     * @return A map representation of the underlying features. The returned value can be converted
     * back to Features using one of the from*FeaturesMap() APIs of this class.
     */
    fun toMap(): Map<String, Map<String, Short>> {
        return features.mapValues { it.value.toMap() }
    }

    override fun equals(other: Any?): Boolean {
        return if (this === other) true
        else if (other !is Features<*>) false
        else features == other.features
    }

    override fun hashCode(): Int = features.hashCode()

    companion object {

        /**
         * @param features Map of feature name to SupportedVersionRange.
         *
         * @return Returns a new Features object representing supported features.
         */
        fun supportedFeatures(
            features: Map<String, SupportedVersionRange>,
        ): Features<SupportedVersionRange> = Features(features)

        fun emptySupportedFeatures(): Features<SupportedVersionRange> {
            return Features(emptyMap())
        }

        private fun <V : BaseVersionRange> fromFeaturesMap(
            featuresMap: Map<String, Map<String, Short>>,
            converter: (Map<String, Short>) -> V
        ): Features<V> {
            return Features(featuresMap.mapValues { converter.invoke(it.value) })
        }

        /**
         * Converts from a map to [Features].
         *
         * @param featuresMap the map representation of a [Features] object,
         * generated using the [toMap] API.
         *
         * @return the [Features] object
         */
        fun fromSupportedFeaturesMap(
            featuresMap: Map<String, Map<String, Short>>
        ): Features<SupportedVersionRange> {
            return fromFeaturesMap(
                featuresMap = featuresMap,
                converter = { SupportedVersionRange.fromMap(it) }
            )
        }
    }
}
