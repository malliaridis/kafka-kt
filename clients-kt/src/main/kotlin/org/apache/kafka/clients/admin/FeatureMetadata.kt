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
 * Encapsulates details about finalized as well as supported features. This is particularly useful
 * to hold the result returned by the [Admin.describeFeatures] API.
 */
class FeatureMetadata(
    finalizedFeatures: Map<String, FinalizedVersionRange> = emptyMap(),
    finalizedFeaturesEpoch: Long? = null,
    supportedFeatures: Map<String, SupportedVersionRange> = emptyMap(),
) {

    private val finalizedFeatures: Map<String, FinalizedVersionRange>

    val finalizedFeaturesEpoch: Long?

    private val supportedFeatures: Map<String, SupportedVersionRange>

    init {
        this.finalizedFeatures = finalizedFeatures.toMap()
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch
        this.supportedFeatures = supportedFeatures.toMap()
    }

    /**
     * Returns a map of finalized feature versions. Each entry in the map contains a key being a
     * feature name and the value being a range of version levels supported by every broker in the
     * cluster.
     */
    fun finalizedFeatures(): Map<String, FinalizedVersionRange> = finalizedFeatures.toMap()

    /**
     * The epoch for the finalized features.
     * If the returned value is empty, it means the finalized features are absent/unavailable.
     */
    @Deprecated(
        message = "Deprecated in Kotlin, use value instead.",
        replaceWith = ReplaceWith("finalizedFeaturesEpoch"),
    )
    fun finalizedFeaturesEpoch(): Long? = finalizedFeaturesEpoch

    /**
     * Returns a map of supported feature versions. Each entry in the map contains a key being a
     * feature name and the value being a range of versions supported by a particular broker in the
     * cluster.
     */
    fun supportedFeatures(): Map<String, SupportedVersionRange> = supportedFeatures.toMap()

    override fun toString(): String {
        return String.format(
            "FeatureMetadata{finalizedFeatures:%s, finalizedFeaturesEpoch:%s, supportedFeatures:%s}",
            mapToString(finalizedFeatures),
            finalizedFeaturesEpoch ?: "<none>",
            mapToString(supportedFeatures)
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as FeatureMetadata

        if (finalizedFeatures != other.finalizedFeatures) return false
        if (finalizedFeaturesEpoch != other.finalizedFeaturesEpoch) return false
        if (supportedFeatures != other.supportedFeatures) return false

        return true
    }

    override fun hashCode(): Int {
        var result = finalizedFeatures.hashCode()
        result = 31 * result + (finalizedFeaturesEpoch?.hashCode() ?: 0)
        result = 31 * result + supportedFeatures.hashCode()
        return result
    }

    companion object {
        private fun <ValueType> mapToString(featureVersionsMap: Map<String, ValueType>): String =
            "{${featureVersionsMap.map { (key, value) -> "($key -> $value)" }}}"
    }
}
