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

import java.util.*
import kotlin.collections.HashMap

/**
 * Encapsulates details about finalized as well as supported features. This is particularly useful
 * to hold the result returned by the [Admin.describeFeatures] API.
 */
data class FeatureMetadata(
    private val finalizedFeatures: Map<String, FinalizedVersionRange> = HashMap(),
    val finalizedFeaturesEpoch: Long? = null,
    val supportedFeatures: Map<String, SupportedVersionRange> = HashMap(),
) {

    constructor(
        finalizedFeatures: Map<String, FinalizedVersionRange>? = null,
        finalizedFeaturesEpoch: Long? = null,
        supportedFeatures: Map<String, SupportedVersionRange>? = null,
    ) : this(
        finalizedFeatures = finalizedFeatures ?: HashMap(),
        finalizedFeaturesEpoch = finalizedFeaturesEpoch,
        supportedFeatures = supportedFeatures ?: HashMap(),
    )

    /**
     * Returns a map of finalized feature versions. Each entry in the map contains a key being a
     * feature name and the value being a range of version levels supported by every broker in the
     * cluster.
     */
    fun finalizedFeatures(): Map<String, FinalizedVersionRange> {
        return HashMap(finalizedFeatures)
    }

    /**
     * The epoch for the finalized features.
     * If the returned value is empty, it means the finalized features are absent/unavailable.
     */
    @Deprecated(
        message = "Deprecated in Kotlin, use value instead.",
        replaceWith = ReplaceWith("finalizedFeaturesEpoch"),
    )
    fun finalizedFeaturesEpoch(): Long? {
        return finalizedFeaturesEpoch
    }

    /**
     * Returns a map of supported feature versions. Each entry in the map contains a key being a
     * feature name and the value being a range of versions supported by a particular broker in the
     * cluster.
     */
    fun supportedFeatures(): Map<String, SupportedVersionRange> {
        return HashMap(supportedFeatures)
    }

    override fun toString(): String {
        return String.format(
            Locale.getDefault(),
            "FeatureMetadata{finalizedFeatures:%s, finalizedFeaturesEpoch:%s, supportedFeatures:%s}",
            mapToString(finalizedFeatures),
            finalizedFeaturesEpoch ?: "<none>",
            mapToString(supportedFeatures)
        )
    }

    companion object {
        private fun <ValueType> mapToString(featureVersionsMap: Map<String, ValueType>): String {
            return String.format(
                Locale.getDefault(),
                "{%s}",
                featureVersionsMap.map { (key, value) ->
                    String.format(
                        Locale.getDefault(),
                        "(%s -> %s)",
                        key,
                        value
                    )
                },
            )
        }
    }
}
