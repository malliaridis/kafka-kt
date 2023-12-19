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

package org.apache.kafka.server.common

import org.apache.kafka.server.common.MetadataVersion.Companion.FEATURE_NAME

class Features(
    private val version: MetadataVersion,
    finalizedFeatures: Map<String, Short>,
    val finalizedFeaturesEpoch: Long,
    kraftMode: Boolean,
) {

    val finalizedFeatures: Map<String, Short>

    init {
        this.finalizedFeatures = HashMap(finalizedFeatures)
        // In KRaft mode, we always include the metadata version in the features map.
        // In ZK mode, we never include it.
        if (kraftMode) this.finalizedFeatures[FEATURE_NAME] = version.featureLevel
        else this.finalizedFeatures.remove(FEATURE_NAME)
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("metadataVersion")
    )
    fun metadataVersion(): MetadataVersion = version

    val metadataVersion: MetadataVersion
        get() = version

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("finalizedFeatures")
    )
    fun finalizedFeatures(): Map<String, Short> = finalizedFeatures

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("finalizedFeaturesEpoch")
    )
    fun finalizedFeaturesEpoch(): Long = finalizedFeaturesEpoch

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Features

        if (version != other.version) return false
        if (finalizedFeaturesEpoch != other.finalizedFeaturesEpoch) return false
        if (finalizedFeatures != other.finalizedFeatures) return false

        return true
    }

    override fun hashCode(): Int {
        var result = version.hashCode()
        result = 31 * result + finalizedFeaturesEpoch.hashCode()
        result = 31 * result + finalizedFeatures.hashCode()
        return result
    }

    override fun toString(): String = "Features" +
            "(version=$version" +
            ", finalizedFeatures=$finalizedFeatures" +
            ", finalizedFeaturesEpoch=$finalizedFeaturesEpoch" +
            ")"

    companion object {

        fun fromKRaftVersion(version: MetadataVersion): Features = Features(
            version = version,
            finalizedFeatures = emptyMap(),
            finalizedFeaturesEpoch = -1,
            kraftMode = true
        )
    }
}
