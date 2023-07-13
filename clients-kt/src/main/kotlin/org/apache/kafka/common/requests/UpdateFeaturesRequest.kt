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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import java.util.stream.Collectors
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKey
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor

class UpdateFeaturesRequest(
    private val data: UpdateFeaturesRequestData, version: Short,
) : AbstractRequest(ApiKeys.UPDATE_FEATURES, version) {

    fun featureUpdates(): Collection<FeatureUpdateItem> =
        data.featureUpdates().map { update -> getFeature(update.feature()) }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): UpdateFeaturesResponse {
        return UpdateFeaturesResponse.createWithErrors(
            topLevelError = ApiError.fromThrowable(e),
            updateErrors = emptyMap(),
            throttleTimeMs = throttleTimeMs
        )
    }

    override fun data(): UpdateFeaturesRequestData = data

    data class FeatureUpdateItem(
        val featureName: String,
        val featureLevel: Short,
        val upgradeType: UpgradeType,
    ) {
        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("featureName")
        )
        fun feature(): String = featureName

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("featureLevel")
        )
        fun versionLevel(): Short = featureLevel

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("upgradeType")
        )
        fun upgradeType(): UpgradeType = upgradeType

        val isDeleteRequest: Boolean
            get() = featureLevel < 1 && upgradeType != UpgradeType.UPGRADE
    }

    class Builder(
        private val data: UpdateFeaturesRequestData,
    ) : AbstractRequest.Builder<UpdateFeaturesRequest>(ApiKeys.UPDATE_FEATURES) {

        override fun build(version: Short): UpdateFeaturesRequest =
            UpdateFeaturesRequest(data, version)

        override fun toString(): String = data.toString()
    }

    fun getFeature(name: String): FeatureUpdateItem {
        val update = data.featureUpdates().find(name)!!

        return if (super.version.toInt() == 0) {
            if (update.allowDowngrade()) FeatureUpdateItem(
                featureName = update.feature(),
                featureLevel = update.maxVersionLevel(),
                upgradeType = UpgradeType.SAFE_DOWNGRADE,
            )
            else FeatureUpdateItem(
                featureName = update.feature(),
                featureLevel = update.maxVersionLevel(),
                upgradeType = UpgradeType.UPGRADE,
            )
        } else FeatureUpdateItem(
            featureName = update.feature(),
            featureLevel = update.maxVersionLevel(),
            upgradeType = UpgradeType.fromCode(update.upgradeType().toInt())
        )
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): UpdateFeaturesRequest =
            UpdateFeaturesRequest(
                UpdateFeaturesRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
