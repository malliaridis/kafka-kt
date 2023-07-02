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
import java.util.*
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.feature.SupportedVersionRange
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKeyCollection
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKeyCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordVersion

/**
 * Possible error codes:
 * - [Errors.UNSUPPORTED_VERSION]
 * - [Errors.INVALID_REQUEST]
 */
class ApiVersionsResponse(
    private val data: ApiVersionsResponseData,
) : AbstractResponse(ApiKeys.API_VERSIONS) {

    override fun data(): ApiVersionsResponseData = data

    fun apiVersion(apiKey: Short): ApiVersionsResponseData.ApiVersion? = data.apiKeys().find(apiKey)

    override fun errorCounts(): Map<Errors, Int> =
        errorCounts(Errors.forCode(data.errorCode()))

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 2

    fun zkMigrationReady(): Boolean = data.zkMigrationReady()

    @Suppress("TooManyFunctions")
    companion object {

        const val UNKNOWN_FINALIZED_FEATURES_EPOCH = -1L

        fun parse(buffer: ByteBuffer, version: Short): ApiVersionsResponse {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
            // falls back while parsing it which means that the version received by this
            // method is not necessarily the real one. It may be version 0 as well.
            val prev = buffer.position()

            return try {
                ApiVersionsResponse(ApiVersionsResponseData(ByteBufferAccessor(buffer), version))
            } catch (e: RuntimeException) {
                buffer.position(prev)
                if (version.toInt() != 0) ApiVersionsResponse(
                    ApiVersionsResponseData(ByteBufferAccessor(buffer), 0.toShort())
                ) else throw e
            }
        }

        fun defaultApiVersionsResponse(
            throttleTimeMs: Int = 0,
            listenerType: ListenerType
        ): ApiVersionsResponse {
            return createApiVersionsResponse(
                throttleTimeMs,
                filterApis(RecordVersion.current(), listenerType),
                Features.emptySupportedFeatures()
            )
        }

        fun createApiVersionsResponse(
            throttleTimeMs: Int,
            minRecordVersion: RecordVersion,
            latestSupportedFeatures: Features<SupportedVersionRange>,
            finalizedFeatures: Map<String?, Short>,
            finalizedFeaturesEpoch: Long,
            controllerApiVersions: NodeApiVersions?,
            listenerType: ListenerType,
        ): ApiVersionsResponse {
            val apiKeys = if (controllerApiVersions != null) intersectForwardableApis(
                listenerType, minRecordVersion, controllerApiVersions.supportedVersions
            )
            else filterApis(minRecordVersion, listenerType)

            return createApiVersionsResponse(
                throttleTimeMs,
                apiKeys,
                latestSupportedFeatures,
                finalizedFeatures,
                finalizedFeaturesEpoch
            )
        }

        fun createApiVersionsResponse(
            throttleTimeMs: Int,
            apiVersions: ApiVersionCollection,
            latestSupportedFeatures: Features<SupportedVersionRange> = Features.emptySupportedFeatures(),
            finalizedFeatures: Map<String?, Short> = emptyMap(),
            finalizedFeaturesEpoch: Long = UNKNOWN_FINALIZED_FEATURES_EPOCH
        ): ApiVersionsResponse {
            return ApiVersionsResponse(
                ApiVersionsResponseData().apply {
                    setThrottleTimeMs(throttleTimeMs)
                    setErrorCode(Errors.NONE.code)
                    setApiKeys(apiVersions)
                    setSupportedFeatures(createSupportedFeatureKeys(latestSupportedFeatures))
                    setFinalizedFeatures(createFinalizedFeatureKeys(finalizedFeatures))
                    setFinalizedFeaturesEpoch(finalizedFeaturesEpoch)
                }
            )
        }

        fun filterApis(
            minRecordVersion: RecordVersion,
            listenerType: ListenerType,
        ): ApiVersionCollection {
            val apiKeys = ApiVersionCollection()

            ApiKeys.apisForListener(listenerType).forEach { apiKey ->
                if (apiKey.minRequiredInterBrokerMagic <= minRecordVersion.value)
                    apiKeys.add(toApiVersion(apiKey))
            }

            return apiKeys
        }

        fun collectApis(apiKeys: Set<ApiKeys>): ApiVersionCollection {
            val res = ApiVersionCollection()
            apiKeys.forEach { res.add(toApiVersion(it)) }

            return res
        }

        /**
         * Find the common range of supported API versions between the locally
         * known range and that of another set.
         *
         * @param listenerType the listener type which constrains the set of exposed APIs
         * @param minRecordVersion min inter broker magic
         * @param activeControllerApiVersions controller ApiVersions
         * @return commonly agreed ApiVersion collection
         */
        fun intersectForwardableApis(
            listenerType: ListenerType,
            minRecordVersion: RecordVersion,
            activeControllerApiVersions: Map<ApiKeys, ApiVersionsResponseData.ApiVersion>
        ): ApiVersionCollection {
            val apiKeys = ApiVersionCollection()

            ApiKeys.apisForListener(listenerType).forEach { apiKey ->

                if (apiKey.minRequiredInterBrokerMagic <= minRecordVersion.value) {
                    val brokerApiVersion = toApiVersion(apiKey)

                    val finalApiVersion = if (!apiKey.forwardable) brokerApiVersion else {
                        val intersectVersion = intersect(
                            brokerApiVersion,
                            activeControllerApiVersions.getOrDefault(apiKey, null)
                        )
                        // If controller doesn't support this API key, or there is no intersection, skip
                        intersectVersion ?: return@forEach
                    }

                    apiKeys.add(finalApiVersion.duplicate())
                }
            }
            return apiKeys
        }

        private fun createSupportedFeatureKeys(
            latestSupportedFeatures: Features<SupportedVersionRange>,
        ): SupportedFeatureKeyCollection {
            val converted = SupportedFeatureKeyCollection()

            latestSupportedFeatures.features.forEach { (key, versionRange) ->
                converted.add(
                    SupportedFeatureKey()
                        .setName(key)
                        .setMinVersion(versionRange.min)
                        .setMaxVersion(versionRange.max)
                )
            }
            return converted
        }

        private fun createFinalizedFeatureKeys(
            finalizedFeatures: Map<String?, Short>
        ): FinalizedFeatureKeyCollection {
            val converted = FinalizedFeatureKeyCollection()

            finalizedFeatures.forEach { (key, versionLevel) ->
                converted.add(
                    FinalizedFeatureKey()
                        .setName(key)
                        .setMinVersionLevel(versionLevel)
                        .setMaxVersionLevel(versionLevel)
                )
            }
            return converted
        }

        fun intersect(
            thisVersion: ApiVersionsResponseData.ApiVersion?,
            other: ApiVersionsResponseData.ApiVersion?
        ): ApiVersionsResponseData.ApiVersion? {
            if (thisVersion == null || other == null) return null

            require(thisVersion.apiKey() == other.apiKey()) {
                "thisVersion.apiKey: ${thisVersion.apiKey()}" +
                        " must be equal to other.apiKey: ${other.apiKey()}"
            }

            val minVersion = thisVersion.minVersion().coerceAtLeast(other.minVersion())
            val maxVersion = thisVersion.maxVersion().coerceAtMost(other.maxVersion())

            return if (minVersion > maxVersion) null
            else ApiVersionsResponseData.ApiVersion()
                .setApiKey(thisVersion.apiKey())
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion)
        }

        fun toApiVersion(apiKey: ApiKeys): ApiVersionsResponseData.ApiVersion {
            return ApiVersionsResponseData.ApiVersion()
                .setApiKey(apiKey.id)
                .setMinVersion(apiKey.oldestVersion())
                .setMaxVersion(apiKey.latestVersion())
        }
    }
}
