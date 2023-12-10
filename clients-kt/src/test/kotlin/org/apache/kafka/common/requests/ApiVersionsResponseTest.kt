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

import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.feature.SupportedVersionRange
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.RecordVersion
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ApiVersionsResponseTest {

    @ParameterizedTest
    @EnumSource(ListenerType::class)
    fun shouldHaveCorrectDefaultApiVersionsResponse(scope: ListenerType) {
        val defaultResponse = TestUtils.defaultApiVersionsResponse(listenerType = scope)
        assertEquals(
            ApiKeys.apisForListener(scope).size,
            defaultResponse.data().apiKeys.size,
            "API versions for all API keys must be maintained.",
        )
        for (key in ApiKeys.apisForListener(scope)) {
            val version = defaultResponse.apiVersion(key.id)
            assertNotNull(version, "Could not find ApiVersion for API " + key.name)
            assertEquals(version.minVersion, key.oldestVersion(), "Incorrect min version for Api ${key.name}")
            assertEquals(version.maxVersion, key.latestVersion(), "Incorrect max version for Api ${key.name}")

            // Check if versions less than min version are indeed set as null, i.e., deprecated.
            for (i in 0 until version.minVersion.toInt()) {
                assertNull(
                    actual = key.messageType.requestSchemas()[i],
                    message = "Request version $i for API ${version.apiKey} must be null",
                )
                assertNull(
                    actual = key.messageType.responseSchemas()[i],
                    message = "Response version $i for API ${version.apiKey} must be null",
                )
            }

            // Check if versions between min and max versions are non null, i.e., valid.
            for (i in version.minVersion..version.maxVersion) {
                assertNotNull(
                    actual = key.messageType.requestSchemas()[i],
                    message = "Request version $i for API ${version.apiKey} must not be null"
                )
                assertNotNull(
                    actual = key.messageType.responseSchemas()[i],
                    message = "Response version $i for API ${version.apiKey} must not be null",
                )
            }
        }
        assertTrue(defaultResponse.data().supportedFeatures.isEmpty())
        assertTrue(defaultResponse.data().finalizedFeatures.isEmpty())
        assertEquals(
            expected = ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            actual = defaultResponse.data().finalizedFeaturesEpoch,
        )
    }

    @Test
    fun shouldHaveCommonlyAgreedApiVersionResponseWithControllerOnForwardableAPIs() {
        val forwardableAPIKey = ApiKeys.CREATE_ACLS
        val nonForwardableAPIKey = ApiKeys.JOIN_GROUP
        val minVersion: Short = 0
        val maxVersion: Short = 1
        val activeControllerApiVersions = mapOf(
            forwardableAPIKey to ApiVersionsResponseData.ApiVersion()
                    .setApiKey(forwardableAPIKey.id)
                    .setMinVersion(minVersion)
                    .setMaxVersion(maxVersion),
            nonForwardableAPIKey to ApiVersionsResponseData.ApiVersion()
                    .setApiKey(nonForwardableAPIKey.id)
                    .setMinVersion(minVersion)
                    .setMaxVersion(maxVersion),
        )
        val commonResponse = ApiVersionsResponse.intersectForwardableApis(
            listenerType = ListenerType.ZK_BROKER,
            minRecordVersion = RecordVersion.current(),
            activeControllerApiVersions = activeControllerApiVersions,
            enableUnstableLastVersion = true,
        )
        verifyVersions(
            forwardableAPIKey = forwardableAPIKey.id,
            minVersion = minVersion,
            maxVersion = maxVersion,
            commonResponse = commonResponse,
        )
        verifyVersions(
            forwardableAPIKey = nonForwardableAPIKey.id,
            minVersion = ApiKeys.JOIN_GROUP.oldestVersion(),
            maxVersion = ApiKeys.JOIN_GROUP.latestVersion(),
            commonResponse = commonResponse,
        )
    }

    @Test
    fun shouldCreateApiResponseOnlyWithKeysSupportedByMagicValue() {
        val response = ApiVersionsResponse.createApiVersionsResponse(
            throttleTimeMs = 10,
            minRecordVersion = RecordVersion.V1,
            latestSupportedFeatures = Features.emptySupportedFeatures(), finalizedFeatures = emptyMap(),
            finalizedFeaturesEpoch = ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            controllerApiVersions = null,
            listenerType = ListenerType.ZK_BROKER,
            enableUnstableLastVersion = true,
            zkMigrationEnabled = false,
        )
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1)
        assertEquals(10, response.throttleTimeMs())
        assertTrue(response.data().supportedFeatures.isEmpty())
        assertTrue(response.data().finalizedFeatures.isEmpty())
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data().finalizedFeaturesEpoch)
    }

    @Test
    fun shouldReturnFeatureKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle() {
        val response = ApiVersionsResponse.createApiVersionsResponse(
            throttleTimeMs = 10,
            minRecordVersion = RecordVersion.V1,
            latestSupportedFeatures = Features.supportedFeatures(
                mapOf("feature" to SupportedVersionRange(1.toShort(), 4.toShort())),
            ),
            finalizedFeatures = mapOf("feature" to 3.toShort()),
            finalizedFeaturesEpoch = 10L,
            controllerApiVersions = null,
            listenerType = ListenerType.ZK_BROKER,
            enableUnstableLastVersion = true,
            zkMigrationEnabled = false,
        )
        verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1)
        assertEquals(10, response.throttleTimeMs())
        assertEquals(1, response.data().supportedFeatures.size)

        val sKey = response.data().supportedFeatures.find("feature")
        assertNotNull(sKey)
        assertEquals(1, sKey.minVersion)
        assertEquals(4, sKey.maxVersion)
        assertEquals(1, response.data().finalizedFeatures.size)

        val fKey = response.data().finalizedFeatures.find("feature")
        assertNotNull(fKey)
        assertEquals(3, fKey.minVersionLevel)
        assertEquals(3, fKey.maxVersionLevel)
        assertEquals(10, response.data().finalizedFeaturesEpoch)
    }

    @ParameterizedTest
    @EnumSource(names = ["ZK_BROKER", "BROKER"])
    fun shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle(listenerType : ListenerType) {
        val response = ApiVersionsResponse.createApiVersionsResponse(
            throttleTimeMs = AbstractResponse.DEFAULT_THROTTLE_TIME,
            minRecordVersion = RecordVersion.current(),
            latestSupportedFeatures = Features.emptySupportedFeatures(),
            finalizedFeatures = emptyMap(),
            finalizedFeaturesEpoch = ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            controllerApiVersions = null,
            listenerType = listenerType,
            enableUnstableLastVersion = true,
            zkMigrationEnabled = false,
        )
        assertEquals(ApiKeys.apisForListener(listenerType), apiKeysInResponse(response))
        assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs())
        assertTrue(response.data().supportedFeatures.isEmpty())
        assertTrue(response.data().finalizedFeatures.isEmpty())
        assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data().finalizedFeaturesEpoch)
    }

    @Test
    fun testMetadataQuorumApisAreDisabled() {
        val response = ApiVersionsResponse.createApiVersionsResponse(
            throttleTimeMs = AbstractResponse.DEFAULT_THROTTLE_TIME,
            minRecordVersion = RecordVersion.current(),
            latestSupportedFeatures = Features.emptySupportedFeatures(), finalizedFeatures = emptyMap(),
            finalizedFeaturesEpoch = ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
            controllerApiVersions = null,
            listenerType = ListenerType.ZK_BROKER,
            enableUnstableLastVersion = true,
            zkMigrationEnabled = false,
        )

        // Ensure that APIs needed for the KRaft mode are not exposed through ApiVersions until we are ready for them
        val exposedApis = apiKeysInResponse(response)
        assertFalse(exposedApis.contains(ApiKeys.VOTE))
        assertFalse(exposedApis.contains(ApiKeys.BEGIN_QUORUM_EPOCH))
        assertFalse(exposedApis.contains(ApiKeys.END_QUORUM_EPOCH))
        assertFalse(exposedApis.contains(ApiKeys.DESCRIBE_QUORUM))
    }

    @Test
    fun testIntersect() {
        assertNull(ApiVersionsResponse.intersect(null, null))
        assertFailsWith<IllegalArgumentException> {
            ApiVersionsResponse.intersect(
                thisVersion = ApiVersionsResponseData.ApiVersion().setApiKey(10),
                other = ApiVersionsResponseData.ApiVersion().setApiKey(3),
            )
        }
        val min: Short = 0
        val max: Short = 10
        val thisVersion = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.FETCH.id)
            .setMinVersion(min)
            .setMaxVersion(Short.MAX_VALUE)
        val other = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.FETCH.id)
            .setMinVersion(Short.MIN_VALUE)
            .setMaxVersion(max)
        val expected = ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.FETCH.id)
            .setMinVersion(min)
            .setMaxVersion(max)
        assertNull(ApiVersionsResponse.intersect(thisVersion, null))
        assertNull(ApiVersionsResponse.intersect(null, other))
        assertEquals(expected, ApiVersionsResponse.intersect(thisVersion, other))
        // test for symmetric
        assertEquals(expected, ApiVersionsResponse.intersect(other, thisVersion))
    }

    private fun verifyVersions(
        forwardableAPIKey: Short,
        minVersion: Short,
        maxVersion: Short,
        commonResponse: ApiVersionCollection,
    ) {
        val expectedVersionsForForwardableAPI = ApiVersionsResponseData.ApiVersion()
            .setApiKey(forwardableAPIKey)
            .setMinVersion(minVersion)
            .setMaxVersion(maxVersion)
        assertEquals(expectedVersionsForForwardableAPI, commonResponse.find(forwardableAPIKey))
    }

    private fun verifyApiKeysForMagic(response: ApiVersionsResponse, maxMagic: Byte) {
        for (version in response.data().apiKeys)
            assertTrue(ApiKeys.forId(version.apiKey.toInt()).minRequiredInterBrokerMagic <= maxMagic)
    }

    private fun apiKeysInResponse(apiVersions: ApiVersionsResponse): Set<ApiKeys> {
        val apiKeys = HashSet<ApiKeys>()
        for (version in apiVersions.data().apiKeys)
            apiKeys.add(ApiKeys.forId(version.apiKey.toInt()))
        return apiKeys
    }
}
