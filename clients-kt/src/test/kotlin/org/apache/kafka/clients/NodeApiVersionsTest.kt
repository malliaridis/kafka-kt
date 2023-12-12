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

package org.apache.kafka.clients

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class NodeApiVersionsTest {

    @Test
    fun testUnsupportedVersionsToString() {
        val versions = NodeApiVersions(
            nodeApiVersions = ApiVersionCollection(),
            nodeSupportedFeatures = emptyList(),
            zkMigrationEnabled = false,
        )
        val bld = StringBuilder()
        var prefix = "("
        for (apiKey in ApiKeys.clientApis()) {
            bld.append(prefix)
                .append(apiKey.name)
                .append("(")
                .append(apiKey.id.toInt())
                .append("): UNSUPPORTED")
            prefix = ", "
        }
        bld.append(")")
        assertEquals(bld.toString(), versions.toString())
    }

    @Test
    fun testUnknownApiVersionsToString() {
        val versions = NodeApiVersions.create(
            apiKey = 337.toShort(),
            minVersion = 0.toShort(),
            maxVersion = 1.toShort(),
        )
        assertTrue(versions.toString().endsWith("UNKNOWN(337): 0 to 1)"))
    }

    @Test
    fun testVersionsToString() {
        val versionList = ApiKeys.entries.map { apiKey ->
            if (apiKey === ApiKeys.DELETE_TOPICS) ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiKey.id)
                    .setMinVersion(10000)
                    .setMaxVersion(10001)
            else ApiVersionsResponse.toApiVersion(apiKey)
        }
        val versions = NodeApiVersions(
            nodeApiVersions = versionList,
            nodeSupportedFeatures = emptyList(),
            zkMigrationEnabled = false,
        )
        val bld = StringBuilder()
        var prefix = "("
        for (apiKey in ApiKeys.entries) {
            bld.append(prefix)
            if (apiKey === ApiKeys.DELETE_TOPICS)
                bld.append("DELETE_TOPICS(20): 10000 to 10001 [unusable: node too new]")
            else {
                bld.append(apiKey.name)
                    .append("(")
                    .append(apiKey.id.toInt())
                    .append("): ")

                if (apiKey.oldestVersion() == apiKey.latestVersion())
                    bld.append(apiKey.oldestVersion().toInt())
                else bld.append(apiKey.oldestVersion().toInt())
                    .append(" to ")
                    .append(apiKey.latestVersion().toInt())

                bld.append(" [usable: ")
                    .append(apiKey.latestVersion().toInt())
                    .append("]")
            }
            prefix = ", "
        }
        bld.append(")")
        assertEquals(bld.toString(), versions.toString())
    }

    @Test
    fun testLatestUsableVersion() {
        val apiVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, 1.toShort(), 3.toShort())
        assertEquals(
            expected = 3,
            actual = apiVersions.latestUsableVersion(apiKey = ApiKeys.PRODUCE).toInt(),
        )
        assertEquals(
            expected = 1,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 0.toShort(),
                latestAllowedVersion = 1.toShort()
            ).toInt(),
        )
        assertEquals(
            expected = 1,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 1.toShort(),
                latestAllowedVersion = 1.toShort(),
            ).toInt(),
        )
        assertEquals(
            expected = 2,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 1.toShort(),
                latestAllowedVersion = 2.toShort()
            ).toInt(),
        )
        assertEquals(
            expected = 3,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 1.toShort(),
                latestAllowedVersion = 3.toShort()
            ).toInt(),
        )
        assertEquals(
            expected = 2,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 2.toShort(),
                latestAllowedVersion = 2.toShort()
            ).toInt(),
        )
        assertEquals(
            expected = 3,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 2.toShort(),
                latestAllowedVersion = 3.toShort()
            ).toInt(),
        )
        assertEquals(
            expected = 3,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 3.toShort(),
                latestAllowedVersion = 3.toShort()
            ).toInt(),
        )
        assertEquals(
            expected = 3,
            actual = apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 3.toShort(),
                latestAllowedVersion = 4.toShort()
            ).toInt(),
        )
    }

    @Test
    fun testLatestUsableVersionOutOfRangeLow() {
        val apiVersions = NodeApiVersions.create(
            apiKey = ApiKeys.PRODUCE.id,
            minVersion = 1.toShort(),
            maxVersion = 2.toShort(),
        )
        assertFailsWith<UnsupportedVersionException> {
            apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 3.toShort(),
                latestAllowedVersion = 4.toShort()
            )
        }
    }

    @Test
    fun testLatestUsableVersionOutOfRangeHigh() {
        val apiVersions = NodeApiVersions.create(
            apiKey = ApiKeys.PRODUCE.id,
            minVersion = 2.toShort(),
            maxVersion = 3.toShort(),
        )
        assertFailsWith<UnsupportedVersionException> {
            apiVersions.latestUsableVersion(
                apiKey = ApiKeys.PRODUCE,
                oldestAllowedVersion = 0.toShort(),
                latestAllowedVersion = 1.toShort(),
            )
        }
    }

    @Test
    fun testUsableVersionCalculationNoKnownVersions() {
        val versions = NodeApiVersions(
            nodeApiVersions = ApiVersionCollection(),
            nodeSupportedFeatures = emptyList(),
            zkMigrationEnabled = false,
        )
        assertFailsWith<UnsupportedVersionException> { versions.latestUsableVersion(ApiKeys.FETCH) }
    }

    @Test
    fun testLatestUsableVersionOutOfRange() {
        val apiVersions = NodeApiVersions.create(
            apiKey = ApiKeys.PRODUCE.id,
            minVersion = 300.toShort(),
            maxVersion = 300.toShort(),
        )
        assertFailsWith<UnsupportedVersionException> {
            apiVersions.latestUsableVersion(ApiKeys.PRODUCE)
        }
    }

    @ParameterizedTest
    @EnumSource(ListenerType::class)
    fun testUsableVersionLatestVersions(scope: ListenerType) {
        val defaultResponse = TestUtils.defaultApiVersionsResponse(listenerType = scope)
        val versionList = defaultResponse.data().apiKeys.toMutableList()
        // Add an API key that we don't know about.
        versionList.add(
            ApiVersionsResponseData.ApiVersion()
                .setApiKey(100.toShort())
                .setMinVersion(0.toShort())
                .setMaxVersion(1.toShort())
        )
        val versions = NodeApiVersions(
            nodeApiVersions = versionList,
            nodeSupportedFeatures = emptyList(),
            zkMigrationEnabled = false
        )
        for (apiKey in ApiKeys.apisForListener(scope)) {
            assertEquals(apiKey.latestVersion(), versions.latestUsableVersion(apiKey))
        }
    }

    @ParameterizedTest
    @EnumSource(ListenerType::class)
    fun testConstructionFromApiVersionsResponse(scope: ListenerType) {
        val apiVersionsResponse = TestUtils.defaultApiVersionsResponse(listenerType = scope)
        val versions = NodeApiVersions(
            nodeApiVersions = apiVersionsResponse.data().apiKeys,
            nodeSupportedFeatures = emptyList(),
            zkMigrationEnabled = false,
        )

        for (apiVersionKey in apiVersionsResponse.data().apiKeys) {
            val apiVersion = versions.apiVersion(ApiKeys.forId(apiVersionKey.apiKey.toInt()))!!
            assertEquals(expected = apiVersionKey.apiKey, actual = apiVersion.apiKey)
            assertEquals(expected = apiVersionKey.minVersion, actual = apiVersion.minVersion)
            assertEquals(expected = apiVersionKey.maxVersion, actual = apiVersion.maxVersion)
        }
    }
}
