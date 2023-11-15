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

import org.apache.kafka.clients.admin.FeatureUpdate
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKey
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKeyCollection
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class UpdateFeaturesRequestTest {
    @Test
    fun testGetErrorResponse() {
        val features = FeatureUpdateKeyCollection()
        features.add(
            FeatureUpdateKey()
                .setFeature("foo")
                .setMaxVersionLevel(2)
        )
        features.add(
            FeatureUpdateKey()
                .setFeature("bar")
                .setMaxVersionLevel(3)
        )
        val request = UpdateFeaturesRequest(
            data = UpdateFeaturesRequestData().setFeatureUpdates(features),
            version = UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION
        )
        val response = request.getErrorResponse(throttleTimeMs = 0, e = UnknownServerException())
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.topLevelError().error)
        assertEquals(0, response.data().results.size)
        assertEquals(mapOf(Errors.UNKNOWN_SERVER_ERROR to 1), response.errorCounts())
    }

    @Test
    fun testUpdateFeaturesV0() {
        val features = FeatureUpdateKeyCollection()
        features.add(
            FeatureUpdateKey()
                .setFeature("foo")
                .setMaxVersionLevel(1.toShort())
                .setAllowDowngrade(true)
        )
        features.add(
            FeatureUpdateKey()
                .setFeature("bar")
                .setMaxVersionLevel(3.toShort())
        )
        var request = UpdateFeaturesRequest(
            data = UpdateFeaturesRequestData().setFeatureUpdates(features),
            version = UpdateFeaturesRequestData.LOWEST_SUPPORTED_VERSION,
        )
        val buffer = request.serialize()
        request = UpdateFeaturesRequest.parse(
            buffer = buffer,
            version = UpdateFeaturesRequestData.LOWEST_SUPPORTED_VERSION,
        )
        val updates = request.featureUpdates().toList()

        assertEquals(2, updates.size)
        assertEquals(FeatureUpdate.UpgradeType.SAFE_DOWNGRADE, updates[0].upgradeType)
        assertEquals(FeatureUpdate.UpgradeType.UPGRADE, updates[1].upgradeType)
    }

    @Test
    fun testUpdateFeaturesV1() {
        val features = FeatureUpdateKeyCollection()
        features.add(
            FeatureUpdateKey()
                .setFeature("foo")
                .setMaxVersionLevel(1)
                .setUpgradeType(FeatureUpdate.UpgradeType.SAFE_DOWNGRADE.code)
        )
        features.add(
            FeatureUpdateKey()
                .setFeature("bar")
                .setMaxVersionLevel(3)
        )
        var request = UpdateFeaturesRequest(
            data = UpdateFeaturesRequestData().setFeatureUpdates(features),
            version = UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION,
        )
        val buffer = request.serialize()
        request = UpdateFeaturesRequest.parse(
            buffer = buffer,
            version = UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION,
        )
        val updates = request.featureUpdates().toList()
        assertEquals(2, updates.size)
        assertEquals(FeatureUpdate.UpgradeType.SAFE_DOWNGRADE, updates[0].upgradeType)
        assertEquals(FeatureUpdate.UpgradeType.UPGRADE, updates[1].upgradeType)
    }

    @Test
    fun testUpdateFeaturesV1OldBoolean() {
        val features = FeatureUpdateKeyCollection()
        features.add(
            FeatureUpdateKey()
                .setFeature("foo")
                .setMaxVersionLevel(1.toShort())
                .setAllowDowngrade(true)
        )
        features.add(
            FeatureUpdateKey()
                .setFeature("bar")
                .setMaxVersionLevel(3.toShort())
        )
        val request = UpdateFeaturesRequest(
            UpdateFeaturesRequestData().setFeatureUpdates(features),
            UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION
        )
        assertFailsWith<UnsupportedVersionException>(
            message = "This should fail since allowDowngrade is not supported in v1 of this RPC",
        ) { request.serialize() }
    }
}
