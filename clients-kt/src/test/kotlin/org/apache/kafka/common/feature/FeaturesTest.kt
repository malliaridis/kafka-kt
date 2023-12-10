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

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FeaturesTest {

    @Test
    fun testEmptyFeatures() {
        val emptyMap = emptyMap<String, Map<String, Short>>()
        val emptySupportedFeatures = Features.emptySupportedFeatures()
        assertTrue(emptySupportedFeatures.features.isEmpty())
        assertTrue(emptySupportedFeatures.toMap().isEmpty())
        assertEquals(emptySupportedFeatures, Features.fromSupportedFeaturesMap(emptyMap))
    }

    @Test
    @Disabled("Kotlin Migration: Assertion is granted by Kotlin non-nullable parameter")
    fun testNullFeatures() {
        // assertFailsWith<NullPointerException> { Features.supportedFeatures(null) }
    }

    @Test
    fun testGetAllFeaturesAPI() {
        val v1 = SupportedVersionRange(1.toShort(), 2.toShort())
        val v2 = SupportedVersionRange(3.toShort(), 4.toShort())
        val allFeatures = mapOf("feature_1" to v1, "feature_2" to v2)
        val features = Features.supportedFeatures(allFeatures)
        assertEquals(allFeatures, features.features)
    }

    @Test
    fun testGetAPI() {
        val v1 = SupportedVersionRange(1.toShort(), 2.toShort())
        val v2 = SupportedVersionRange(3.toShort(), 4.toShort())
        val allFeatures = mapOf("feature_1" to v1, "feature_2" to v2)
        val features = Features.supportedFeatures(allFeatures)
        assertEquals(v1, features["feature_1"])
        assertEquals(v2, features["feature_2"])
        assertNull(features["nonexistent_feature"])
    }

    @Test
    fun testFromFeaturesMapToFeaturesMap() {
        val v1 = SupportedVersionRange(1.toShort(), 2.toShort())
        val v2 = SupportedVersionRange(3.toShort(), 4.toShort())
        val allFeatures = mapOf("feature_1" to v1, "feature_2" to v2)
        val features = Features.supportedFeatures(allFeatures)
        val expected = mapOf(
            "feature_1" to mapOf("min_version" to 1.toShort(), "max_version" to 2.toShort()),
            "feature_2" to mapOf("min_version" to 3.toShort(), "max_version" to 4.toShort()),
        )
        assertEquals(expected, features.toMap())
        assertEquals(features, Features.fromSupportedFeaturesMap(expected))
    }

    @Test
    fun testToStringSupportedFeatures() {
        val v1 = SupportedVersionRange(1.toShort(), 2.toShort())
        val v2 = SupportedVersionRange(3.toShort(), 4.toShort())
        val allFeatures = mapOf("feature_1" to v1, "feature_2" to v2)
        val features = Features.supportedFeatures(allFeatures)
        assertEquals(
            expected = "Features{" +
                    "(feature_1 -> SupportedVersionRange[min_version:1, max_version:2]), " +
                    "(feature_2 -> SupportedVersionRange[min_version:3, max_version:4])" +
                    "}",
            actual = features.toString()
        )
    }

    @Test
    fun testSupportedFeaturesFromMapFailureWithInvalidMissingMaxVersion() {
        // This is invalid because 'max_version' key is missing.
        val invalidFeatures = mapOf("feature_1" to mapOf("min_version" to 1.toShort()))
        assertFailsWith<IllegalArgumentException> { Features.fromSupportedFeaturesMap(invalidFeatures) }
    }

    @Test
    fun testEquals() {
        val v1 = SupportedVersionRange(1.toShort(), 2.toShort())
        val allFeatures = mapOf("feature_1" to v1)
        val features = Features.supportedFeatures(allFeatures)
        val featuresClone = Features.supportedFeatures(allFeatures)
        assertEquals(features, featuresClone)
        val v2 = SupportedVersionRange(1.toShort(), 3.toShort())
        val allFeaturesDifferent = mapOf("feature_1" to v2)
        val featuresDifferent = Features.supportedFeatures(allFeaturesDifferent)
        assertNotEquals(features, featuresDifferent)
        assertNotNull(features)
    }
}
