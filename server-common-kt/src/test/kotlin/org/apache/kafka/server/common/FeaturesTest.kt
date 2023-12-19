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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class FeaturesTest {

    @Test
    fun testKRaftModeFeatures() {
        val features = Features(
            version = MetadataVersion.MINIMUM_KRAFT_VERSION,
            finalizedFeatures = mapOf("foo" to 2.toShort()),
            finalizedFeaturesEpoch = 123,
            kraftMode = true,
        )
        assertEquals(
            MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel,
            features.finalizedFeatures[MetadataVersion.FEATURE_NAME]
        )
        assertEquals(
            2,
            features.finalizedFeatures["foo"]
        )
        assertEquals(2, features.finalizedFeatures.size)
    }

    @Test
    fun testZkModeFeatures() {
        val features = Features(
            version = MetadataVersion.MINIMUM_KRAFT_VERSION,
            finalizedFeatures = mapOf("foo" to 2.toShort()),
            finalizedFeaturesEpoch = 123,
            kraftMode = false,
        )
        assertNull(features.finalizedFeatures[MetadataVersion.FEATURE_NAME])
        assertEquals(2, features.finalizedFeatures["foo"])
        assertEquals(1, features.finalizedFeatures.size)
    }
}
