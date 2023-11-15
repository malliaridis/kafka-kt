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

import org.apache.kafka.common.message.UpdateFeaturesResponseData
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResultCollection
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class UpdateFeaturesResponseTest {

    @Test
    fun testErrorCounts() {
        val results = UpdatableFeatureResultCollection()
        results.add(
            UpdatableFeatureResult()
                .setFeature("foo")
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
        )
        results.add(
            UpdatableFeatureResult()
                .setFeature("bar")
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
        )
        results.add(
            UpdatableFeatureResult()
                .setFeature("baz")
                .setErrorCode(Errors.FEATURE_UPDATE_FAILED.code)
        )
        val response = UpdateFeaturesResponse(
            UpdateFeaturesResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code)
                .setResults(results)
        )
        val errorCounts = response.errorCounts()

        assertEquals(3, errorCounts.size)
        assertEquals(1, errorCounts[Errors.INVALID_REQUEST])
        assertEquals(2, errorCounts[Errors.UNKNOWN_SERVER_ERROR])
        assertEquals(1, errorCounts[Errors.FEATURE_UPDATE_FAILED])
    }
}
