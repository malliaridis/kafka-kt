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

package org.apache.kafka.common.protocol

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ApiKeysTest {
    
    @Test
    fun testForIdWithInvalidIdLow() {
        assertFailsWith<IllegalArgumentException> { ApiKeys.forId(-1) }
    }

    @Test
    fun testForIdWithInvalidIdHigh() {
        assertFailsWith<IllegalArgumentException> { ApiKeys.forId(10000) }
    }

    @Test
    fun testAlterPartitionIsClusterAction() {
        assertTrue(ApiKeys.ALTER_PARTITION.clusterAction)
    }

    /**
     * All valid client responses which may be throttled should have a field named
     * 'throttle_time_ms' to return the throttle time to the client. Exclusions are
     *
     * - Cluster actions used only for inter-broker are throttled only if unauthorized
     * - SASL_HANDSHAKE and SASL_AUTHENTICATE are not throttled when used for authentication
     * when a connection is established or for re-authentication thereafter; these requests
     * return an error response that may be throttled if they are sent otherwise.
     */
    @Test
    fun testResponseThrottleTime() {
        val authenticationKeys = setOf(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE)
        // Newer protocol apis include throttle time ms even for cluster actions
        val clusterActionsWithThrottleTimeMs = setOf(
            ApiKeys.ALTER_PARTITION,
            ApiKeys.ALLOCATE_PRODUCER_IDS,
            ApiKeys.UPDATE_FEATURES,
        )
        for (apiKey in ApiKeys.zkBrokerApis()) {
            val responseSchema = apiKey.messageType.responseSchemas()[apiKey.latestVersion().toInt()]!!
            val throttleTimeField = responseSchema["throttle_time_ms"]
            if (
                apiKey.clusterAction
                && !clusterActionsWithThrottleTimeMs.contains(apiKey)
                || authenticationKeys.contains(apiKey)
            ) assertNull(throttleTimeField, "Unexpected throttle time field: $apiKey")
            else assertNotNull(throttleTimeField, "Throttle time field missing: $apiKey")
        }
    }

    @Test
    fun testApiScope() {
        val apisMissingScope = ApiKeys.values()
            .filter { apiKey -> apiKey.messageType.listeners().isEmpty() }
            .toSet()
        assertEquals(emptySet(), apisMissingScope, "Found some APIs missing scope definition")
    }
}
