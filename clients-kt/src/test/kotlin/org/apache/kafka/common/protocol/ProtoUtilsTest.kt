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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProtoUtilsTest {

    @Test
    fun testDelayedAllocationSchemaDetection() {
        //verifies that schemas known to retain a reference to the underlying byte buffer are correctly detected.
        for (key in ApiKeys.values()) {
            when (key) {
                ApiKeys.PRODUCE,
                ApiKeys.JOIN_GROUP,
                ApiKeys.SYNC_GROUP,
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.EXPIRE_DELEGATION_TOKEN,
                ApiKeys.RENEW_DELEGATION_TOKEN,
                ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                ApiKeys.ENVELOPE,
                -> assertTrue(
                    actual = key.requiresDelayedAllocation,
                    message = "$key should require delayed allocation",
                )

                else -> if (key.forwardable) assertTrue(
                    actual = key.requiresDelayedAllocation,
                    message = "$key should require delayed allocation since it is forwardable",
                ) else assertFalse(
                    actual = key.requiresDelayedAllocation,
                    message = "$key should not require delayed allocation",
                )
            }
        }
    }
}
