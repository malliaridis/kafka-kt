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

package org.apache.kafka.common.config

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ConfigResourceTest {

    @Test
    fun shouldGetTypeFromId() {
        assertEquals(ConfigResource.Type.TOPIC, ConfigResource.Type.forId(2.toByte()))
        assertEquals(ConfigResource.Type.BROKER, ConfigResource.Type.forId(4.toByte()))
    }

    @Test
    fun shouldReturnUnknownForUnknownCode() {
        assertEquals(ConfigResource.Type.UNKNOWN, ConfigResource.Type.forId((-1.toByte()).toByte()))
        assertEquals(ConfigResource.Type.UNKNOWN, ConfigResource.Type.forId(0.toByte()))
        assertEquals(ConfigResource.Type.UNKNOWN, ConfigResource.Type.forId(1.toByte()))
    }

    @Test
    fun shouldRoundTripEveryType() {
        ConfigResource.Type.values().forEach { type ->
            assertEquals(
                expected = type,
                actual = ConfigResource.Type.forId(type.id),
                message = type.toString()
            )
        }
    }
}
