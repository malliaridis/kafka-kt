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

package org.apache.kafka.clients.admin

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ConfigTest {

    private var config: Config? = null

    @BeforeEach
    fun setUp() {
        config = Config(listOf(E1, E2))
    }

    @Test
    fun shouldGetEntry() {
        assertEquals(E1, config!!["a"])
        assertEquals(E2, config!!["c"])
    }

    @Test
    fun shouldReturnNullOnGetUnknownEntry() {
        assertNull(config!!["unknown"])
    }

    @Test
    fun shouldGetAllEntries() {
        assertEquals(2, config!!.entries().size)
        assertTrue(config!!.entries().contains(E1))
        assertTrue(config!!.entries().contains(E2))
    }

    @Test
    fun shouldImplementEqualsProperly() {
        assertEquals(config, config)
        assertEquals(config, Config(config!!.entries()))
        assertNotEquals(Config(listOf(E1)), config)
        assertNotEquals("this", config.toString())
    }

    @Test
    fun shouldImplementHashCodeProperly() {
        assertEquals(config.hashCode(), config.hashCode())
        assertEquals(config.hashCode(), Config(config!!.entries()).hashCode())
        assertNotEquals(Config(listOf(E1)).hashCode(), config.hashCode())
    }

    @Test
    fun shouldImplementToStringProperly() {
        assertTrue(config.toString().contains(E1.toString()))
        assertTrue(config.toString().contains(E2.toString()))
    }

    @Test
    fun testHashCodeAndEqualsWithNull() {
        val ce0 = ConfigEntry(
            name = "abc",
            value = null,
        )
        val ce1 = ConfigEntry(
            name = "abc",
            value = null,
        )
        assertEquals(ce0, ce1)
        assertEquals(ce0.hashCode(), ce1.hashCode())
    }

    @Test
    fun testEquals() {
        val ce0 = ConfigEntry(
            name = "abc",
            value = null,
            source = ConfigEntry.ConfigSource.DEFAULT_CONFIG,
        )
        val ce1 =
            ConfigEntry(
                name = "abc",
                value = null,
                source = ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            )
        assertNotEquals(ce0, ce1)
    }

    companion object {

        private val E1 = ConfigEntry("a", "b")

        private val E2 = ConfigEntry("c", "d")

        fun newConfigEntry(
            name: String,
            value: String?,
            source: ConfigEntry.ConfigSource,
            isSensitive: Boolean,
            isReadOnly: Boolean,
            synonyms: List<ConfigEntry.ConfigSynonym>,
        ): ConfigEntry {
            return ConfigEntry(
                name,
                value,
                source,
                isSensitive,
                isReadOnly,
                synonyms,
                ConfigEntry.ConfigType.UNKNOWN,
                null
            )
        }
    }
}
