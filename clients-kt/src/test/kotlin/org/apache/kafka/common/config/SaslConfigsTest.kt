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
import kotlin.test.assertFailsWith

class SaslConfigsTest {
    
    @Test
    fun testSaslLoginRefreshDefaults() {
        val vals = ConfigDef().withClientSaslSupport().parse(emptyMap<Any?, Any>())
        assertEquals(
            expected = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR,
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR],
        )
        assertEquals(
            expected = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER,
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER],
        )
        assertEquals(
            expected = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS],
        )
        assertEquals(
            expected = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS,
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS],
        )
    }

    @Test
    fun testSaslLoginRefreshMinValuesAreValid() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR] = "0.5"
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER] = "0.0"
        props[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] = "0"
        props[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS] = "0"
        val vals = ConfigDef().withClientSaslSupport().parse(props)

        assertEquals(
            expected = "0.5".toDouble(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR],
        )
        assertEquals(
            expected = "0.0".toDouble(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER],
        )
        assertEquals(
            expected = "0".toShort(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS],
        )
        assertEquals(
            expected = "0".toShort(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS],
        )
    }

    @Test
    fun testSaslLoginRefreshMaxValuesAreValid() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR] = "1.0"
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER] = "0.25"
        props[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] = "900"
        props[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS] = "3600"
        val vals = ConfigDef().withClientSaslSupport().parse(props)

        assertEquals(
            expected = "1.0".toDouble(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR],
        )
        assertEquals(
            expected = "0.25".toDouble(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER],
        )
        assertEquals(
            expected = "900".toShort(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS],
        )
        assertEquals(
            expected = "3600".toShort(),
            actual = vals[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS],
        )
    }

    @Test
    fun testSaslLoginRefreshWindowFactorMinValueIsReallyMinimum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR] = "0.499999"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshWindowFactorMaxValueIsReallyMaximum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR] = "1.0001"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshWindowJitterMinValueIsReallyMinimum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER] = "-0.000001"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshWindowJitterMaxValueIsReallyMaximum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER] = "0.251"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshMinPeriodSecondsMinValueIsReallyMinimum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] = "-1"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshMinPeriodSecondsMaxValueIsReallyMaximum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] = "901"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshBufferSecondsMinValueIsReallyMinimum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS] = "-1"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }

    @Test
    fun testSaslLoginRefreshBufferSecondsMaxValueIsReallyMaximum() {
        val props = mutableMapOf<Any?, Any?>()
        props[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS] = "3601"
        assertFailsWith<ConfigException> { ConfigDef().withClientSaslSupport().parse(props) }
    }
}
