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

package org.apache.kafka.common.utils

import java.security.Provider
import java.security.Security
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProviderCreator
import org.apache.kafka.common.security.ssl.mock.TestPlainSaslServerProviderCreator
import org.apache.kafka.common.security.ssl.mock.TestScramSaslServerProviderCreator
import org.apache.kafka.common.utils.SecurityUtils.addConfiguredSecurityProviders
import org.apache.kafka.common.utils.SecurityUtils.parseKafkaPrincipal
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SecurityUtilsTest {

    private val testScramSaslServerProviderCreator: SecurityProviderCreator = TestScramSaslServerProviderCreator()

    private val testPlainSaslServerProviderCreator: SecurityProviderCreator = TestPlainSaslServerProviderCreator()

    private val testScramSaslServerProvider = testScramSaslServerProviderCreator.provider

    private val testPlainSaslServerProvider = testPlainSaslServerProviderCreator.provider

    private fun clearTestProviders() {
        Security.removeProvider(testScramSaslServerProvider.name)
        Security.removeProvider(testPlainSaslServerProvider.name)
    }

    @BeforeEach // Remove the providers if already added
    fun setUp() {
        clearTestProviders()
    }

    // Remove the providers after running test cases
    @AfterEach
    fun tearDown() {
        clearTestProviders()
    }

    @Test
    fun testPrincipalNameCanContainSeparator() {
        val name = "name:with:separator:in:it"
        val principal = parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + name)
        assertEquals(KafkaPrincipal.USER_TYPE, principal.principalType)
        assertEquals(name, principal.getName())
    }

    @Test
    fun testParseKafkaPrincipalWithNonUserPrincipalType() {
        val name = "foo"
        val principalType = "Group"
        val principal = parseKafkaPrincipal("$principalType:$name")
        assertEquals(principalType, principal.principalType)
        assertEquals(name, principal.getName())
    }

    private fun getProviderIndexFromName(providerName: String, providers: Array<Provider>): Int {
        for (index in providers.indices)
            if (providers[index].name == providerName) return index

        return -1
    }

    // Tests if the custom providers configured are being added to the JVM correctly. These providers are
    // expected to be added at the start of the list of available providers and with the relative ordering maintained
    @Test
    fun testAddCustomSecurityProvider() {
        val customProviderClasses = testScramSaslServerProviderCreator.javaClass.getName() + "," +
                testPlainSaslServerProviderCreator.javaClass.getName()
        val configs: MutableMap<String, String?> = HashMap()
        configs[SecurityConfig.SECURITY_PROVIDERS_CONFIG] = customProviderClasses
        addConfiguredSecurityProviders(configs)
        val providers = Security.getProviders()
        val testScramSaslServerProviderIndex = getProviderIndexFromName(testScramSaslServerProvider.name, providers)
        val testPlainSaslServerProviderIndex = getProviderIndexFromName(testPlainSaslServerProvider.name, providers)
        assertEquals(
            expected = 0, actual = testScramSaslServerProviderIndex,
            message = testScramSaslServerProvider.name + " testProvider not found at expected index",
        )
        assertEquals(
            expected = 1, actual = testPlainSaslServerProviderIndex,
            message = testPlainSaslServerProvider.name + " testProvider not found at expected index",
        )
    }
}
