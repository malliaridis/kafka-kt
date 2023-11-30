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

package org.apache.kafka.common.security

import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasUtils.DISALLOWED_LOGIN_MODULES_CONFIG
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * Tests parsing of [SaslConfigs.SASL_JAAS_CONFIG] property and verifies that the format
 * and parsing are consistent with JAAS configuration files loaded by the JRE.
 */
class JaasContextTest {

    private lateinit var jaasConfigFile: File

    @BeforeEach
    @Throws(IOException::class)
    fun setUp() {
        jaasConfigFile = tempFile(prefix = "jaas", suffix = ".conf")
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.toString())
        Configuration.setConfiguration(null)
    }

    @AfterEach
    @Throws(Exception::class)
    fun tearDown() {
        Files.delete(jaasConfigFile.toPath())
        System.clearProperty(DISALLOWED_LOGIN_MODULES_CONFIG)
    }

    @Test
    @Throws(Exception::class)
    fun testConfigNoOptions() {
        checkConfiguration(
            loginModule = "test.testConfigNoOptions",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options = emptyMap(),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testControlFlag() {
        val controlFlags = arrayOf(
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            AppConfigurationEntry.LoginModuleControlFlag.REQUISITE,
            AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
            AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL
        )
        val options = mapOf("propName" to "propValue")
        for (controlFlag in controlFlags) checkConfiguration(
            loginModule = "test.testControlFlag",
            controlFlag = controlFlag,
            options = options,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testSingleOption() {
        val options = mapOf("propName" to "propValue")
        checkConfiguration(
            loginModule = "test.testSingleOption",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE,
            options = options,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testMultipleOptions() {
        val options = (0..9).associate { "propName$it" to "propValue$it" }
        checkConfiguration(
            loginModule = "test.testMultipleOptions",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
            options = options,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testQuotedOptionValue() {
        val options = mapOf(
            "propName" to "prop value",
            "propName2" to "value1 = 1, value2 = 2",
        )
        val config =
            """test.testQuotedOptionValue required propName="${options["propName"]}" propName2="${options["propName2"]}";"""
        checkConfiguration(
            jaasConfigProp = config,
            loginModule = "test.testQuotedOptionValue",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options = options,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testQuotedOptionName() {
        val options = mapOf("prop name" to "propValue")
        val config = "test.testQuotedOptionName required \"prop name\"=propValue;"
        checkConfiguration(
            jaasConfigProp = config,
            loginModule = "test.testQuotedOptionName",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options = options,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testMultipleLoginModules() {
        val builder = StringBuilder()
        val moduleCount = 3
        val moduleOptions: MutableMap<Int, Map<String, Any>> = HashMap()
        for (i in 0 until moduleCount) {
            val options = mapOf(
                "index" to "Index$i",
                "module" to "Module$i",
            )
            moduleOptions[i] = options
            val module = jaasConfigProp(
                loginModule = "test.Module$i",
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options = options,
            )
            builder.append(' ')
            builder.append(module)
        }
        val jaasConfigProp = builder.toString()
        val clientContextName = "CLIENT"
        val configuration: Configuration = JaasConfig(clientContextName, jaasConfigProp)
        val dynamicEntries = configuration.getAppConfigurationEntry(clientContextName)
        assertEquals(moduleCount, dynamicEntries.size)
        for (i in 0 until moduleCount) {
            val entry = dynamicEntries[i]
            checkEntry(
                entry = entry,
                loginModule = "test.Module$i",
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options = moduleOptions[i]!!,
            )
        }
        val serverContextName = "SERVER"
        writeConfiguration(serverContextName, jaasConfigProp)
        val staticEntries = Configuration.getConfiguration().getAppConfigurationEntry(serverContextName)
        for (i in 0 until moduleCount) {
            val staticEntry = staticEntries[i]
            checkEntry(
                entry = staticEntry,
                loginModule = dynamicEntries[i].loginModuleName,
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options = dynamicEntries[i].options,
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testMissingLoginModule() {
        checkInvalidConfiguration(jaasConfigProp = "  required option1=value1;")
    }

    @Test
    @Throws(Exception::class)
    fun testMissingControlFlag() {
        checkInvalidConfiguration(jaasConfigProp = "test.loginModule option1=value1;")
    }

    @Test
    @Throws(Exception::class)
    fun testMissingOptionValue() {
        checkInvalidConfiguration(jaasConfigProp = "loginModule required option1;")
    }

    @Test
    @Throws(Exception::class)
    fun testMissingSemicolon() {
        checkInvalidConfiguration(jaasConfigProp = "test.testMissingSemicolon required option1=value1")
    }

    @Test
    @Throws(Exception::class)
    fun testNumericOptionWithoutQuotes() {
        checkInvalidConfiguration(jaasConfigProp = "test.testNumericOptionWithoutQuotes required option1=3;")
    }

    @Test
    @Throws(Exception::class)
    fun testInvalidControlFlag() {
        checkInvalidConfiguration(jaasConfigProp = "test.testInvalidControlFlag { option1=3;")
    }

    @Test
    @Throws(Exception::class)
    fun testDisallowedLoginModulesSystemProperty() {
        //test JndiLoginModule is not allowed by default
        val jaasConfigProp1 = "com.sun.security.auth.module.JndiLoginModule required;"
        assertFailsWith<IllegalArgumentException> { configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp1) }

        //test ListenerName Override
        writeConfiguration(
            listOf(
                "KafkaServer { test.LoginModuleDefault required; };",
                "plaintext.KafkaServer { com.sun.security.auth.module.JndiLoginModule requisite; };",
            )
        )
        assertFailsWith<IllegalArgumentException> {
            JaasContext.loadServerContext(
                listenerName = ListenerName("plaintext"),
                mechanism = "SOME-MECHANISM",
                configs = emptyMap<String, Any?>(),
            )
        }

        //test org.apache.kafka.disallowed.login.modules system property with multiple modules
        System.setProperty(
            DISALLOWED_LOGIN_MODULES_CONFIG,
            " com.ibm.security.auth.module.LdapLoginModule , com.ibm.security.auth.module.Krb5LoginModule "
        )
        val jaasConfigProp2 = "com.ibm.security.auth.module.LdapLoginModule required;"
        assertFailsWith<IllegalArgumentException> {
            configurationEntry(
                contextType = JaasContext.Type.CLIENT,
                jaasConfigProp = jaasConfigProp2,
            )
        }

        //test ListenerName Override
        writeConfiguration(
            listOf(
                "KafkaServer { test.LoginModuleDefault required; };",
                "plaintext.KafkaServer { com.ibm.security.auth.module.Krb5LoginModule requisite; };"
            )
        )
        assertFailsWith<IllegalArgumentException> {
            JaasContext.loadServerContext(
                listenerName = ListenerName("plaintext"),
                mechanism = "SOME-MECHANISM",
                configs = emptyMap<String, Any?>(),
            )
        }

        //Remove default value for org.apache.kafka.disallowed.login.modules
        System.setProperty(DISALLOWED_LOGIN_MODULES_CONFIG, "")
        checkConfiguration(
            loginModule = "com.sun.security.auth.module.JndiLoginModule",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options = emptyMap(),
        )

        //test ListenerName Override
        writeConfiguration(
            listOf(
                "KafkaServer { com.ibm.security.auth.module.LdapLoginModule required; };",
                "plaintext.KafkaServer { com.sun.security.auth.module.JndiLoginModule requisite; };",
            )
        )
        val context = JaasContext.loadServerContext(
            listenerName = ListenerName("plaintext"),
            mechanism = "SOME-MECHANISM",
            configs = emptyMap<String, Any?>()
        )
        assertEquals(1, context.configurationEntries.size)
        checkEntry(
            entry = context.configurationEntries[0],
            loginModule = "com.sun.security.auth.module.JndiLoginModule",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE,
            options = emptyMap<String, Any?>(),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testNumericOptionWithQuotes() {
        val options = mapOf("option1" to "3")
        val config = "test.testNumericOptionWithQuotes required option1=\"3\";"
        checkConfiguration(
            jaasConfigProp = config,
            loginModule = "test.testNumericOptionWithQuotes",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options = options,
        )
    }

    @Test
    @Throws(IOException::class)
    fun testLoadForServerWithListenerNameOverride() {
        writeConfiguration(
            listOf(
                "KafkaServer { test.LoginModuleDefault required; };",
                "plaintext.KafkaServer { test.LoginModuleOverride requisite; };",
            )
        )
        val context = JaasContext.loadServerContext(
            listenerName = ListenerName("plaintext"),
            mechanism = "SOME-MECHANISM",
            configs = emptyMap<String, Any?>(),
        )
        assertEquals("plaintext.KafkaServer", context.name)
        assertEquals(JaasContext.Type.SERVER, context.type)
        assertEquals(1, context.configurationEntries.size)
        checkEntry(
            entry = context.configurationEntries[0],
            loginModule = "test.LoginModuleOverride",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE,
            options = emptyMap<String, Any>(),
        )
    }

    @Test
    @Throws(IOException::class)
    fun testLoadForServerWithListenerNameAndFallback() {
        writeConfiguration(
            listOf(
                "KafkaServer { test.LoginModule required; };",
                "other.KafkaServer { test.LoginModuleOther requisite; };"
            )
        )
        val context = JaasContext.loadServerContext(
            listenerName = ListenerName("plaintext"),
            mechanism = "SOME-MECHANISM",
            configs = emptyMap<String, Any>(),
        )
        assertEquals("KafkaServer", context.name)
        assertEquals(JaasContext.Type.SERVER, context.type)
        assertEquals(1, context.configurationEntries.size)
        checkEntry(
            entry = context.configurationEntries[0],
            loginModule = "test.LoginModule",
            controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options = emptyMap<String, Any>(),
        )
    }

    @Test
    @Throws(IOException::class)
    fun testLoadForServerWithWrongListenerName() {
        writeConfiguration("Server", "test.LoginModule required;")
        assertFailsWith<IllegalArgumentException> {
            JaasContext.loadServerContext(
                listenerName = ListenerName("plaintext"),
                mechanism = "SOME-MECHANISM",
                configs = emptyMap<String, Any>(),
            )
        }
    }

    private fun configurationEntry(contextType: JaasContext.Type, jaasConfigProp: String?): AppConfigurationEntry {
        val saslJaasConfig = if (jaasConfigProp == null) null else Password(jaasConfigProp)
        val context = JaasContext.load(
            contextType = contextType,
            listenerContextName = null,
            globalContextName = contextType.name,
            dynamicJaasConfig = saslJaasConfig,
        )
        val entries = context.configurationEntries
        assertEquals(1, entries.size)
        return entries[0]
    }

    private fun controlFlag(loginModuleControlFlag: AppConfigurationEntry.LoginModuleControlFlag): String {
        // LoginModuleControlFlag.toString() has format "LoginModuleControlFlag: flag"
        val tokens = loginModuleControlFlag.toString()
            .split(" ".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray()
        return tokens[tokens.size - 1]
    }

    private fun jaasConfigProp(
        loginModule: String,
        controlFlag: AppConfigurationEntry.LoginModuleControlFlag,
        options: Map<String, Any>,
    ): String {
        val builder = StringBuilder()
        builder.append(loginModule)
        builder.append(' ')
        builder.append(controlFlag(controlFlag))
        for ((key, value) in options) {
            builder.append(' ')
            builder.append(key)
            builder.append('=')
            builder.append(value)
        }
        builder.append(';')
        return builder.toString()
    }

    @Throws(IOException::class)
    private fun writeConfiguration(contextName: String, jaasConfigProp: String) {
        val lines = listOf("$contextName { ", jaasConfigProp, "};")
        writeConfiguration(lines)
    }

    @Throws(IOException::class)
    private fun writeConfiguration(lines: List<String>) {
        Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8)
        Configuration.setConfiguration(null)
    }

    @Throws(Exception::class)
    private fun checkConfiguration(
        loginModule: String,
        controlFlag: AppConfigurationEntry.LoginModuleControlFlag,
        options: Map<String, Any>,
    ) {
        val jaasConfigProp = jaasConfigProp(loginModule, controlFlag, options)
        checkConfiguration(jaasConfigProp, loginModule, controlFlag, options)
    }

    private fun checkEntry(
        entry: AppConfigurationEntry,
        loginModule: String,
        controlFlag: AppConfigurationEntry.LoginModuleControlFlag,
        options: Map<String, *>,
    ) {
        assertEquals(loginModule, entry.loginModuleName)
        assertEquals(controlFlag, entry.controlFlag)
        assertEquals(options, entry.options)
    }

    @Throws(Exception::class)
    private fun checkConfiguration(
        jaasConfigProp: String,
        loginModule: String,
        controlFlag: AppConfigurationEntry.LoginModuleControlFlag,
        options: Map<String, Any>,
    ) {
        val dynamicEntry = configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp)
        checkEntry(dynamicEntry, loginModule, controlFlag, options)
        assertNull(
            Configuration.getConfiguration().getAppConfigurationEntry(JaasContext.Type.CLIENT.name),
            "Static configuration updated"
        )
        writeConfiguration(JaasContext.Type.SERVER.name, jaasConfigProp)
        val staticEntry = configurationEntry(contextType = JaasContext.Type.SERVER, jaasConfigProp = null)
        checkEntry(staticEntry, loginModule, controlFlag, options)
    }

    @Throws(IOException::class)
    private fun checkInvalidConfiguration(jaasConfigProp: String) {
        try {
            writeConfiguration(JaasContext.Type.SERVER.name, jaasConfigProp)
            val entry = configurationEntry(JaasContext.Type.SERVER, null)
            fail("Invalid JAAS configuration file didn't throw exception, entry=$entry")
        } catch (_: SecurityException) {
            // Expected exception
        }
        try {
            val entry = configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp)
            fail("Invalid JAAS configuration property didn't throw exception, entry=$entry")
        } catch (_: IllegalArgumentException) {
            // Expected exception
        }
    }
}
