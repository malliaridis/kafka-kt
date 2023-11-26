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

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.NonNullValidator
import org.apache.kafka.common.config.provider.MockFileConfigProvider
import org.apache.kafka.common.config.provider.MockVaultConfigProvider
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.metrics.FakeMetricsReporter
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.metrics.MetricsReporter
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.test.MockConsumerInterceptor
import org.junit.jupiter.api.Test
import java.util.*
import org.apache.kafka.common.utils.Utils
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class AbstractConfigTest {

    @Test
    fun testConfiguredInstances() {
        testValidInputs("")
        testValidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter")
        testValidInputs(
            "org.apache.kafka.common.metrics.FakeMetricsReporter, org.apache.kafka.common.metrics.FakeMetricsReporter"
        )
        testInvalidInputs(",")
        testInvalidInputs("org.apache.kafka.clients.producer.unknown-metrics-reporter")
        testInvalidInputs("test1,test2")
        testInvalidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter,")
    }

    @Test
    fun testEmptyList() {
        var conf: AbstractConfig
        val configDef = ConfigDef().define(
            name = "a",
            type = ConfigDef.Type.LIST,
            defaultValue = "",
            validator = NonNullValidator(),
            importance = Importance.HIGH,
            documentation = "doc",
        )
        conf = AbstractConfig(configDef, emptyMap())
        assertEquals(emptyList(), conf.getList("a"))
        conf = AbstractConfig(configDef, mapOf("a" to ""))
        assertEquals(emptyList(), conf.getList("a"))
        conf = AbstractConfig(configDef, mapOf("a" to "b,c,d"))
        assertEquals(listOf("b", "c", "d"), conf.getList("a"))
    }

    @Test
    fun testOriginalsWithPrefix() {
        val props = Properties()
        props["foo.bar"] = "abc"
        props["setting"] = "def"
        val config = TestConfig(props)
        val originalsWithPrefix = config.originalsWithPrefix("foo.")
        assertTrue(config.unused().contains("foo.bar"))
        originalsWithPrefix["bar"]
        assertFalse(config.unused().contains("foo.bar"))
        val expected: MutableMap<String, Any> = HashMap()
        expected["bar"] = "abc"
        assertEquals(expected, originalsWithPrefix)
    }

    @Test
    fun testValuesWithPrefixOverride() {
        val prefix = "prefix."
        val props = mapOf(
            "sasl.mechanism" to "PLAIN",
            "prefix.sasl.mechanism" to "GSSAPI",
            "prefix.sasl.kerberos.kinit.cmd" to "/usr/bin/kinit2",
            "prefix.ssl.truststore.location" to "my location",
            "sasl.kerberos.service.name" to "service name",
            "ssl.keymanager.algorithm" to "algorithm",
        )
        val config = TestSecurityConfig(props)
        val valuesWithPrefixOverride = config.valuesWithPrefixOverride(prefix)

        // prefix overrides global
        assertTrue(config.unused().contains("prefix.sasl.mechanism"))
        assertTrue(config.unused().contains("sasl.mechanism"))
        assertEquals("GSSAPI", valuesWithPrefixOverride["sasl.mechanism"])
        assertFalse(config.unused().contains("sasl.mechanism"))
        assertFalse(config.unused().contains("prefix.sasl.mechanism"))

        // prefix overrides default
        assertTrue(config.unused().contains("prefix.sasl.kerberos.kinit.cmd"))
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"))
        assertEquals("/usr/bin/kinit2", valuesWithPrefixOverride["sasl.kerberos.kinit.cmd"])
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"))
        assertFalse(config.unused().contains("prefix.sasl.kerberos.kinit.cmd"))

        // prefix override with no default
        assertTrue(config.unused().contains("prefix.ssl.truststore.location"))
        assertFalse(config.unused().contains("ssl.truststore.location"))
        assertEquals("my location", valuesWithPrefixOverride["ssl.truststore.location"])
        assertFalse(config.unused().contains("ssl.truststore.location"))
        assertFalse(config.unused().contains("prefix.ssl.truststore.location"))

        // global overrides default
        assertTrue(config.unused().contains("ssl.keymanager.algorithm"))
        assertEquals("algorithm", valuesWithPrefixOverride["ssl.keymanager.algorithm"])
        assertFalse(config.unused().contains("ssl.keymanager.algorithm"))

        // global with no default
        assertTrue(config.unused().contains("sasl.kerberos.service.name"))
        assertEquals("service name", valuesWithPrefixOverride["sasl.kerberos.service.name"])
        assertFalse(config.unused().contains("sasl.kerberos.service.name"))

        // unset with default
        assertFalse(config.unused().contains("sasl.kerberos.min.time.before.relogin"))
        assertEquals(
            SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
            valuesWithPrefixOverride["sasl.kerberos.min.time.before.relogin"],
        )
        assertFalse(config.unused().contains("sasl.kerberos.min.time.before.relogin"))

        // unset with no default
        assertFalse(config.unused().contains("ssl.key.password"))
        assertNull(valuesWithPrefixOverride["ssl.key.password"])
        assertFalse(config.unused().contains("ssl.key.password"))
    }

    @Test
    fun testValuesWithSecondaryPrefix() {
        val prefix = "listener.name.listener1."
        val saslJaasConfig1 = Password("test.myLoginModule1 required;")
        val (value) = Password("test.myLoginModule2 required;")
        val saslJaasConfig3 = Password("test.myLoginModule3 required;")
        val props = mapOf(
            "listener.name.listener1.test-mechanism.sasl.jaas.config" to saslJaasConfig1.value,
            "test-mechanism.sasl.jaas.config" to value,
            "sasl.jaas.config" to saslJaasConfig3.value,
            "listener.name.listener1.gssapi.sasl.kerberos.kinit.cmd" to "/usr/bin/kinit2",
            "listener.name.listener1.gssapi.sasl.kerberos.service.name" to "testkafka",
            "listener.name.listener1.gssapi.sasl.kerberos.min.time.before.relogin" to "60000",
            "ssl.provider" to "TEST",
        )
        val config = TestSecurityConfig(props)
        val valuesWithPrefixOverride = config.valuesWithPrefixOverride(prefix)

        // prefix with mechanism overrides global
        assertTrue(config.unused().contains("listener.name.listener1.test-mechanism.sasl.jaas.config"))
        assertTrue(config.unused().contains("test-mechanism.sasl.jaas.config"))
        assertEquals(saslJaasConfig1, valuesWithPrefixOverride["test-mechanism.sasl.jaas.config"])
        assertEquals(saslJaasConfig3, valuesWithPrefixOverride["sasl.jaas.config"])
        assertFalse(config.unused().contains("listener.name.listener1.test-mechanism.sasl.jaas.config"))
        assertFalse(config.unused().contains("test-mechanism.sasl.jaas.config"))
        assertFalse(config.unused().contains("sasl.jaas.config"))

        // prefix with mechanism overrides default
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"))
        assertTrue(config.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.kinit.cmd"))
        assertFalse(config.unused().contains("gssapi.sasl.kerberos.kinit.cmd"))
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"))
        assertEquals("/usr/bin/kinit2", valuesWithPrefixOverride["gssapi.sasl.kerberos.kinit.cmd"])
        assertFalse(config.unused().contains("listener.name.listener1.sasl.kerberos.kinit.cmd"))

        // prefix override for mechanism with no default
        assertFalse(config.unused().contains("sasl.kerberos.service.name"))
        assertTrue(config.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"))
        assertFalse(config.unused().contains("gssapi.sasl.kerberos.service.name"))
        assertFalse(config.unused().contains("sasl.kerberos.service.name"))
        assertEquals("testkafka", valuesWithPrefixOverride["gssapi.sasl.kerberos.service.name"])
        assertFalse(config.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"))

        // unset with no default
        assertTrue(config.unused().contains("ssl.provider"))
        assertNull(valuesWithPrefixOverride["gssapi.ssl.provider"])
        assertTrue(config.unused().contains("ssl.provider"))
    }

    @Test
    fun testValuesWithPrefixAllOrNothing() {
        val prefix1 = "prefix1."
        val prefix2 = "prefix2."
        val props = mapOf(
            "sasl.mechanism" to "PLAIN",
            "prefix1.sasl.mechanism" to "GSSAPI",
            "prefix1.sasl.kerberos.kinit.cmd" to "/usr/bin/kinit2",
            "prefix1.ssl.truststore.location" to "my location",
            "sasl.kerberos.service.name" to "service name",
            "ssl.keymanager.algorithm" to "algorithm",
        )
        val config = TestSecurityConfig(props)
        val valuesWithPrefixAllOrNothing1 = config.valuesWithPrefixAllOrNothing(prefix1)

        // All prefixed values are there
        assertEquals("GSSAPI", valuesWithPrefixAllOrNothing1["sasl.mechanism"])
        assertEquals("/usr/bin/kinit2", valuesWithPrefixAllOrNothing1["sasl.kerberos.kinit.cmd"])
        assertEquals("my location", valuesWithPrefixAllOrNothing1["ssl.truststore.location"])

        // Non-prefixed values are missing
        assertFalse(valuesWithPrefixAllOrNothing1.containsKey("sasl.kerberos.service.name"))
        assertFalse(valuesWithPrefixAllOrNothing1.containsKey("ssl.keymanager.algorithm"))
        val valuesWithPrefixAllOrNothing2 = config.valuesWithPrefixAllOrNothing(prefix2)
        assertTrue(valuesWithPrefixAllOrNothing2.containsKey("sasl.kerberos.service.name"))
        assertTrue(valuesWithPrefixAllOrNothing2.containsKey("ssl.keymanager.algorithm"))
    }

    @Test
    fun testUnusedConfigs() {
        val props = Properties()
        val configValue = "org.apache.kafka.common.config.AbstractConfigTest\$ConfiguredFakeMetricsReporter"
        props[TestConfig.METRIC_REPORTER_CLASSES_CONFIG] = configValue
        props[ConfiguredFakeMetricsReporter.EXTRA_CONFIG] = "my_value"
        val config = TestConfig(props)
        assertTrue(
            config.unused().contains(ConfiguredFakeMetricsReporter.EXTRA_CONFIG),
            ConfiguredFakeMetricsReporter.EXTRA_CONFIG +
                    " should be marked unused before getConfiguredInstances is called",
        )
        config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter::class.java)
        assertFalse(
            config.unused().contains(ConfiguredFakeMetricsReporter.EXTRA_CONFIG),
            "${ConfiguredFakeMetricsReporter.EXTRA_CONFIG} should be marked as used",
        )
    }

    private fun testValidInputs(configValue: String) {
        val props = Properties()
        props[TestConfig.METRIC_REPORTER_CLASSES_CONFIG] = configValue
        val config = TestConfig(props)
        try {
            config.getConfiguredInstances(
                TestConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter::class.java,
            )
        } catch (_: ConfigException) {
            fail("No exceptions are expected here, valid props are :$props")
        }
    }

    private fun testInvalidInputs(configValue: String) {
        val props = Properties()
        props[TestConfig.METRIC_REPORTER_CLASSES_CONFIG] = configValue
        val config = TestConfig(props)
        try {
            config.getConfiguredInstances(
                TestConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter::class.java
            )
            fail("Expected a config exception due to invalid props :$props")
        } catch (_: KafkaException) {
            // this is good
        }
    }

    @Test
    fun testConfiguredInstancesClosedOnFailure() {
        try {
            val props: MutableMap<String, String> = HashMap()
            val threeConsumerInterceptors = MockConsumerInterceptor::class.java.getName() + ", " +
                    MockConsumerInterceptor::class.java.getName() + ", " +
                    MockConsumerInterceptor::class.java.getName()
            props[TestConfig.METRIC_REPORTER_CLASSES_CONFIG] = threeConsumerInterceptors
            props["client.id"] = "test"
            val testConfig = TestConfig(props)
            MockConsumerInterceptor.setThrowOnConfigExceptionThreshold(3)

            assertFailsWith<Exception> {
                testConfig.getConfiguredInstances(
                    TestConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    Any::class.java
                )
            }
            assertEquals(3, MockConsumerInterceptor.CONFIG_COUNT.get())
            assertEquals(3, MockConsumerInterceptor.CLOSE_COUNT.get())
        } finally {
            MockConsumerInterceptor.resetCounters()
        }
    }

    @Test
    fun testClassConfigs() {
        class RestrictedClassLoader : ClassLoader(null) {
            @Throws(ClassNotFoundException::class)
            override fun findClass(name: String): Class<*> {
                return if (
                    name == ClassTestConfig.DEFAULT_CLASS.getName() ||
                    name == ClassTestConfig.RESTRICTED_CLASS.getName()
                ) throw ClassNotFoundException()
                else ClassTestConfig::class.java.getClassLoader().loadClass(name)
            }
        }

        val restrictedClassLoader: ClassLoader = RestrictedClassLoader()
        val defaultClassLoader = AbstractConfig::class.java.getClassLoader()
        val originClassLoader = Thread.currentThread().getContextClassLoader()
        try {
            // Test default classloading where all classes are visible to thread context classloader
            Thread.currentThread().setContextClassLoader(defaultClassLoader)
            var testConfig = ClassTestConfig()
            testConfig.checkInstances(ClassTestConfig.DEFAULT_CLASS, ClassTestConfig.DEFAULT_CLASS)

            // Test default classloading where default classes are not visible to thread context classloader
            // Static classloading is used for default classes, so instance creation should succeed.
            Thread.currentThread().setContextClassLoader(restrictedClassLoader)
            testConfig = ClassTestConfig()
            testConfig.checkInstances(ClassTestConfig.DEFAULT_CLASS, ClassTestConfig.DEFAULT_CLASS)

            // Test class overrides with names or classes where all classes are visible to thread context classloader
            Thread.currentThread().setContextClassLoader(defaultClassLoader)
            ClassTestConfig.testOverrides()

            // Test class overrides with names or classes where all classes are visible to Kafka classloader,
            // context classloader is null
            Thread.currentThread().setContextClassLoader(null)
            ClassTestConfig.testOverrides()

            // Test class overrides where some classes are not visible to thread context classloader
            Thread.currentThread().setContextClassLoader(restrictedClassLoader)
            // Properties specified as classes should succeed
            testConfig = ClassTestConfig(ClassTestConfig.RESTRICTED_CLASS, listOf(ClassTestConfig.RESTRICTED_CLASS))
            testConfig.checkInstances(ClassTestConfig.RESTRICTED_CLASS, ClassTestConfig.RESTRICTED_CLASS)
            testConfig = ClassTestConfig(
                ClassTestConfig.RESTRICTED_CLASS,
                listOf(ClassTestConfig.VISIBLE_CLASS, ClassTestConfig.RESTRICTED_CLASS)
            )
            testConfig.checkInstances(
                ClassTestConfig.RESTRICTED_CLASS,
                ClassTestConfig.VISIBLE_CLASS,
                ClassTestConfig.RESTRICTED_CLASS
            )

            // Properties specified as classNames should fail to load classes
            assertFailsWith<ConfigException>("Config created with class property that cannot be loaded") {
                ClassTestConfig(ClassTestConfig.RESTRICTED_CLASS.getName(), null)
            }
            val config = ClassTestConfig(
                null,
                listOf(ClassTestConfig.VISIBLE_CLASS.getName(), ClassTestConfig.RESTRICTED_CLASS.getName())
            )
            assertFailsWith<KafkaException>("Should have failed to load class") {
                config.getConfiguredInstances("list.prop", MetricsReporter::class.java)
            }
            val config2 = ClassTestConfig(
                null,
                ClassTestConfig.VISIBLE_CLASS.getName() + "," + ClassTestConfig.RESTRICTED_CLASS.getName()
            )
            assertFailsWith<KafkaException>("Should have failed to load class") {
                config2.getConfiguredInstances("list.prop", MetricsReporter::class.java)
            }
        } finally {
            Thread.currentThread().setContextClassLoader(originClassLoader)
        }
    }

    @Test
    fun testOriginalWithOverrides() {
        val props = Properties()
        props["config.providers"] = "file"
        val config = TestIndirectConfigResolution(props)
        assertEquals(config.originals()["config.providers"], "file")
        assertEquals(
            expected = config.originals(mapOf("config.providers" to "file2"))["config.providers"],
            actual = "file2",
        )
    }

    @Test
    fun testOriginalsWithConfigProvidersProps() {
        val props = Properties()

        // Test Case: Valid Test Case for ConfigProviders as part of config.properties
        props["config.providers"] = "file"
        props["config.providers.file.class"] = MockFileConfigProvider::class.java.getName()
        val id = UUID.randomUUID().toString()
        props["config.providers.file.param.testId"] = id
        props["prefix.ssl.truststore.location.number"] = 5
        props["sasl.kerberos.service.name"] = "service name"
        props["sasl.kerberos.key"] = "\${file:/usr/kerberos:key}"
        props["sasl.kerberos.password"] = "\${file:/usr/kerberos:password}"
        val config = TestIndirectConfigResolution(props)
        assertEquals("testKey", config.originals()["sasl.kerberos.key"])
        assertEquals("randomPassword", config.originals()["sasl.kerberos.password"])
        assertEquals(5, config.originals()["prefix.ssl.truststore.location.number"])
        assertEquals("service name", config.originals()["sasl.kerberos.service.name"])
        MockFileConfigProvider.assertClosed(id)
    }

    @Test
    fun testConfigProvidersPropsAsParam() {
        // Test Case: Valid Test Case for ConfigProviders as a separate variable
        val providers = Properties()
        providers["config.providers"] = "file"
        providers["config.providers.file.class"] = MockFileConfigProvider::class.java.getName()
        val id = UUID.randomUUID().toString()
        providers["config.providers.file.param.testId"] = id
        val props = Properties()
        props["sasl.kerberos.key"] = "\${file:/usr/kerberos:key}"
        props["sasl.kerberos.password"] = "\${file:/usr/kerberos:password}"
        val config = TestIndirectConfigResolution(
            convertPropertiesToMap(props),
            convertPropertiesToMap(providers),
        )
        assertEquals("testKey", config.originals()["sasl.kerberos.key"])
        assertEquals("randomPassword", config.originals()["sasl.kerberos.password"])
        MockFileConfigProvider.assertClosed(id)
    }

    @Test
    fun testImmutableOriginalsWithConfigProvidersProps() {
        // Test Case: Valid Test Case for ConfigProviders as a separate variable
        val providers = Properties()
        providers["config.providers"] = "file"
        providers["config.providers.file.class"] = MockFileConfigProvider::class.java.getName()
        val id = UUID.randomUUID().toString()
        providers["config.providers.file.param.testId"] = id
        val props = Properties()
        props["sasl.kerberos.key"] = "\${file:/usr/kerberos:key}"
        val immutableMap = convertPropertiesToMap(props)
        val provMap = convertPropertiesToMap(providers)
        val config = TestIndirectConfigResolution(immutableMap, provMap)
        assertEquals("testKey", config.originals()["sasl.kerberos.key"])
        MockFileConfigProvider.assertClosed(id)
    }

    @Test
    fun testAutoConfigResolutionWithMultipleConfigProviders() {
        // Test Case: Valid Test Case With Multiple ConfigProviders as a separate variable
        val providers = Properties()
        providers["config.providers"] = "file,vault"
        providers["config.providers.file.class"] = MockFileConfigProvider::class.java.getName()
        val id = UUID.randomUUID().toString()
        providers["config.providers.file.param.testId"] = id
        providers["config.providers.vault.class"] = MockVaultConfigProvider::class.java.getName()
        val props = Properties()
        props["sasl.kerberos.key"] = "\${file:/usr/kerberos:key}"
        props["sasl.kerberos.password"] = "\${file:/usr/kerberos:password}"
        props["sasl.truststore.key"] = "\${vault:/usr/truststore:truststoreKey}"
        props["sasl.truststore.password"] = "\${vault:/usr/truststore:truststorePassword}"
        val config = TestIndirectConfigResolution(
            convertPropertiesToMap(props),
            convertPropertiesToMap(providers),
        )
        assertEquals("testKey", config.originals()["sasl.kerberos.key"])
        assertEquals("randomPassword", config.originals()["sasl.kerberos.password"])
        assertEquals("testTruststoreKey", config.originals()["sasl.truststore.key"])
        assertEquals("randomtruststorePassword", config.originals()["sasl.truststore.password"])
        MockFileConfigProvider.assertClosed(id)
    }

    @Test
    fun testAutoConfigResolutionWithInvalidConfigProviderClass() {
        // Test Case: Invalid class for Config Provider
        val props = Properties()
        props["config.providers"] = "file"
        props["config.providers.file.class"] = "org.apache.kafka.common.config.provider.InvalidConfigProvider"
        props["testKey"] = "\${test:/foo/bar/testpath:testKey}"
        try {
            TestIndirectConfigResolution(props)
            fail("Expected a config exception due to invalid props :$props")
        } catch (_: KafkaException) {
            // this is good
        }
    }

    @Test
    fun testAutoConfigResolutionWithMissingConfigProvider() {
        // Test Case: Config Provider for a variable missing in config file.
        val props = Properties()
        props["testKey"] = "\${test:/foo/bar/testpath:testKey}"
        val config = TestIndirectConfigResolution(props)
        assertEquals("\${test:/foo/bar/testpath:testKey}", config.originals()["testKey"])
    }

    @Test
    fun testAutoConfigResolutionWithMissingConfigKey() {
        // Test Case: Config Provider fails to resolve the config (key not present)
        val props = Properties()
        props["config.providers"] = "test"
        props["config.providers.test.class"] = MockFileConfigProvider::class.java.getName()
        val id = UUID.randomUUID().toString()
        props["config.providers.test.param.testId"] = id
        props["random"] = "\${test:/foo/bar/testpath:random}"
        val config = TestIndirectConfigResolution(props)
        assertEquals("\${test:/foo/bar/testpath:random}", config.originals()["random"])
        MockFileConfigProvider.assertClosed(id)
    }

    @Test
    fun testAutoConfigResolutionWithDuplicateConfigProvider() {
        // Test Case: If ConfigProvider is provided in both originals and provider. Only the ones in provider
        // should be used.
        val providers = Properties()
        providers["config.providers"] = "test"
        providers["config.providers.test.class"] = MockVaultConfigProvider::class.java.getName()
        val props = Properties()
        props["sasl.kerberos.key"] = "\${file:/usr/kerberos:key}"
        props["config.providers"] = "file"
        props["config.providers.file.class"] = MockVaultConfigProvider::class.java.getName()
        val config = TestIndirectConfigResolution(
            convertPropertiesToMap(props),
            convertPropertiesToMap(providers),
        )
        assertEquals("\${file:/usr/kerberos:key}", config.originals()["sasl.kerberos.key"])
    }

    @Test
    fun testConfigProviderConfigurationWithConfigParams() {
        // Test Case: Valid Test Case With Multiple ConfigProviders as a separate variable
        val providers = Properties()
        providers["config.providers"] = "vault"
        providers["config.providers.vault.class"] = MockVaultConfigProvider::class.java.getName()
        providers["config.providers.vault.param.key"] = "randomKey"
        providers["config.providers.vault.param.location"] = "/usr/vault"
        val props = Properties()
        props["sasl.truststore.key"] = "\${vault:/usr/truststore:truststoreKey}"
        props["sasl.truststore.password"] = "\${vault:/usr/truststore:truststorePassword}"
        props["sasl.truststore.location"] = "\${vault:/usr/truststore:truststoreLocation}"
        val config = TestIndirectConfigResolution(
            convertPropertiesToMap(props),
            convertPropertiesToMap(providers),
        )
        assertEquals("/usr/vault", config.originals()["sasl.truststore.location"])
    }

    @Test
    fun testDocumentationOf() {
        val props = Properties()
        val config = TestIndirectConfigResolution(props)
        assertEquals(
            TestIndirectConfigResolution.INDIRECT_CONFIGS_DOC,
            config.documentationOf(TestIndirectConfigResolution.INDIRECT_CONFIGS)
        )
    }

    @Test
    fun testDocumentationOfExpectNull() {
        val props = Properties()
        val config = TestIndirectConfigResolution(props)
        assertNull(config.documentationOf("xyz"))
    }

    private class TestIndirectConfigResolution : AbstractConfig {

        constructor(props: Properties) : this(props = convertPropertiesToMap(props))

        constructor(props: Map<String, Any?>) : super(definition = CONFIG, originals = props, doLog = true)
        
        constructor(
            props: Map<String, Any?>,
            providers: Map<String, Any?>,
        ) : super(CONFIG, props, providers, true)

        companion object {

            const val INDIRECT_CONFIGS = "indirect.variables"

            const val INDIRECT_CONFIGS_DOC = "Variables whose values can be obtained from ConfigProviders"

            private val CONFIG: ConfigDef = ConfigDef().define(
                name = INDIRECT_CONFIGS,
                type = ConfigDef.Type.LIST,
                defaultValue = "",
                importance = Importance.LOW,
                documentation = INDIRECT_CONFIGS_DOC,
            )
        }
    }

    private class ClassTestConfig : AbstractConfig {
        
        constructor() : super(CONFIG, emptyMap())
        
        constructor(classPropOverride: Any?, listPropOverride: Any?) : super(
            CONFIG,
            overrideProps(classPropOverride, listPropOverride)
        )

        fun checkInstances(expectedClassPropClass: Class<*>?, vararg expectedListPropClasses: Class<*>?) {
            assertEquals(
                expectedClassPropClass, getConfiguredInstance(
                    "class.prop",
                    MetricsReporter::class.java
                )!!.javaClass
            )
            val list: List<*> = getConfiguredInstances(
                "list.prop",
                MetricsReporter::class.java
            )
            for (i in list.indices) assertEquals(expectedListPropClasses[i], list[i]!!.javaClass)
        }

        companion object {
            
            val DEFAULT_CLASS: Class<*> = FakeMetricsReporter::class.java
            
            val VISIBLE_CLASS: Class<*> = JmxReporter::class.java
            
            val RESTRICTED_CLASS: Class<*> = ConfiguredFakeMetricsReporter::class.java
            
            private val CONFIG: ConfigDef = ConfigDef().define(
                name = "class.prop",
                type = ConfigDef.Type.CLASS,
                defaultValue = DEFAULT_CLASS,
                importance = Importance.HIGH,
                documentation = "docs",
            ).define(
                name = "list.prop",
                type = ConfigDef.Type.LIST,
                defaultValue = listOf(DEFAULT_CLASS),
                importance = Importance.HIGH,
                documentation = "docs"
            )

            fun testOverrides() {
                val testConfig1 = ClassTestConfig(RESTRICTED_CLASS, listOf(VISIBLE_CLASS, RESTRICTED_CLASS))
                testConfig1.checkInstances(RESTRICTED_CLASS, VISIBLE_CLASS, RESTRICTED_CLASS)
                val testConfig2 = ClassTestConfig(
                    RESTRICTED_CLASS.getName(),
                    listOf(VISIBLE_CLASS.getName(), RESTRICTED_CLASS.getName())
                )
                testConfig2.checkInstances(RESTRICTED_CLASS, VISIBLE_CLASS, RESTRICTED_CLASS)
                val testConfig3 = ClassTestConfig(
                    RESTRICTED_CLASS.getName(),
                    VISIBLE_CLASS.getName() + "," + RESTRICTED_CLASS.getName()
                )
                testConfig3.checkInstances(RESTRICTED_CLASS, VISIBLE_CLASS, RESTRICTED_CLASS)
            }

            private fun overrideProps(classProp: Any?, listProp: Any?): Map<String, Any?> {
                val props: MutableMap<String, Any?> = HashMap()
                if (classProp != null) props["class.prop"] = classProp
                if (listProp != null) props["list.prop"] = listProp
                return props
            }
        }
    }

    private class TestConfig(props: Map<String, Any?>) : AbstractConfig(CONFIG, props) {

        constructor(props: Properties) : this(props = Utils.propsToMap(props))

        companion object {

            const val METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters"

            private const val METRIC_REPORTER_CLASSES_DOC = "A list of classes to use as metrics reporters."

            private val CONFIG: ConfigDef = ConfigDef().define(
                name = METRIC_REPORTER_CLASSES_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = "",
                importance = Importance.LOW,
                documentation = METRIC_REPORTER_CLASSES_DOC,
            )
        }
    }

    class ConfiguredFakeMetricsReporter : FakeMetricsReporter() {

        override fun configure(configs: Map<String, Any?>) {
            // Calling get() should have the side effect of marking that config as used.
            // this is required by testUnusedConfigs
            configs[EXTRA_CONFIG]
        }

        companion object {
            const val EXTRA_CONFIG = "metric.extra_config"
        }
    }

    companion object {

        fun convertPropertiesToMap(props: Map<*, *>): Map<String, Any?> {
            for ((key, value) in props) {
                if (key !is String) throw ConfigException(
                    name = key.toString(),
                    value = value,
                    message = "Key must be a string.",
                )
            }

            return props.asSequence()
                .map { it.key.toString() to it.value }
                .toMap()
        }
    }
}
