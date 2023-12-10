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

package org.apache.kafka.common.security.ssl

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.security.GeneralSecurityException
import java.security.KeyPair
import java.security.KeyStore
import java.security.Security
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.TestSecurityConfig
import org.apache.kafka.common.security.auth.SslEngineFactory
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.FileBasedStore
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PemStore
import org.apache.kafka.common.security.ssl.SslFactory.CertificateEntries.Companion.ensureCompatible
import org.apache.kafka.common.security.ssl.mock.TestKeyManagerFactory
import org.apache.kafka.common.security.ssl.mock.TestProviderCreator
import org.apache.kafka.common.security.ssl.mock.TestTrustManagerFactory
import org.apache.kafka.test.TestSslUtils.CertificateBuilder
import org.apache.kafka.test.TestSslUtils.SslConfigsBuilder
import org.apache.kafka.test.TestSslUtils.TestSslEngineFactory
import org.apache.kafka.test.TestSslUtils.createSslConfig
import org.apache.kafka.test.TestSslUtils.generateKeyPair
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

abstract class SslFactoryTest(private val tlsProtocol: String) {
    
    @Test
    @Throws(Exception::class)
    fun testSslFactoryConfiguration() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val serverSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(serverSslConfig)
        //host and port are hints
        val engine = sslFactory.createSslEngine(peerHost = "localhost", peerPort = 0)
        assertNotNull(engine)
        assertEquals(setOf(tlsProtocol), engine.enabledProtocols.toSet())
        assertFalse(engine.useClientMode)
    }

    @Test
    fun testSslFactoryWithCustomKeyManagerConfiguration() {
        val testProviderCreator = TestProviderCreator()
        val serverSslConfig = createSslConfig(
            keyManagerAlgorithm = TestKeyManagerFactory.ALGORITHM,
            trustManagerAlgorithm = TestTrustManagerFactory.ALGORITHM,
            tlsProtocol = tlsProtocol,
        ).toMutableMap()
        serverSslConfig[SecurityConfig.SECURITY_PROVIDERS_CONFIG] = testProviderCreator.javaClass.getName()
        
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(serverSslConfig)
        assertNotNull(sslFactory.sslEngineFactory, "SslEngineFactory not created")
        Security.removeProvider(testProviderCreator.provider.name)
    }

    @Test
    fun testSslFactoryWithoutProviderClassConfiguration() {
        // An exception is thrown as the algorithm is not registered through a provider
        val serverSslConfig = createSslConfig(
            keyManagerAlgorithm = TestKeyManagerFactory.ALGORITHM,
            trustManagerAlgorithm = TestTrustManagerFactory.ALGORITHM,
            tlsProtocol = tlsProtocol
        )
        val sslFactory = SslFactory(Mode.SERVER)
        assertFailsWith<KafkaException> { sslFactory.configure(serverSslConfig) }
    }

    @Test
    fun testSslFactoryWithIncorrectProviderClassConfiguration() {
        // An exception is thrown as the algorithm is not registered through a provider
        val serverSslConfig = createSslConfig(
            keyManagerAlgorithm = TestKeyManagerFactory.ALGORITHM,
            trustManagerAlgorithm = TestTrustManagerFactory.ALGORITHM,
            tlsProtocol = tlsProtocol,
        ).toMutableMap()
        serverSslConfig[SecurityConfig.SECURITY_PROVIDERS_CONFIG] = "com.fake.ProviderClass1,com.fake.ProviderClass2"
        
        val sslFactory = SslFactory(Mode.SERVER)
        assertFailsWith<KafkaException> { sslFactory.configure(serverSslConfig) }
    }

    @Test
    @Throws(Exception::class)
    fun testSslFactoryWithoutPasswordConfiguration() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val serverSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
            .toMutableMap()
        // unset the password
        serverSslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
        val sslFactory = SslFactory(Mode.SERVER)
        try {
            sslFactory.configure(serverSslConfig)
        } catch (e: Exception) {
            fail("An exception was thrown when configuring the truststore without a password: $e")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testClientMode() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
            .createNewTrustStore(trustStoreFile)
            .useClientCert(false)
            .build()
        val sslFactory = SslFactory(Mode.CLIENT)
        sslFactory.configure(clientSslConfig)
        
        //host and port are hints
        val engine = sslFactory.createSslEngine(peerHost = "localhost", peerPort = 0)
        assertTrue(engine.useClientMode)
    }

    @Test
    @Throws(IOException::class, GeneralSecurityException::class)
    fun staleSslEngineFactoryShouldBeClosed() {
        var trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        var clientSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .useClientCert(false)
            .build()
            .toMutableMap()
        clientSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(clientSslConfig)
        val sslEngineFactory = sslFactory.sslEngineFactory as TestSslEngineFactory
        assertNotNull(sslEngineFactory)
        assertFalse(sslEngineFactory.closed)
        
        trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        clientSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
            .toMutableMap()
        clientSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java
        
        sslFactory.reconfigure(clientSslConfig)
        val newSslEngineFactory = sslFactory.sslEngineFactory
        assertNotEquals(sslEngineFactory, newSslEngineFactory)
        // the older one should be closed
        assertTrue(sslEngineFactory.closed)
    }

    @Test
    @Throws(Exception::class)
    fun testReconfiguration() {
        var trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        var sslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(sslConfig)
        var sslEngineFactory = sslFactory.sslEngineFactory
        assertNotNull(sslEngineFactory, "SslEngineFactory not created")

        // Verify that SslEngineFactory is not recreated on reconfigure() if config and
        // file are not changed
        sslFactory.reconfigure(sslConfig)
        assertSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory recreated unnecessarily")

        // Verify that the SslEngineFactory is recreated on reconfigure() if config is changed
        trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        sslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
        sslFactory.reconfigure(sslConfig)
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory

        // Verify that builder is recreated on reconfigure() if config is not changed, but truststore file was modified
        trustStoreFile.setLastModified(System.currentTimeMillis() + 10000)
        sslFactory.reconfigure(sslConfig)
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory

        // Verify that builder is recreated on reconfigure() if config is not changed, but keystore file was modified
        val keyStoreFile = File(sslConfig[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] as String?)
        keyStoreFile.setLastModified(System.currentTimeMillis() + 10000)
        sslFactory.reconfigure(sslConfig)
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory

        // Verify that builder is recreated after validation on reconfigure() if config is not changed, but keystore file was modified
        keyStoreFile.setLastModified(System.currentTimeMillis() + 15000)
        sslFactory.validateReconfiguration(sslConfig)
        sslFactory.reconfigure(sslConfig)
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory

        // Verify that the builder is not recreated if modification time cannot be determined
        keyStoreFile.setLastModified(System.currentTimeMillis() + 20000)
        Files.delete(keyStoreFile.toPath())
        sslFactory.reconfigure(sslConfig)
        assertSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory recreated unnecessarily")
    }

    @Test
    @Throws(Exception::class)
    fun testReconfigurationWithoutTruststore() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val sslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
            .toMutableMap()
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)

        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(sslConfig)
        val sslContext = (sslFactory.sslEngineFactory as DefaultSslEngineFactory).sslContext()
        assertNotNull(sslContext, "SSL context not created")
        assertSame(
            expected = sslContext,
            actual = (sslFactory.sslEngineFactory as DefaultSslEngineFactory).sslContext(),
            message = "SSL context recreated unnecessarily",
        )
        assertFalse(sslFactory.createSslEngine("localhost", 0).useClientMode)
        val sslConfig2 = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()

        assertFailsWith<ConfigException>(
            message = "Truststore configured dynamically for listener without previous truststore",
        ) { sslFactory.validateReconfiguration(sslConfig2) }
    }

    @Test
    @Throws(Exception::class)
    fun testReconfigurationWithoutKeystore() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        var sslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
            .toMutableMap()
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)

        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(sslConfig)
        val sslContext = (sslFactory.sslEngineFactory as DefaultSslEngineFactory).sslContext()
        assertNotNull(sslContext, "SSL context not created")
        assertSame(
            expected = sslContext,
            actual = (sslFactory.sslEngineFactory as DefaultSslEngineFactory).sslContext(),
            message = "SSL context recreated unnecessarily",
        )
        assertFalse(sslFactory.createSslEngine("localhost", 0).useClientMode)
        val newTrustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")

        sslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(newTrustStoreFile)
            .build()
            .toMutableMap()
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)
        sslFactory.reconfigure(sslConfig)
        assertNotSame(
            illegal = sslContext,
            actual = (sslFactory.sslEngineFactory as DefaultSslEngineFactory).sslContext(),
            message = "SSL context not recreated",
        )
        sslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(newTrustStoreFile)
            .build()
            .toMutableMap()

        assertFailsWith<ConfigException>(
            message = "Keystore configured dynamically for listener without previous keystore",
        ) { sslFactory.validateReconfiguration(sslConfig) }
    }

    @Test
    @Throws(Exception::class)
    fun testPemReconfiguration() {
        val props = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile = null)
            .usePem(true)
            .build()
            .toMutableMap()
        var sslConfig = TestSecurityConfig(props)
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(sslConfig.values())
        var sslEngineFactory: SslEngineFactory? = sslFactory.sslEngineFactory
        assertNotNull(sslEngineFactory, "SslEngineFactory not created")

        props["some.config"] = "some.value"
        sslConfig = TestSecurityConfig(props)
        sslFactory.reconfigure(sslConfig.values())

        assertSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory recreated unnecessarily")

        props[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] =
            Password((props[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] as Password).value + " ")
        sslConfig = TestSecurityConfig(props)
        sslFactory.reconfigure(sslConfig.values())

        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory

        props[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] =
            Password((props[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] as Password).value + " ")
        sslConfig = TestSecurityConfig(props)
        sslFactory.reconfigure(sslConfig.values())

        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory

        props[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] =
            Password((props[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] as Password).value + " ")
        sslConfig = TestSecurityConfig(props)
        sslFactory.reconfigure(sslConfig.values())

        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory, "SslEngineFactory not recreated")
        sslEngineFactory = sslFactory.sslEngineFactory
    }

    @Test
    @Throws(Exception::class)
    fun testKeyStoreTrustStoreValidation() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val serverSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .build()
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(serverSslConfig)
        assertNotNull(sslFactory.sslEngineFactory, "SslEngineFactory not created")
    }

    @Test
    @Throws(Exception::class)
    fun testUntrustedKeyStoreValidationFails() {
        val trustStoreFile1 = tempFile(prefix = "truststore1", suffix = ".jks")
        val trustStoreFile2 = tempFile(prefix = "truststore2", suffix = ".jks")
        val sslConfig1 = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile1)
            .build()
            .toMutableMap()
        val sslConfig2 = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile2)
            .build()
        val sslFactory = SslFactory(
            mode = Mode.SERVER,
            clientAuthConfigOverride = null,
            keystoreVerifiableUsingTruststore = true,
        )

        listOf(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
        ).forEach { key -> sslConfig1[key] = sslConfig2[key]!! }

        assertFailsWith<ConfigException>(
            message = "Validation did not fail with untrusted truststore",
        ) { sslFactory.configure(sslConfig1) }
    }

    @Test
    @Throws(Exception::class)
    fun testKeystoreVerifiableUsingTruststore() {
        verifyKeystoreVerifiableUsingTruststore(usePem = false, tlsProtocol = tlsProtocol)
    }

    @Test
    @Throws(Exception::class)
    fun testPemKeystoreVerifiableUsingTruststore() {
        verifyKeystoreVerifiableUsingTruststore(usePem = true, tlsProtocol = tlsProtocol)
    }

    @Throws(Exception::class)
    private fun verifyKeystoreVerifiableUsingTruststore(usePem: Boolean, tlsProtocol: String) {
        val trustStoreFile1 = if (usePem) null else tempFile(prefix = "truststore1", suffix = ".jks")
        val sslConfig1 = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile1)
            .usePem(usePem)
            .build()
        val sslFactory = SslFactory(
            mode = Mode.SERVER,
            clientAuthConfigOverride = null,
            keystoreVerifiableUsingTruststore = true,
        )
        sslFactory.configure(sslConfig1)

        val trustStoreFile2 = if (usePem) null else tempFile(prefix = "truststore2", suffix = ".jks")
        val sslConfig2 = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile2)
            .usePem(usePem)
            .build()
        // Verify that `createSSLContext` fails even if certificate from new keystore is trusted by
        // the new truststore, if certificate is not trusted by the existing truststore on the `SslFactory`.
        // This is to prevent both keystores and truststores to be modified simultaneously on an inter-broker
        // listener to stores that may not work with other brokers where the update hasn't yet been performed.
        assertFailsWith<ConfigException>(
            message = "ValidateReconfiguration did not fail as expected",
        ) { sslFactory.validateReconfiguration(sslConfig2) }
    }

    @Test
    @Throws(Exception::class)
    fun testCertificateEntriesValidation() {
        verifyCertificateEntriesValidation(usePem = false, tlsProtocol = tlsProtocol)
    }

    @Test
    @Throws(Exception::class)
    fun testPemCertificateEntriesValidation() {
        verifyCertificateEntriesValidation(usePem = true, tlsProtocol = tlsProtocol)
    }

    @Throws(Exception::class)
    private fun verifyCertificateEntriesValidation(usePem: Boolean, tlsProtocol: String?) {
        val trustStoreFile = if (usePem) null else tempFile(prefix = "truststore", suffix = ".jks")
        val serverSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .usePem(usePem)
            .build()

        val newTrustStoreFile = if (usePem) null else tempFile(prefix = "truststore", suffix = ".jks")
        val newCnConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(newTrustStoreFile)
            .cn("Another CN")
            .usePem(usePem)
            .build()

        val ks1 = sslKeyStore(serverSslConfig)
        val ks2 = sslKeyStore(serverSslConfig)
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2))

        // Use different alias name, validation should succeed
        ks2.setCertificateEntry("another", ks1.getCertificate("localhost"))
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2))
        val ks3 = sslKeyStore(newCnConfig)
        assertNotEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks3))
    }

    /**
     * Tests client side ssl.engine.factory configuration is used when specified
     */
    @Test
    @Throws(Exception::class)
    fun testClientSpecifiedSslEngineFactoryUsed() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
            .createNewTrustStore(trustStoreFile)
            .useClientCert(false)
            .build()
            .toMutableMap()
        clientSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java

        val sslFactory = SslFactory(Mode.CLIENT)
        sslFactory.configure(clientSslConfig)
        assertIs<TestSslEngineFactory>(
            value = sslFactory.sslEngineFactory,
            message = "SslEngineFactory must be of expected type",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testEngineFactoryClosed() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
            .createNewTrustStore(trustStoreFile)
            .useClientCert(false)
            .build()
            .toMutableMap()
        clientSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java

        val sslFactory = SslFactory(Mode.CLIENT)
        sslFactory.configure(clientSslConfig)
        val engine = sslFactory.sslEngineFactory as TestSslEngineFactory
        assertFalse(engine.closed)
        sslFactory.close()
        assertTrue(engine.closed)
    }

    /**
     * Tests server side ssl.engine.factory configuration is used when specified
     */
    @Test
    @Throws(Exception::class)
    fun testServerSpecifiedSslEngineFactoryUsed() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val serverSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(trustStoreFile)
            .useClientCert(false)
            .build()
            .toMutableMap()
        serverSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java

        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(serverSslConfig)
        assertIs<TestSslEngineFactory>(
            value = sslFactory.sslEngineFactory,
            message = "SslEngineFactory must be of expected type",
        )
    }

    /**
     * Tests invalid ssl.engine.factory configuration
     */
    @Test
    @Throws(Exception::class)
    fun testInvalidSslEngineFactory() {
        val trustStoreFile = tempFile(prefix = "truststore", suffix = ".jks")
        val clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
            .createNewTrustStore(trustStoreFile)
            .useClientCert(false)
            .build()
            .toMutableMap()
        clientSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = String::class.java

        val sslFactory = SslFactory(Mode.CLIENT)
        assertFailsWith<ClassCastException> { sslFactory.configure(clientSslConfig) }
    }

    @Test
    @Throws(IOException::class, GeneralSecurityException::class)
    fun testUsedConfigs() {
        val serverSslConfig = sslConfigsBuilder(Mode.SERVER)
            .createNewTrustStore(tempFile(prefix = "truststore", suffix = ".jks"))
            .useClientCert(false)
            .build()
            .toMutableMap()
        serverSslConfig[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] = TestSslEngineFactory::class.java

        val securityConfig = TestSecurityConfig(serverSslConfig)
        val sslFactory = SslFactory(Mode.SERVER)
        sslFactory.configure(securityConfig.values())
        assertFalse(securityConfig.unused().contains(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG))
    }

    @Test
    @Throws(java.lang.Exception::class)
    fun testDynamicUpdateCompatibility() {
        val keyPair = generateKeyPair("RSA")
        val ks = createKeyStore(
            keyPair = keyPair,
            commonName = "*.example.com",
            org = "Kafka",
            utf8 = true,
            dnsNames = arrayOf("localhost", "*.example.com"),
        )
        ensureCompatible(ks, ks)
        ensureCompatible(ks, createKeyStore(keyPair, "*.example.com", "Kafka", true, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, " *.example.com", " Kafka ", true, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.example.COM", "Kafka", true, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.EXAMPLE.COM", "KAFKA", true, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.EXAMPLE.COM", "Kafka", true, "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.EXAMPLE.COM", "Kafka", true, "localhost"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.example.com", "Kafka", false, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.example.COM", "Kafka", false, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.EXAMPLE.COM", "KAFKA", false, "localhost", "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.EXAMPLE.COM", "Kafka", false, "*.example.com"))
        ensureCompatible(ks, createKeyStore(keyPair, "*.EXAMPLE.COM", "Kafka", false, "localhost"))
        assertFailsWith<ConfigException> {
            ensureCompatible(
                newKeystore = ks,
                oldKeystore = createKeyStore(
                    keyPair = keyPair,
                    commonName = " *.example.com",
                    org = " Kafka ",
                    utf8 = false,
                    dnsNames = arrayOf("localhost", "*.example.com"),
                )
            )
        }
        assertFailsWith<ConfigException> {
            ensureCompatible(
                newKeystore = ks,
                oldKeystore = createKeyStore(
                    keyPair = keyPair,
                    commonName = "*.another.example.com",
                    org = "Kafka",
                    utf8 = true,
                    dnsNames = arrayOf("*.example.com"),
                ),
            )
        }
        assertFailsWith<ConfigException> {
            ensureCompatible(
                newKeystore = ks,
                oldKeystore = createKeyStore(
                    keyPair = keyPair,
                    commonName = "*.EXAMPLE.COM",
                    org = "Kafka",
                    utf8 = true,
                    dnsNames = arrayOf("*.another.example.com"),
                ),
            )
        }
    }

    @Throws(java.lang.Exception::class)
    private fun createKeyStore(
        keyPair: KeyPair,
        commonName: String,
        org: String,
        utf8: Boolean,
        vararg dnsNames: String,
    ): KeyStore {
        val cert = CertificateBuilder().sanDnsNames(*dnsNames)
            .generate(commonName, org, utf8, keyPair)
        val ks = KeyStore.getInstance("PKCS12")
        ks.load(null, null)
        ks.setKeyEntry("kafka", keyPair.private, null, arrayOf(cert))
        return ks
    }

    private fun sslKeyStore(sslConfig: Map<String, Any?>): KeyStore {
        val store = if (sslConfig[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] != null) FileBasedStore(
            type = sslConfig[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] as String,
            path = sslConfig[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] as String,
            password = sslConfig[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] as Password?,
            keyPassword = sslConfig[SslConfigs.SSL_KEY_PASSWORD_CONFIG] as Password?,
            isKeyStore = true,
        )
        else PemStore(
            certificateChain = sslConfig[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] as Password,
            privateKey = sslConfig[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] as Password,
            keyPassword = sslConfig[SslConfigs.SSL_KEY_PASSWORD_CONFIG] as Password?,
        )
        return store.get()
    }

    private fun sslConfigsBuilder(mode: Mode): SslConfigsBuilder = SslConfigsBuilder(mode).tlsProtocol(tlsProtocol)
}
