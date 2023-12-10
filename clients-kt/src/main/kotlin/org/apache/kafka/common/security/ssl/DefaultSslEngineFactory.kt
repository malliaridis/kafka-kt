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

import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.security.GeneralSecurityException
import java.security.Key
import java.security.KeyFactory
import java.security.KeyStore
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.security.spec.InvalidKeySpecException
import java.security.spec.PKCS8EncodedKeySpec
import java.util.*
import java.util.regex.Pattern
import javax.crypto.Cipher
import javax.crypto.EncryptedPrivateKeyInfo
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManagerFactory
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SslClientAuth
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.auth.SslEngineFactory
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.common.utils.Utils.readFileAsString
import org.slf4j.LoggerFactory

class DefaultSslEngineFactory : SslEngineFactory {

    private var configs: Map<String, *>? = null

    private var protocol: String? = null

    private var provider: String? = null

    private var kmfAlgorithm: String? = null

    private var tmfAlgorithm: String? = null

    private var keystore: SecurityStore? = null

    private var truststore: SecurityStore? = null

    private var cipherSuites: Array<String>? = null

    private var enabledProtocols: Array<String>? = null

    private var secureRandomImplementation: SecureRandom? = null

    private var sslContext: SSLContext? = null

    private lateinit var sslClientAuth: SslClientAuth

    override fun createClientSslEngine(
        peerHost: String,
        peerPort: Int,
        endpointIdentification: String?
    ): SSLEngine {
        return createSslEngine(
            mode = Mode.CLIENT,
            peerHost = peerHost,
            peerPort = peerPort,
            endpointIdentification = endpointIdentification,
        )
    }

    override fun createServerSslEngine(peerHost: String, peerPort: Int): SSLEngine {
        return createSslEngine(
            mode = Mode.SERVER,
            peerHost = peerHost,
            peerPort = peerPort,
            endpointIdentification = null,
        )
    }

    override fun shouldBeRebuilt(nextConfigs: Map<String, Any?>): Boolean {
        return if (nextConfigs != configs) true
        else if (truststore != null && truststore!!.modified()) true
        else keystore != null && keystore!!.modified()
    }

    override fun reconfigurableConfigs(): Set<String> {
        return SslConfigs.RECONFIGURABLE_CONFIGS
    }

    override fun keystore(): KeyStore? = keystore?.get()

    override fun truststore(): KeyStore? = if (truststore != null) truststore!!.get()!! else null

    override fun configure(configs: Map<String, *>) {
        this.configs = configs
        protocol = configs[SslConfigs.SSL_PROTOCOL_CONFIG] as String?
        provider = configs[SslConfigs.SSL_PROVIDER_CONFIG] as String?
        SecurityUtils.addConfiguredSecurityProviders(configs)

        val cipherSuitesList = configs[SslConfigs.SSL_CIPHER_SUITES_CONFIG] as List<String>?
        cipherSuites =
            if (!cipherSuitesList.isNullOrEmpty()) cipherSuitesList.toTypedArray<String>()
            else null

        val enabledProtocolsList = configs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] as List<String>?
        enabledProtocols =
            if (!enabledProtocolsList.isNullOrEmpty()) enabledProtocolsList.toTypedArray<String>()
            else null

        secureRandomImplementation = createSecureRandom(
            configs[SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG] as String?
        )

        sslClientAuth =
            createSslClientAuth(configs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] as String?)

        kmfAlgorithm = configs[SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG] as String?
        tmfAlgorithm = configs[SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG] as String?

        keystore = createKeystore(
            type = configs[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] as String?,
            path = configs[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] as String?,
            password = configs[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] as Password?,
            keyPassword = configs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] as Password?,
            privateKey = configs[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] as Password?,
            certificateChain = configs[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] as Password?
        )

        truststore = createTruststore(
            configs[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] as String?,
            configs[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] as String?,
            configs[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] as Password?,
            configs[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] as Password?
        )

        sslContext = createSSLContext(keystore, truststore)
    }

    override fun close() {
        sslContext = null
    }

    //For Test only
    fun sslContext(): SSLContext? = sslContext

    private fun createSslEngine(
        mode: Mode,
        peerHost: String,
        peerPort: Int,
        endpointIdentification: String?
    ): SSLEngine {
        val sslEngine = sslContext!!.createSSLEngine(peerHost, peerPort)
        if (cipherSuites != null) sslEngine.enabledCipherSuites = cipherSuites
        if (enabledProtocols != null) sslEngine.enabledProtocols = enabledProtocols
        if (mode === Mode.SERVER) {
            sslEngine.useClientMode = false
            when (sslClientAuth) {
                SslClientAuth.REQUIRED -> sslEngine.needClientAuth = true
                SslClientAuth.REQUESTED -> sslEngine.wantClientAuth = true
                SslClientAuth.NONE -> {}
            }
        } else {
            sslEngine.useClientMode = true
            val sslParams = sslEngine.sslParameters
            // SSLParameters#setEndpointIdentificationAlgorithm enables endpoint validation
            // only in client mode. Hence, validation is enabled only for clients.
            sslParams.endpointIdentificationAlgorithm = endpointIdentification
            sslEngine.sslParameters = sslParams
        }
        return sslEngine
    }

    private fun createSSLContext(keystore: SecurityStore?, truststore: SecurityStore?): SSLContext {
        return try {
            val sslContext: SSLContext = provider?.let { SSLContext.getInstance(it, it) }
                ?: SSLContext.getInstance(protocol)

            var keyManagers: Array<KeyManager?>? = null
            if (keystore != null || kmfAlgorithm != null) {
                val kmfAlgorithm = kmfAlgorithm ?: KeyManagerFactory.getDefaultAlgorithm()
                val kmf = KeyManagerFactory.getInstance(kmfAlgorithm)

                kmf.init(keystore?.get(), keystore?.keyPassword())

                keyManagers = kmf.keyManagers
            }

            val tmfAlgorithm = tmfAlgorithm ?: TrustManagerFactory.getDefaultAlgorithm()
            val tmf = TrustManagerFactory.getInstance(tmfAlgorithm)
            val ts = truststore?.get()
            tmf.init(ts)
            sslContext.init(keyManagers, tmf.trustManagers, secureRandomImplementation)

            log.debug(
                "Created SSL context with keystore {}, truststore {}, provider {}.",
                keystore,
                truststore,
                sslContext.provider.name,
            )

            sslContext
        } catch (e: Exception) {
            throw KafkaException(cause = e)
        }
    }

    // Visibility to override for testing
    internal fun createKeystore(
        type: String?,
        path: String?,
        password: Password?,
        keyPassword: Password?,
        privateKey: Password?,
        certificateChain: Password?
    ): SecurityStore? {
        return if (privateKey != null) {
            if (PEM_TYPE != type) throw InvalidConfigurationException(
                "SSL private key can be specified only for PEM, but key store type is $type."
            ) else if (certificateChain == null) throw InvalidConfigurationException(
                "SSL private key is specified, but certificate chain is not specified."
            ) else if (path != null) throw InvalidConfigurationException(
                "Both SSL key store location and separate private key are specified."
            ) else if (password != null) throw InvalidConfigurationException(
                "SSL key store password cannot be specified with PEM format, only key password may be specified."
            ) else PemStore(
                certificateChain = certificateChain,
                privateKey = privateKey,
                keyPassword = keyPassword,
            )
        } else if (certificateChain != null) throw InvalidConfigurationException(
            "SSL certificate chain is specified, but private key is not specified"
        ) else if (PEM_TYPE == type && path != null) {
            if (password != null) throw InvalidConfigurationException(
                "SSL key store password cannot be specified with PEM format, only key password may be specified"
            )
            else FileBasedPemStore(
                path = path,
                keyPassword = keyPassword,
                isKeyStore = true
            )
        } else if (path == null && password != null) throw InvalidConfigurationException(
            "SSL key store is not specified, but key store password is specified."
        ) else if (path != null && password == null) throw InvalidConfigurationException(
            "SSL key store is specified, but key store password is not specified."
        ) else if (path != null && password != null) FileBasedStore(
            type = type!!,
            path = path,
            password = password,
            keyPassword = keyPassword,
            isKeyStore = true,
        ) else null // path == null, clients may use this path with brokers that don't require client auth
    }

    internal interface SecurityStore {

        fun get(): KeyStore

        fun keyPassword(): CharArray?

        fun modified(): Boolean
    }

    // package access for testing
    internal open class FileBasedStore(
        type: String,
        path: String,
        password: Password? = null,
        keyPassword: Password? = null,
        isKeyStore: Boolean
    ) : SecurityStore {

        private val type: String?

        protected val path: String

        private val password: Password?

        protected val keyPassword: Password?

        private val fileLastModifiedMs: Long?

        private val keyStore: KeyStore

        init {
            this.type = type
            this.path = path
            this.password = password
            this.keyPassword = keyPassword
            fileLastModifiedMs = lastModifiedMs(path)
            keyStore = load(isKeyStore)
        }

        override fun get(): KeyStore {
            return keyStore
        }

        override fun keyPassword(): CharArray? {
            val passwd = keyPassword ?: password
            return passwd?.value?.toCharArray()
        }

        /**
         * Loads this keystore
         * @return the keystore
         * @throws KafkaException if the file could not be read or if the keystore could not be
         * loaded using the specified configs (e.g. if the password or keystore type is invalid)
         */
        protected open fun load(isKeyStore: Boolean): KeyStore {
            try {
                Files.newInputStream(Paths.get(path)).use { input ->
                    val ks = KeyStore.getInstance(type)
                    // If a password is not set access to the truststore is still available, but
                    // integrity checking is disabled.
                    val passwordChars = password?.value?.toCharArray()
                    ks.load(input, passwordChars)
                    return ks
                }
            } catch (e: GeneralSecurityException) {
                throw KafkaException("Failed to load SSL keystore $path of type $type", e)
            } catch (e: IOException) {
                throw KafkaException("Failed to load SSL keystore $path of type $type", e)
            }
        }

        private fun lastModifiedMs(path: String): Long? {
            return try {
                Files.getLastModifiedTime(Paths.get(path)).toMillis()
            } catch (e: IOException) {
                log.error("Modification time of key store could not be obtained: $path", e)
                null
            }
        }

        override fun modified(): Boolean {
            val modifiedMs = lastModifiedMs(path)
            return modifiedMs != null && modifiedMs != fileLastModifiedMs
        }

        override fun toString(): String {
            return "SecurityStore(" +
                    "path=$path" +
                    ", modificationTime=${fileLastModifiedMs?.let { Date(it) }}" +
                    ")"
        }
    }

    internal class FileBasedPemStore(
        path: String,
        keyPassword: Password?,
        isKeyStore: Boolean,
    ) : FileBasedStore(
        type = PEM_TYPE,
        path = path,
        keyPassword = keyPassword,
        isKeyStore = isKeyStore,
    ) {

        override fun load(isKeyStore: Boolean): KeyStore {
            return try {
                val storeContents = Password(readFileAsString(path))
                val pemStore = if (isKeyStore) PemStore(
                    storeContents,
                    storeContents,
                    keyPassword
                ) else PemStore(storeContents)
                pemStore.keyStore
            } catch (e: Exception) {
                throw InvalidConfigurationException("Failed to load PEM SSL keystore $path", e)
            }
        }
    }

    internal class PemStore : SecurityStore {

        private val keyPassword: CharArray?

        internal val keyStore: KeyStore

        constructor(certificateChain: Password, privateKey: Password, keyPassword: Password?) {
            this.keyPassword = keyPassword?.value?.toCharArray()
            keyStore = createKeyStoreFromPem(
                privateKey.value,
                certificateChain.value,
                this.keyPassword
            )
        }

        constructor(trustStoreCerts: Password) {
            keyPassword = null
            keyStore = createTrustStoreFromPem(trustStoreCerts.value)
        }

        override fun get(): KeyStore = keyStore

        override fun keyPassword(): CharArray? = keyPassword

        override fun modified(): Boolean = false

        private fun createKeyStoreFromPem(
            privateKeyPem: String,
            certChainPem: String,
            keyPassword: CharArray?
        ): KeyStore {
            return try {
                val ks = KeyStore.getInstance("PKCS12")
                ks.load(null, null)
                val key: Key = privateKey(privateKeyPem, keyPassword)
                val certChain = certs(certChainPem)
                ks.setKeyEntry("kafka", key, keyPassword, certChain)
                ks
            } catch (e: Exception) {
                throw InvalidConfigurationException("Invalid PEM keystore configs", e)
            }
        }

        private fun createTrustStoreFromPem(trustedCertsPem: String): KeyStore {
            return try {
                val ts = KeyStore.getInstance("PKCS12")
                ts.load(null, null)
                val certs = certs(trustedCertsPem)
                for (i in certs.indices) {
                    ts.setCertificateEntry("kafka$i", certs[i])
                }
                ts
            } catch (e: InvalidConfigurationException) {
                throw e
            } catch (e: Exception) {
                throw InvalidConfigurationException("Invalid PEM truststore configs", e)
            }
        }

        @Throws(GeneralSecurityException::class)
        private fun certs(pem: String): Array<Certificate?> {
            val certEntries = CERTIFICATE_PARSER.pemEntries(pem)
            if (certEntries.isEmpty()) throw InvalidConfigurationException(
                "At least one certificate expected, but none found"
            )

            val certs = arrayOfNulls<Certificate>(certEntries.size)
            certs.indices.forEach { index ->
                certs[index] = CertificateFactory.getInstance("X.509")
                    .generateCertificate(ByteArrayInputStream(certEntries[index]))
            }
            return certs
        }

        @Throws(Exception::class)
        private fun privateKey(pem: String, keyPassword: CharArray?): PrivateKey {
            val keyEntries = PRIVATE_KEY_PARSER.pemEntries(pem)

            if (keyEntries.isEmpty()) throw InvalidConfigurationException("Private key not provided")
            if (keyEntries.size != 1) throw InvalidConfigurationException(
                "Expected one private key, but found " + keyEntries.size
            )

            val keyBytes = keyEntries[0]
            val keySpec: PKCS8EncodedKeySpec =
                if (keyPassword == null) PKCS8EncodedKeySpec(keyBytes)
                else {
                    val keyInfo = EncryptedPrivateKeyInfo(keyBytes)
                    val algorithm = keyInfo.algName
                    val keyFactory = SecretKeyFactory.getInstance(algorithm)
                    val pbeKey = keyFactory.generateSecret(PBEKeySpec(keyPassword))
                    val cipher = Cipher.getInstance(algorithm)
                    cipher.init(Cipher.DECRYPT_MODE, pbeKey, keyInfo.algParameters)
                    keyInfo.getKeySpec(cipher)
                }

            var firstException: InvalidKeySpecException? = null
            KEY_FACTORIES.forEach { factory ->
                try {
                    return factory.generatePrivate(keySpec)
                } catch (e: InvalidKeySpecException) {
                    if (firstException == null) firstException = e
                }
            }
            throw InvalidConfigurationException("Private key could not be loaded", firstException)
        }

        companion object {
            private val CERTIFICATE_PARSER = PemParser("CERTIFICATE")
            private val PRIVATE_KEY_PARSER = PemParser("PRIVATE KEY")
            private val KEY_FACTORIES = listOf(
                keyFactory("RSA"),
                keyFactory("DSA"),
                keyFactory("EC")
            )

            private fun keyFactory(algorithm: String): KeyFactory {
                return try {
                    KeyFactory.getInstance(algorithm)
                } catch (e: Exception) {
                    throw InvalidConfigurationException(
                        "Could not create key factory for algorithm $algorithm",
                        e
                    )
                }
            }
        }
    }

    /**
     * Parser to process certificate/private key entries from PEM files
     * Examples:
     * -----BEGIN CERTIFICATE-----
     * Base64 cert
     * -----END CERTIFICATE-----
     *
     * -----BEGIN ENCRYPTED PRIVATE KEY-----
     * Base64 private key
     * -----END ENCRYPTED PRIVATE KEY-----
     * Additional data may be included before headers, so we match all entries within the PEM.
     */
    internal class PemParser(private val name: String) {

        private val pattern: Pattern

        init {
            val beginOrEndFormat = "-+%s\\s*.*%s[^-]*-+\\s+"
            val nameIgnoreSpace = name.replace(" ", "\\s+")
            val encodingParams = "\\s*[^\\r\\n]*:[^\\r\\n]*[\\r\\n]+"
            val base64Pattern = "([a-zA-Z0-9/+=\\s]*)"
            val patternStr = String.format(beginOrEndFormat, "BEGIN", nameIgnoreSpace) +
                    String.format("(?:%s)*", encodingParams) +
                    base64Pattern + String.format(beginOrEndFormat, "END", nameIgnoreSpace)

            pattern = Pattern.compile(patternStr)
        }

        internal fun pemEntries(pem: String): List<ByteArray> {
            val matcher = pattern.matcher(pem + "\n") // allow last newline to be omitted in value
            val entries: MutableList<ByteArray> = ArrayList()

            while (matcher.find()) {
                val base64Str = matcher.group(1).replace("\\s".toRegex(), "")
                entries.add(Base64.getDecoder().decode(base64Str))
            }

            if (entries.isEmpty())
                throw InvalidConfigurationException("No matching $name entries in PEM file")

            return entries
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(DefaultSslEngineFactory::class.java)

        const val PEM_TYPE = "PEM"

        private fun createSslClientAuth(key: String?): SslClientAuth {
            val auth = SslClientAuth.forConfig(key)
            if (auth != null) return auth

            log.warn("Unrecognized client authentication configuration {}. Falling " +
                    "back to NONE. Recognized client authentication configurations are {}.",
                key,
                SslClientAuth.values()
                    .map(SslClientAuth::name)
                    .joinToString(", "),
            )

            return SslClientAuth.NONE
        }

        private fun createSecureRandom(key: String?): SecureRandom? {
            return if (key == null) null
            else try {
                SecureRandom.getInstance(key)
            } catch (e: GeneralSecurityException) {
                throw KafkaException(cause = e)
            }
        }

        private fun createTruststore(
            type: String?,
            path: String?,
            password: Password?,
            trustStoreCerts: Password?
        ): SecurityStore? {
            return if (trustStoreCerts != null) {
                if (PEM_TYPE != type) throw InvalidConfigurationException(
                    "SSL trust store certs can be specified only for PEM, but trust store type is $type."
                ) else if (path != null) throw InvalidConfigurationException(
                    "Both SSL trust store location and separate trust certificates are specified."
                ) else if (password != null) throw InvalidConfigurationException(
                    "SSL trust store password cannot be specified for PEM format."
                ) else PemStore(trustStoreCerts)
            } else if (PEM_TYPE == type && path != null) {
                if (password != null) throw InvalidConfigurationException(
                    "SSL trust store password cannot be specified for PEM format."
                ) else FileBasedPemStore(
                    path = path,
                    keyPassword = null,
                    isKeyStore = false
                )
            } else if (path == null && password != null) throw InvalidConfigurationException(
                "SSL trust store is not specified, but trust store password is specified."
            ) else if (path != null) FileBasedStore(
                type = type!!,
                path = path,
                password = password,
                isKeyStore = false
            ) else null
        }
    }
}
