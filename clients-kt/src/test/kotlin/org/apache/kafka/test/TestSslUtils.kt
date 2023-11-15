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

package org.apache.kafka.test

import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.auth.SslEngineFactory
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
import org.bouncycastle.asn1.DEROctetString
import org.bouncycastle.asn1.DERSequence
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.crypto.util.PrivateKeyFactory
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.PKCS8Generator
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder
import org.bouncycastle.operator.bc.BcContentSignerBuilder
import org.bouncycastle.operator.bc.BcDSAContentSignerBuilder
import org.bouncycastle.operator.bc.BcECContentSignerBuilder
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder
import org.bouncycastle.util.io.pem.PemWriter
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter
import java.math.BigInteger
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.security.GeneralSecurityException
import java.security.Key
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.KeyStore
import java.security.NoSuchAlgorithmException
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.Security
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.*
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManagerFactory

object TestSslUtils {

    const val TRUST_STORE_PASSWORD = "TrustStorePassword"

    val DEFAULT_TLS_PROTOCOL_FOR_TESTS = SslConfigs.DEFAULT_SSL_PROTOCOL

    /**
     * Create a self-signed X.509 Certificate.
     * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
     *
     * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
     * @param pair the KeyPair
     * @param days how many days from now the Certificate is valid for
     * @param algorithm the signing algorithm, eg "SHA1withRSA"
     * @return the self-signed certificate
     * @throws CertificateException thrown if a security error or an IO error occurred.
     */
    @Throws(CertificateException::class)
    fun generateCertificate(
        dn: String?,
        pair: KeyPair,
        days: Int,
        algorithm: String?,
    ): X509Certificate {
        return CertificateBuilder(days, algorithm).generate(dn, pair)
    }

    @Throws(NoSuchAlgorithmException::class)
    fun generateKeyPair(algorithm: String): KeyPair {
        val keyGen = KeyPairGenerator.getInstance(algorithm)
        keyGen.initialize(if (algorithm == "EC") 256 else 2048)
        return keyGen.genKeyPair()
    }

    @Throws(GeneralSecurityException::class, IOException::class)
    private fun createEmptyKeyStore(): KeyStore {
        val ks = KeyStore.getInstance("JKS")
        ks.load(null, null) // initialize
        return ks
    }

    @Throws(GeneralSecurityException::class, IOException::class)
    private fun saveKeyStore(
        ks: KeyStore, filename: String,
        password: Password
    ) {
        Files.newOutputStream(Paths.get(filename)).use { out ->
            ks.store(out, password.value.toCharArray())
        }
    }

    /**
     * Creates a keystore with a single key and saves it to a file.
     *
     * @param filename String file to save
     * @param password String store password to set on keystore
     * @param keyPassword String key password to set on key
     * @param alias String alias to use for the key
     * @param privateKey Key to save in keystore
     * @param cert Certificate to use as certificate chain associated to key
     * @throws GeneralSecurityException for any error with the security APIs
     * @throws IOException if there is an I/O error saving the file
     */
    @Throws(GeneralSecurityException::class, IOException::class)
    fun createKeyStore(
        filename: String,
        password: Password,
        keyPassword: Password,
        alias: String?,
        privateKey: Key?,
        cert: Certificate,
    ) {
        val ks = createEmptyKeyStore()
        ks.setKeyEntry(alias, privateKey, keyPassword.value.toCharArray(), arrayOf(cert))
        saveKeyStore(ks, filename, password)
    }

    @Throws(GeneralSecurityException::class, IOException::class)
    fun <T : Certificate?> createTrustStore(
        filename: String,
        password: Password,
        certs: Map<String?, T>,
    ) {
        var ks = KeyStore.getInstance("JKS")
        try {
            Files.newInputStream(Paths.get(filename)).use { `in` ->
                ks.load(`in`, password.value.toCharArray())
            }
        } catch (e: EOFException) {
            ks = createEmptyKeyStore()
        }
        for ((key, value) in certs) {
            ks.setCertificateEntry(key, value)
        }
        saveKeyStore(ks, filename, password)
    }

    fun createSslConfig(
        keyManagerAlgorithm: String,
        trustManagerAlgorithm: String,
        tlsProtocol: String
    ): Map<String, Any> {
        val sslConfigs: MutableMap<String, Any> = HashMap()
        sslConfigs[SslConfigs.SSL_PROTOCOL_CONFIG] = tlsProtocol // protocol to create SSLContext
        sslConfigs[SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG] = keyManagerAlgorithm
        sslConfigs[SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG] = trustManagerAlgorithm
        val enabledProtocols: MutableList<String> = ArrayList()
        enabledProtocols.add(tlsProtocol)
        sslConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = enabledProtocols
        return sslConfigs
    }

    @JvmOverloads
    @Throws(IOException::class, GeneralSecurityException::class)
    fun createSslConfig(
        useClientCert: Boolean,
        trustStore: Boolean,
        mode: Mode,
        trustStoreFile: File,
        certAlias: String?,
        cn: String = "localhost",
    ): Map<String, Any> = createSslConfig(
        useClientCert = useClientCert,
        createTrustStore = trustStore,
        mode = mode,
        trustStoreFile = trustStoreFile,
        certAlias = certAlias,
        cn = cn,
        certBuilder = CertificateBuilder(),
    )

    @Throws(IOException::class, GeneralSecurityException::class)
    fun createSslConfig(
        useClientCert: Boolean,
        createTrustStore: Boolean,
        mode: Mode,
        trustStoreFile: File,
        certAlias: String?,
        cn: String,
        certBuilder: CertificateBuilder
    ): Map<String, Any> {
        var builder = SslConfigsBuilder(mode)
            .useClientCert(useClientCert)
            .certAlias(certAlias)
            .cn(cn)
            .certBuilder(certBuilder)

        builder =
            if (createTrustStore) builder.createNewTrustStore(trustStoreFile)
            else builder.useExistingTrustStore(trustStoreFile)

        return builder.build()
    }

    @Throws(Exception::class)
    fun convertToPem(
        sslProps: MutableMap<String, Any?>,
        writeToFile: Boolean,
        encryptPrivateKey: Boolean,
    ) {
        var tsPath = sslProps[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] as String?
        val tsType = sslProps[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] as String?
        val tsPassword = sslProps.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) as Password
        var trustCerts = sslProps.remove(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG) as Password?

        if (trustCerts == null && tsPath != null)
            trustCerts = exportCertificates(tsPath, tsPassword, tsType)

        if (trustCerts != null) {
            if (tsPath == null) {
                tsPath = TestUtils.tempFile("truststore", ".pem").path
                sslProps[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = tsPath
            }
            sslProps[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE

            if (writeToFile) writeToFile(tsPath!!, trustCerts)
            else {
                sslProps[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] = trustCerts
                sslProps.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
            }
        }
        var ksPath = sslProps[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] as String?
        var certChain =
            sslProps.remove(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG) as Password?
        var key = sslProps.remove(SslConfigs.SSL_KEYSTORE_KEY_CONFIG) as Password?

        if (certChain == null && ksPath != null) {
            val ksType = sslProps[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] as String?
            val ksPassword = sslProps.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) as Password
            val keyPassword = sslProps[SslConfigs.SSL_KEY_PASSWORD_CONFIG] as Password
            certChain = exportCertificates(ksPath, ksPassword, ksType)
            val pemKeyPassword = if (encryptPrivateKey) keyPassword else null
            key = exportPrivateKey(ksPath, ksPassword, keyPassword, ksType, pemKeyPassword)
            if (!encryptPrivateKey) sslProps.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
        }

        if (certChain != null) {
            if (ksPath == null) {
                ksPath = TestUtils.tempFile("keystore", ".pem").path
                sslProps[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = ksPath
            }
            sslProps[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE

            if (writeToFile) writeToFile(ksPath!!, key!!, certChain) else {
                sslProps[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] = key
                sslProps[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] = certChain
                sslProps.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
            }
        }
    }

    @Throws(IOException::class)
    private fun writeToFile(path: String, vararg entries: Password) {
        FileOutputStream(path).use { out ->
            for ((value) in entries) out.write(value.toByteArray(StandardCharsets.UTF_8))
        }
    }

    @Throws(Exception::class)
    fun convertToPemWithoutFiles(sslProps: Properties) {
        val tsPath = sslProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
        if (tsPath != null) {
            val trustCerts = exportCertificates(
                tsPath,
                sslProps[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] as Password,
                sslProps.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)
            )
            sslProps.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
            sslProps.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
            sslProps.setProperty(
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                DefaultSslEngineFactory.PEM_TYPE
            )
            sslProps[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] = trustCerts
        }

        val ksPath = sslProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
        if (ksPath != null) {
            val ksType = sslProps.getProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)
            val ksPassword = sslProps[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] as Password
            val keyPassword = sslProps[SslConfigs.SSL_KEY_PASSWORD_CONFIG] as Password
            val certChain = exportCertificates(ksPath, ksPassword, ksType)
            val key = exportPrivateKey(ksPath, ksPassword, keyPassword, ksType, keyPassword)

            sslProps.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
            sslProps.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
            sslProps.setProperty(
                SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                DefaultSslEngineFactory.PEM_TYPE,
            )
            sslProps[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] = certChain
            sslProps[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] = key
        }
    }

    @Throws(Exception::class)
    fun exportCertificates(
        storePath: String,
        storePassword: Password,
        storeType: String?,
    ): Password {
        val builder = StringBuilder()
        FileInputStream(storePath).use { input ->
            val ks = KeyStore.getInstance(storeType)
            ks.load(input, storePassword.value.toCharArray())
            val aliases = ks.aliases()

            require(aliases.hasMoreElements()) { "No certificates found in file $storePath" }

            while (aliases.hasMoreElements()) {
                val alias = aliases.nextElement()
                val certs = ks.getCertificateChain(alias)

                if (certs != null) for (cert in certs) builder.append(pem(cert))
                else builder.append(pem(ks.getCertificate(alias)))
            }
        }
        return Password(builder.toString())
    }

    @Throws(Exception::class)
    fun exportPrivateKey(
        storePath: String,
        storePassword: Password,
        keyPassword: Password,
        storeType: String?,
        pemKeyPassword: Password?
    ): Password {
        FileInputStream(storePath).use { inputStream ->
            val ks = KeyStore.getInstance(storeType)
            ks.load(inputStream, storePassword.value.toCharArray())
            val alias = ks.aliases().nextElement()
            return Password(
                value = pem(
                    privateKey = ks.getKey(alias, keyPassword.value.toCharArray()) as PrivateKey,
                    password = pemKeyPassword,
                )
            )
        }
    }

    @Throws(IOException::class)
    fun pem(cert: Certificate?): String {
        val out = ByteArrayOutputStream()
        PemWriter(OutputStreamWriter(out, StandardCharsets.UTF_8.name())).use { pemWriter ->
            pemWriter.writeObject(JcaMiscPEMGenerator(cert))
        }
        return String(out.toByteArray(), StandardCharsets.UTF_8)
    }

    @Throws(IOException::class)
    fun pem(privateKey: PrivateKey?, password: Password?): String {
        val out = ByteArrayOutputStream()
        PemWriter(OutputStreamWriter(out, StandardCharsets.UTF_8.name())).use { pemWriter ->
            if (password == null)
                pemWriter.writeObject(JcaPKCS8Generator(privateKey, null))
            else {
                val encryptorBuilder = JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.PBE_SHA1_3DES)
                encryptorBuilder.setPassword(password.value.toCharArray())
                try {
                    pemWriter.writeObject(JcaPKCS8Generator(privateKey, encryptorBuilder.build()))
                } catch (e: Exception) {
                    throw RuntimeException(e)
                }
            }
        }
        return String(out.toByteArray(), StandardCharsets.UTF_8)
    }

    class CertificateBuilder(
        private val days: Int = 30,
        private val algorithm: String? = "SHA1withRSA",
    ) {

        private lateinit var subjectAltName: ByteArray

        @Throws(IOException::class)
        fun sanDnsNames(vararg hostNames: String?): CertificateBuilder {
            val altNames = arrayOfNulls<GeneralName>(hostNames.size)
            for (i in hostNames.indices) altNames[i] = GeneralName(
                GeneralName.dNSName,
                hostNames[i]
            )
            subjectAltName = GeneralNames.getInstance(DERSequence(altNames)).encoded
            return this
        }

        @Throws(IOException::class)
        fun sanIpAddress(hostAddress: InetAddress): CertificateBuilder {
            subjectAltName = GeneralNames(
                GeneralName(
                    GeneralName.iPAddress,
                    DEROctetString(hostAddress.address)
                )
            ).encoded
            return this
        }

        @Throws(CertificateException::class)
        fun generate(dn: String?, keyPair: KeyPair): X509Certificate {
            return try {
                Security.addProvider(BouncyCastleProvider())
                val sigAlgId = DefaultSignatureAlgorithmIdentifierFinder().find(algorithm)
                val digAlgId = DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId)
                val privateKeyAsymKeyParam = PrivateKeyFactory.createKey(keyPair.private.encoded)
                val subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.public.encoded)
                val signerBuilder: BcContentSignerBuilder
                val keyAlgorithm = keyPair.public.algorithm

                signerBuilder = when (keyAlgorithm) {
                    "RSA" -> BcRSAContentSignerBuilder(sigAlgId, digAlgId)
                    "DSA" -> BcDSAContentSignerBuilder(sigAlgId, digAlgId)
                    "EC" -> BcECContentSignerBuilder(sigAlgId, digAlgId)
                    else -> throw IllegalArgumentException("Unsupported algorithm $keyAlgorithm")
                }

                val sigGen = signerBuilder.build(privateKeyAsymKeyParam)
                val name = X500Name(dn)
                val from = Date()
                val to = Date(from.time + days * 86400000L)
                val sn = BigInteger(64, SecureRandom())
                val v3CertGen = X509v3CertificateBuilder(name, sn, from, to, name, subPubKeyInfo)

                if (::subjectAltName.isInitialized) v3CertGen.addExtension(
                    Extension.subjectAlternativeName,
                    false,
                    subjectAltName,
                )
                val certificateHolder = v3CertGen.build(sigGen)
                JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder)
            } catch (ce: CertificateException) {
                throw ce
            } catch (e: Exception) {
                throw CertificateException(e)
            }
        }
    }

    class SslConfigsBuilder(val mode: Mode) {

        var tlsProtocol: String = DEFAULT_TLS_PROTOCOL_FOR_TESTS

        var useClientCert = false

        var createTrustStore: Boolean = true

        lateinit var trustStoreFile: File

        var trustStorePassword: Password = Password(TRUST_STORE_PASSWORD)

        var keyStorePassword: Password = Password(
            if (mode === Mode.SERVER) "ServerPassword"
            else "ClientPassword"
        )

        var keyPassword: Password = keyStorePassword

        var certAlias: String? = mode.name.lowercase()

        var cn: String = "localhost"

        var algorithm: String = "RSA"

        var certBuilder: CertificateBuilder = CertificateBuilder()

        var usePem = false

        fun tlsProtocol(tlsProtocol: String): SslConfigsBuilder {
            this.tlsProtocol = tlsProtocol
            return this
        }

        fun createNewTrustStore(trustStoreFile: File): SslConfigsBuilder {
            this.trustStoreFile = trustStoreFile
            createTrustStore = true
            return this
        }

        fun useExistingTrustStore(trustStoreFile: File): SslConfigsBuilder {
            this.trustStoreFile = trustStoreFile
            createTrustStore = false
            return this
        }

        fun useClientCert(useClientCert: Boolean): SslConfigsBuilder {
            this.useClientCert = useClientCert
            return this
        }

        fun certAlias(certAlias: String?): SslConfigsBuilder {
            this.certAlias = certAlias
            return this
        }

        fun cn(cn: String): SslConfigsBuilder {
            this.cn = cn
            return this
        }

        fun algorithm(algorithm: String): SslConfigsBuilder {
            this.algorithm = algorithm
            return this
        }

        fun certBuilder(certBuilder: CertificateBuilder): SslConfigsBuilder {
            this.certBuilder = certBuilder
            return this
        }

        fun usePem(usePem: Boolean): SslConfigsBuilder {
            this.usePem = usePem
            return this
        }

        @Throws(IOException::class, GeneralSecurityException::class)
        fun build(): Map<String, Any> {
            return if (usePem) buildPem()
            else buildJks()
        }

        @Throws(IOException::class, GeneralSecurityException::class)
        private fun buildJks(): Map<String, Any> {
            val certs: MutableMap<String?, X509Certificate> = HashMap()
            var keyStoreFile: File? = null
            if (mode === Mode.CLIENT && useClientCert) {
                keyStoreFile = TestUtils.tempFile("clientKS", ".jks")
                val cKP = generateKeyPair(algorithm)
                val cCert = certBuilder.generate("CN=$cn, O=A client", cKP)
                createKeyStore(
                    filename = keyStoreFile.path,
                    password = keyStorePassword,
                    keyPassword = keyPassword,
                    alias = "client",
                    privateKey = cKP.private,
                    cert = cCert,
                )
                certs[certAlias] = cCert
            } else if (mode === Mode.SERVER) {
                keyStoreFile = TestUtils.tempFile("serverKS", ".jks")
                val sKP = generateKeyPair(algorithm)
                val sCert = certBuilder.generate("CN=$cn, O=A server", sKP)
                createKeyStore(
                    filename = keyStoreFile.path,
                    password = keyStorePassword,
                    keyPassword = keyPassword,
                    alias = "server",
                    privateKey = sKP.private,
                    cert = sCert
                )
                certs[certAlias] = sCert
                keyStoreFile.deleteOnExit()
            }

            if (createTrustStore) {
                createTrustStore(trustStoreFile.path, trustStorePassword, certs)
                trustStoreFile.deleteOnExit()
            }

            val sslConfigs: MutableMap<String, Any> = HashMap()
            // protocol to create SSLContext
            sslConfigs[SslConfigs.SSL_PROTOCOL_CONFIG] = tlsProtocol
            if (mode === Mode.SERVER || mode === Mode.CLIENT && keyStoreFile != null) {
                sslConfigs[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = keyStoreFile!!.path
                sslConfigs[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = "JKS"
                sslConfigs[SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG] =
                    TrustManagerFactory.getDefaultAlgorithm()
                sslConfigs[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = keyStorePassword
                sslConfigs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = keyPassword
            }
            sslConfigs[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = trustStoreFile.path
            sslConfigs[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = trustStorePassword
            sslConfigs[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = "JKS"
            sslConfigs[SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG] =
                TrustManagerFactory.getDefaultAlgorithm()

            val enabledProtocols = mutableListOf<String>()
            enabledProtocols.add(tlsProtocol)
            sslConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = enabledProtocols
            return sslConfigs
        }

        @Throws(IOException::class, GeneralSecurityException::class)
        private fun buildPem(): Map<String, Any> {
            require(createTrustStore) { "PEM configs cannot be created with existing trust stores" }
            val sslConfigs: MutableMap<String, Any> = HashMap()
            sslConfigs[SslConfigs.SSL_PROTOCOL_CONFIG] = tlsProtocol
            sslConfigs[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = listOf(tlsProtocol)
            if (mode !== Mode.CLIENT || useClientCert) {
                val keyPair = generateKeyPair(algorithm)
                val cert =
                    certBuilder.generate("CN=$cn, O=A ${mode.name.lowercase()}", keyPair)
                val privateKeyPem = Password(pem(keyPair.private, keyPassword))
                val certPem = Password(pem(cert))
                sslConfigs[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
                sslConfigs[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
                sslConfigs[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] = privateKeyPem
                sslConfigs[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] = certPem
                sslConfigs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = keyPassword
                sslConfigs[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] = certPem
            }
            return sslConfigs
        }
    }

    class TestSslEngineFactory : SslEngineFactory {
        var closed = false
        var defaultSslEngineFactory = DefaultSslEngineFactory()
        override fun createClientSslEngine(
            peerHost: String,
            peerPort: Int,
            endpointIdentification: String
        ): SSLEngine {
            return defaultSslEngineFactory.createClientSslEngine(
                peerHost,
                peerPort,
                endpointIdentification
            )
        }

        override fun createServerSslEngine(peerHost: String, peerPort: Int): SSLEngine =
            defaultSslEngineFactory.createServerSslEngine(peerHost, peerPort)

        override fun shouldBeRebuilt(nextConfigs: Map<String, Any?>): Boolean =
            defaultSslEngineFactory.shouldBeRebuilt(nextConfigs)

        override fun reconfigurableConfigs(): Set<String> =
            defaultSslEngineFactory.reconfigurableConfigs()

        override fun keystore(): KeyStore? = defaultSslEngineFactory.keystore()

        override fun truststore(): KeyStore? = defaultSslEngineFactory.truststore()

        @Throws(IOException::class)
        override fun close() {
            defaultSslEngineFactory.close()
            closed = true
        }

        override fun configure(configs: Map<String, *>) {
            defaultSslEngineFactory.configure(configs)
        }
    }
}
