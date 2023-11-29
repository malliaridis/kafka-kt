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

import java.io.Closeable
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.ByteBuffer
import java.security.GeneralSecurityException
import java.security.KeyStore
import java.security.Principal
import java.security.cert.X509Certificate
import java.util.*
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.security.auth.SslEngineFactory
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.ensureCapacity
import org.apache.kafka.common.utils.Utils.newInstance
import org.slf4j.LoggerFactory

/**
 * Create an SslFactory.
 *
 * @param mode Whether to use client or server mode.
 * @param clientAuthConfigOverride The value to override `ssl.client.auth` with, or `null` if we
 * don't want to override it.
 * @param keystoreVerifiableUsingTruststore True if we should require the keystore to be verifiable
 * using the truststore.
 */
class SslFactory(
    private val mode: Mode,
    private val clientAuthConfigOverride: String? = null,
    private val keystoreVerifiableUsingTruststore: Boolean = false
) : Reconfigurable, Closeable {

    private var endpointIdentification: String? = null

    var sslEngineFactory: SslEngineFactory? = null

    private var sslEngineFactoryConfig: Map<String, Any?>? = null

    @Throws(KafkaException::class)
    override fun configure(configs: Map<String, Any?>) {
        check(sslEngineFactory == null) { "SslFactory was already configured." }

        endpointIdentification = configs[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] as String?

        // The input map must be a mutable RecordingMap in production.
        val nextConfigs = configs.toMutableMap()
        if (clientAuthConfigOverride != null) {
            nextConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = clientAuthConfigOverride
        }
        val builder = instantiateSslEngineFactory(nextConfigs)
        if (keystoreVerifiableUsingTruststore) {
            try {
                SslEngineValidator.validate(builder, builder)
            } catch (e: Exception) {
                throw ConfigException(
                    name = "A client SSLEngine created with the provided settings " +
                            "can't connect to a server SSLEngine created with those settings.",
                    value = e
                )
            }
        }
        sslEngineFactory = builder
    }

    override fun reconfigurableConfigs(): Set<String> {
        return sslEngineFactory!!.reconfigurableConfigs()
    }

    override fun validateReconfiguration(configs: Map<String, *>) {
        createNewSslEngineFactory(configs)
    }

    @Throws(KafkaException::class)
    override fun reconfigure(configs: Map<String, *>) {
        val newSslEngineFactory = createNewSslEngineFactory(configs)

        if (newSslEngineFactory != sslEngineFactory) {
            closeQuietly(sslEngineFactory, "close stale ssl engine factory")
            sslEngineFactory = newSslEngineFactory
            log.info(
                "Created new {} SSL engine builder with keystore {} truststore {}",
                mode,
                newSslEngineFactory.keystore(),
                newSslEngineFactory.truststore(),
            )
        }
    }

    private fun instantiateSslEngineFactory(configs: Map<String, Any?>): SslEngineFactory {
        val sslEngineFactoryClass =
            configs[SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG] as Class<out SslEngineFactory>?

        val sslEngineFactory =
            if (sslEngineFactoryClass == null) DefaultSslEngineFactory()
            else newInstance(sslEngineFactoryClass)

        sslEngineFactory.configure(configs)
        sslEngineFactoryConfig = configs
        return sslEngineFactory
    }

    private fun createNewSslEngineFactory(newConfigs: Map<String, *>): SslEngineFactory {
        val sslEngineFactory = checkNotNull(sslEngineFactory) { "SslFactory has not been configured." }

        val nextConfigs = sslEngineFactoryConfig!!.toMutableMap()
        copyMapEntries(nextConfigs, newConfigs, reconfigurableConfigs())

        if (clientAuthConfigOverride != null) {
            nextConfigs[BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG] = clientAuthConfigOverride
        }

        return if (!sslEngineFactory.shouldBeRebuilt(nextConfigs)) sslEngineFactory
        else try {
            val newSslEngineFactory = instantiateSslEngineFactory(nextConfigs)
            if (sslEngineFactory.keystore() == null) {
                if (newSslEngineFactory.keystore() != null) throw ConfigException(
                    "Cannot add SSL keystore to an existing listener for " +
                            "which no keystore was configured."
                )
            } else {
                if (newSslEngineFactory.keystore() == null) throw ConfigException(
                    "Cannot remove the SSL keystore from an existing listener for " +
                            "which a keystore was configured."
                )

                CertificateEntries.ensureCompatible(
                    newSslEngineFactory.keystore(),
                    sslEngineFactory.keystore()
                )
            }
            if (sslEngineFactory.truststore() == null && newSslEngineFactory.truststore() != null) {
                throw ConfigException(
                    "Cannot add SSL truststore to an existing listener for which no " +
                            "truststore was configured."
                )
            }
            if (keystoreVerifiableUsingTruststore) {
                if (sslEngineFactory.truststore() != null || sslEngineFactory.keystore() != null)
                    SslEngineValidator.validate(
                        oldEngineBuilder = sslEngineFactory,
                        newEngineBuilder = newSslEngineFactory,
                    )
            }
            newSslEngineFactory
        } catch (e: Exception) {
            log.debug("Validation of dynamic config update of SSLFactory failed.", e)
            throw ConfigException("Validation of dynamic config update of SSLFactory failed: $e")
        }
    }

    fun createSslEngine(socket: Socket): SSLEngine = createSslEngine(peerHost(socket), socket.port)

    /**
     * Prefer `createSslEngine(Socket)` if a `Socket` instance is available. If using this overload,
     * avoid reverse DNS resolution in the computation of `peerHost`.
     */
    fun createSslEngine(peerHost: String, peerPort: Int): SSLEngine {
        val sslEngineFactory = sslEngineFactory
        check(sslEngineFactory != null) { "SslFactory has not been configured." }

        return when(mode) {
            Mode.CLIENT -> sslEngineFactory.createClientSslEngine(
                peerHost = peerHost,
                peerPort = peerPort,
                endpointIdentification = endpointIdentification,
            )
            Mode.SERVER -> sslEngineFactory.createServerSslEngine(peerHost, peerPort)
        }
    }

    /**
     * Returns host/IP address of remote host without reverse DNS lookup to be used as the host
     * for creating SSL engine. This is used as a hint for session reuse strategy and also for
     * hostname verification of server hostnames.
     *
     * Scenarios:
     *  * Server-side
     *    * Server accepts connection from a client. Server knows only client IP address. We want to
     *      avoid reverse DNS lookup of the client IP address since the server does not verify or
     *      use client hostname. The IP address can be used directly.
     *  * Client-side
     *    * Client connects to server using hostname. No lookup is necessary and the hostname should
     *      be used to create the SSL engine. This hostname is validated against the hostname in
     *      SubjectAltName (dns) or CommonName in the certificate if hostname verification is
     *      enabled. Authentication fails if hostname does not match.
     *    * Client connects to server using IP address, but certificate contains only SubjectAltName
     *      (dns). Use of reverse DNS lookup to determine hostname introduces a security
     *      vulnerability since authentication would be reliant on a secure DNS. Hence hostname
     *      verification should fail in this case.
     *    * Client connects to server using IP address and certificate contains SubjectAltName
     *      (ipaddress). This could be used when Kafka is on a private network. If reverse DNS
     *      lookup is used, authentication would succeed using IP address if lookup fails and IP
     *      address is used, but authentication would fail if lookup succeeds and dns name is used.
     *      For consistency and to avoid dependency on a potentially insecure DNS, reverse DNS
     *      lookup should be avoided and the IP address specified by the client for connection
     *      should be used to create the SSL engine.
     */
    private fun peerHost(socket: Socket): String {
        return InetSocketAddress(socket.inetAddress, 0).hostString
    }

    override fun close() {
        closeQuietly(sslEngineFactory, "close engine factory")
    }

    internal class CertificateEntries(private val alias: String, cert: X509Certificate) {

        private val subjectPrincipal: Principal

        private val subjectAltNames: Set<List<*>>

        init {
            subjectPrincipal = cert.subjectX500Principal
            val altNames = cert.subjectAlternativeNames
            // use a set for comparison
            subjectAltNames = altNames?.let { altNames.toHashSet() } ?: emptySet()
        }

        override fun hashCode(): Int {
            return Objects.hash(subjectPrincipal, subjectAltNames)
        }

        override fun equals(other: Any?): Boolean {
            if (other !is CertificateEntries) return false
            return subjectPrincipal == other.subjectPrincipal
                    && subjectAltNames == other.subjectAltNames
        }

        override fun toString(): String {
            return "subjectPrincipal=" + subjectPrincipal +
                    ", subjectAltNames=" + subjectAltNames
        }

        companion object {

            @Throws(GeneralSecurityException::class)
            fun create(keystore: KeyStore?): List<CertificateEntries> {
                val aliases = keystore!!.aliases()
                val entries: MutableList<CertificateEntries> = ArrayList()

                while (aliases.hasMoreElements()) {
                    val alias = aliases.nextElement()
                    val cert = keystore.getCertificate(alias)

                    if (cert is X509Certificate) entries.add(CertificateEntries(alias, cert))
                }
                return entries
            }

            @Throws(GeneralSecurityException::class)
            fun ensureCompatible(newKeystore: KeyStore?, oldKeystore: KeyStore?) {
                val newEntries = create(newKeystore)
                val oldEntries = create(oldKeystore)

                if (newEntries.size != oldEntries.size) {
                    throw ConfigException(
                        String.format(
                            "Keystore entries do not match, existing store contains %d entries, " +
                                    "new store contains %d entries",
                            oldEntries.size,
                            newEntries.size
                        )
                    )
                }

                for (i in newEntries.indices) {
                    val newEntry = newEntries[i]
                    val oldEntry = oldEntries[i]

                    if (newEntry.subjectPrincipal != oldEntry.subjectPrincipal) {
                        throw ConfigException(
                            String.format(
                                "Keystore DistinguishedName does not match: " +
                                        " existing={alias=%s, DN=%s}, new={alias=%s, DN=%s}",
                                oldEntry.alias,
                                oldEntry.subjectPrincipal,
                                newEntry.alias,
                                newEntry.subjectPrincipal
                            )
                        )
                    }

                    if (!newEntry.subjectAltNames.containsAll(oldEntry.subjectAltNames)) {
                        throw ConfigException(
                            String.format(
                                "Keystore SubjectAltNames do not match: " +
                                        " existing={alias=%s, SAN=%s}, new={alias=%s, SAN=%s}",
                                oldEntry.alias,
                                oldEntry.subjectAltNames,
                                newEntry.alias,
                                newEntry.subjectAltNames
                            )
                        )
                    }
                }
            }
        }
    }

    /**
     * Validator used to verify dynamic update of keystore used in inter-broker communication.
     * The validator checks that a successful handshake can be performed using the keystore and
     * truststore configured on this SslFactory.
     */
    private class SslEngineValidator private constructor(private val sslEngine: SSLEngine) {

        private lateinit var handshakeResult: SSLEngineResult

        private var appBuffer: ByteBuffer

        private var netBuffer: ByteBuffer

        init {
            appBuffer = ByteBuffer.allocate(sslEngine.session.applicationBufferSize)
            netBuffer = ByteBuffer.allocate(sslEngine.session.packetBufferSize)
        }

        @Throws(SSLException::class)
        fun beginHandshake() {
            sslEngine.beginHandshake()
        }

        @Throws(SSLException::class)
        fun handshake(peerValidator: SslEngineValidator) {
            var handshakeStatus = sslEngine.handshakeStatus
            while (true) {
                when (handshakeStatus) {
                    SSLEngineResult.HandshakeStatus.NEED_WRAP -> {
                        if (netBuffer.position() != 0) // Wait for peer to consume previously wrapped data
                            return
                        handshakeResult = sslEngine.wrap(EMPTY_BUF, netBuffer)
                        when (handshakeResult.status) {
                            SSLEngineResult.Status.OK -> {}
                            SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                                netBuffer.compact()
                                netBuffer =
                                    ensureCapacity(netBuffer, sslEngine.session.packetBufferSize)
                                netBuffer.flip()
                            }

                            SSLEngineResult.Status.BUFFER_UNDERFLOW, SSLEngineResult.Status.CLOSED ->
                                throw SSLException(
                                    "Unexpected handshake status: ${handshakeResult.status}"
                                )

                            else -> throw SSLException("Unexpected handshake status: ${handshakeResult.status}")
                        }
                        return
                    }

                    SSLEngineResult.HandshakeStatus.NEED_UNWRAP -> {
                        if (peerValidator.netBuffer.position() == 0) // no data to unwrap, return to process peer
                            return
                        peerValidator.netBuffer.flip() // unwrap the data from peer
                        handshakeResult = sslEngine.unwrap(peerValidator.netBuffer, appBuffer)
                        peerValidator.netBuffer.compact()
                        handshakeStatus = handshakeResult.handshakeStatus

                        when (handshakeResult.status) {
                            SSLEngineResult.Status.OK -> {}
                            SSLEngineResult.Status.BUFFER_OVERFLOW -> appBuffer =
                                ensureCapacity(appBuffer, sslEngine.session.applicationBufferSize)

                            SSLEngineResult.Status.BUFFER_UNDERFLOW -> netBuffer =
                                ensureCapacity(netBuffer, sslEngine.session.packetBufferSize)

                            SSLEngineResult.Status.CLOSED ->
                                throw SSLException("Unexpected handshake status: ${handshakeResult.status}")

                            else -> throw SSLException("Unexpected handshake status: ${handshakeResult.status}")
                        }
                    }

                    SSLEngineResult.HandshakeStatus.NEED_TASK -> {
                        sslEngine.delegatedTask.run()
                        handshakeStatus = sslEngine.handshakeStatus
                    }

                    SSLEngineResult.HandshakeStatus.FINISHED -> return
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING -> {
                        if (handshakeResult.handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED)
                            throw SSLException("Did not finish handshake")
                        return
                    }

                    else -> error("Unexpected handshake status $handshakeStatus")
                }
            }
        }

        fun complete(): Boolean {
            return sslEngine.handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED ||
                    sslEngine.handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
        }

        fun close() {
            sslEngine.closeOutbound()
            try {
                sslEngine.closeInbound()
            } catch (e: Exception) {
                // ignore
            }
        }

        companion object {

            private val EMPTY_BUF = ByteBuffer.allocate(0)

            @Throws(SSLException::class)
            fun validate(
                oldEngineBuilder: SslEngineFactory,
                newEngineBuilder: SslEngineFactory
            ) {
                validate(
                    createSslEngineForValidation(oldEngineBuilder, Mode.SERVER),
                    createSslEngineForValidation(newEngineBuilder, Mode.CLIENT)
                )
                validate(
                    createSslEngineForValidation(newEngineBuilder, Mode.SERVER),
                    createSslEngineForValidation(oldEngineBuilder, Mode.CLIENT)
                )
            }

            private fun createSslEngineForValidation(
                sslEngineFactory: SslEngineFactory,
                mode: Mode
            ): SSLEngine {
                // Use empty hostname, disable hostname verification
                return when(mode) {
                    Mode.CLIENT -> sslEngineFactory.createClientSslEngine("", 0, "")
                    Mode.SERVER -> sslEngineFactory.createServerSslEngine("", 0)
                }
            }

            @Throws(SSLException::class)
            fun validate(clientEngine: SSLEngine, serverEngine: SSLEngine) {
                val clientValidator = SslEngineValidator(clientEngine)
                val serverValidator = SslEngineValidator(serverEngine)
                try {
                    clientValidator.beginHandshake()
                    serverValidator.beginHandshake()
                    while (!serverValidator.complete() || !clientValidator.complete()) {
                        clientValidator.handshake(serverValidator)
                        serverValidator.handshake(clientValidator)
                    }
                } finally {
                    clientValidator.close()
                    serverValidator.close()
                }
            }
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(SslFactory::class.java)

        /**
         * Copy entries from one map into another.
         *
         * @param destMap The map to copy entries into.
         * @param srcMap The map to copy entries from.
         * @param keySet Only entries with these keys will be copied.
         * @param K The map key type.
         * @param V The map value type.
         */
        private fun <K, V> copyMapEntries(
            destMap: MutableMap<K, V>,
            srcMap: Map<K, V>,
            keySet: Set<K>
        ) = keySet.forEach { key -> copyMapEntry(destMap, srcMap, key) }

        /**
         * Copy entry from one map into another.
         *
         * @param destMap The map to copy entries into.
         * @param srcMap The map to copy entries from.
         * @param key The entry with this key will be copied
         * @param K The map key type.
         * @param V The map value type.
         */
        private fun <K, V> copyMapEntry(
            destMap: MutableMap<K, V>,
            srcMap: Map<K, V>,
            key: K
        ) = srcMap[key]?.let { destMap[key] = it }
    }
}
